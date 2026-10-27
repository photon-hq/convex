# @photon-ai/convex

A [Convex](https://convex.dev) component for [Photon](https://photon.codes) Beta
(2.0): receive messages from Beta messaging platforms such as iMessage, SMS,
WhatsApp Business, Email and Telegram, and reply through the Photon API.

> Using Photon Stable? Its component is `@spectrum-ts/convex`, in
> [photon-hq/spectrum-convex](https://github.com/photon-hq/spectrum-convex). It
> receives security fixes only, until the Stable platform shuts down.

Photon delivers incoming messages to a webhook, and a Convex isolate is frozen
the moment an HTTP action returns its response. So this component verifies each
delivery and writes it to a table **before** the 200 goes back: an acknowledged
message is already durable. Everything after that runs on Convex's scheduler —
your handler, and an outbox that sends replies through the Photon API with
retries.

It also handles bursts of messages: a newer message can cancel the turn in
progress and carry its messages forward, and replies can be paced.

## Install

```sh
npm i @photon-ai/convex
```

```ts
// convex/convex.config.ts
import photon from "@photon-ai/convex/convex.config";
import { defineApp } from "convex/server";

const app = defineApp();
app.use(photon);
export default app;
```

In the [Photon Beta dashboard](https://app.beta.photon.codes), open your project's
**Webhooks** page and add a destination:

- URL: `https://<deployment>.convex.site/photon/webhook`
- API version: `2026-07-01` — the version this release reads (see
  [API versions](#api-versions)). A destination's API version can't be changed
  later.
- Events: `message.received`

Then set these in the Convex dashboard, on the app's deployment:

| Variable | Purpose |
|---|---|
| `PHOTON_PROJECT_ID` | Your project's id (`pho_prj_…`) |
| `PHOTON_API_KEY` | A project API key, used to send |
| `PHOTON_WEBHOOK_SECRET` | The destination's signing secret (`whsec_…`) |
| `PHOTON_BASE_URL` | Optional. The API origin; defaults to `https://api.photon.codes` |

Each can also be passed to `new PhotonComponent` as `projectId`, `apiKey`,
`webhookSecret` and `baseUrl`.

## Use

```ts
// convex/photon.ts
import { PhotonComponent } from "@photon-ai/convex";
import { components, internal } from "./_generated/api";

export const photon: PhotonComponent = new PhotonComponent(components.photon, {
  onTurn: internal.agent.respond, // called once per turn of received messages
});
```

```ts
// convex/http.ts — POST https://<deployment>.convex.site/photon/webhook
import { httpRouter } from "convex/server";
import { photon } from "./photon";

const http = httpRouter();
photon.registerRoutes(http);
export default http;
```

```ts
// convex/agent.ts
import { v } from "convex/values";
import { internalAction } from "./_generated/server";
import { photon } from "./photon";

export const respond = internalAction({
  args: { spaceId: v.string(), turnId: v.string(), messages: v.array(v.any()), carried: v.array(v.any()) },
  returns: v.null(),
  handler: async (ctx, args) => {
    await photon.markMessageRead(ctx, args);
    // `messages` and `carried` are Photon `Message`s (the type is exported by @photon-ai/api).
    const reply = await generate(args.messages, args.carried); // your LLM call

    // In collapse mode a newer message cancels this turn; re-check after
    // anything slow. Immediate mode (the default) never cancels.
    if (await photon.isCancelled(ctx, args)) return null;

    await photon.sendMessage(ctx, {
      spaceId: args.spaceId,
      turnId: args.turnId,
      content: { type: "text", format: "plain", text: reply },
    });
    await photon.completeTurn(ctx, args);
    return null;
  },
});
```

See [`example/`](./example) for a complete app.

## How the pipeline behaves

```
webhook ──▶ verify + enqueue (durable) ──▶ 200
                     │
                     ├─ dedupe on message id (deliveries are at-least-once)
                     └─ start a turn
                                │
                                ▶ your onTurn handler
                                        │
                                        ▶ outbox ─▶ Photon API
```

### Choosing a turn policy

The component does not impose a conversation model. Pick one with `mode`:

| `mode` | Behaviour | Use when |
|---|---|---|
| `"immediate"` *(default)* | Every message is handled on its own. N messages → N handler calls. `messages` always has one entry, `carried` is always empty. | Replies are cheap and answering each message is correct — echo bots, commands, notifications. |
| `"collapse"` | A burst becomes one turn. A new message supersedes the in-flight turn, cancels any reply that hasn't shipped, and carries its messages forward as prior context. | Replies are expensive (an LLM call), or answering each message separately would read as the agent talking over itself. |

```ts
new PhotonComponent(components.photon, {
  onTurn: internal.agent.respond,
  mode: "collapse",   // default is "immediate"
  debounceMs: 0,      // optional extra settling window, collapse mode only
  pacingMs: 600,      // gap between a turn's replies
});
```

`debounceMs` is a latency cost on every reply, so it defaults to 0 — in
collapse mode, cancellation already absorbs most bursts on its own, because a
follow-up usually arrives while the previous reply is still in flight.

Call `completeTurn` when the handler finishes. In collapse mode, the messages
of a turn that never completes are carried into the next turn even if it
replied.

A turn's replies go out one at a time, in the order they were queued, each
after the first at least `pacingMs` after the one before it. Read receipts and
reactions are never held back or delayed, and don't count as replies.

### Guarantees in both modes

- **At-least-once deliveries are deduped** on the message id.
- **Read receipts and typing indicators never start a turn** (`message.read`
  and `space.typing.update` content). Otherwise a bot replies to the read
  receipt for its own last message and echoes forever. Every other content type
  starts a turn, including ones this release doesn't know.
- **Retries don't send twice.** Each outbox row keeps one `idempotencyKey`
  across its retries, and `sendMessage` and `markMessageRead` send it as the
  request's `idempotency-key` header. A retry after a lost response gets the
  original result back instead of sending the message a second time.
- **A message stays recoverable until a reply to it goes out.** It is not spent
  when it is drained, and not when the handler returns: a handler that has
  finished has only *queued* its reply, and a follow-up moments later can still
  cancel it and carry the message into the next turn. A reply that is already
  going out can't be recalled, so it is finished, with retries, and counts as
  the answer; if it then fails for good, its row in `listOutbox` shows it.

## Sending

The component calls the Photon API itself, from Convex's default runtime — no
`"use node"` action. `PhotonComponent` passes the credentials with every send;
the component keeps the latest in its `config` table, so **anyone with access
to the deployment's dashboard can read the API key**.

| Method | API call |
|---|---|
| `photon.sendMessage(ctx, { spaceId, content, turnId?, parentMessageId?, email?, telegram?, pacingMs? })` | `sendMessage` — `content` is the API's send content, e.g. `{ type: "text", format: "plain", text }` or `format: "markdown"`. `parentMessageId` makes it a reply; `email` and `telegram` are the platform options. |
| `photon.markMessageRead(ctx, { spaceId, turnId })` | `markMessageRead` on the turn's newest message |
| `ctx.runMutation(components.photon.outbox.enqueue, { spaceId, turnId, kind: "setSpaceTyping" })` | `setSpaceTyping` (typing on). Uses the credentials the last send stored; to set them, pass `projectId`, `apiKey` and, if you use one, `baseUrl` together. |

Replies are sent as the agent's own user in the space — the `receivedBy` of the
incoming messages. A space that has never received a message with `receivedBy`
has no sender, and its sends fail without a request, so the component can't
start a conversation in a new space.

A send is retried after a network error or a `408`, `429` or `5xx`: up to 5
attempts, 4 s, 16 s, 64 s and 256 s apart. An attempt whose action fails before
recording the result is retried the same way; the component checks on each
attempt a minute after it starts. Any other error is final — for
example `409 SPACE_NOT_SENDABLE` (SMS doesn't take replies) or `422
OPERATION_UNAVAILABLE` (read receipts and typing are not available on every
platform or in every space). `listOutbox` returns a space's most recent outbox
rows (50 by default), oldest first, with their `status` and `lastError`. A send
queued for a turn that a newer message has cancelled is not sent: `sendMessage`
returns `{ queued: false, reason: "turn-cancelled" }`.

Uploading attachments is not part of this release. Received attachment
messages are stored and handed to your handler like any other message.

## The webhook endpoint

| Request | Response |
|---|---|
| `PHOTON_WEBHOOK_SECRET` missing, or it cannot be decoded as a signing secret | `500` |
| Missing `webhook-*` header, a signature that doesn't match the secret, or a timestamp more than 5 minutes off | `401` |
| Signed, but the body is not a JSON object, the `apiVersion` is not one this release reads, `type` is missing, or a `message.received` has no `data.object.id` or `data.object.platform` | `400` |
| Any event type other than `message.received` | `200`, not stored |
| A message without `data.object.space.id` | `200`, not stored |
| A message, or a redelivery of one | `200`, stored once |

Photon does not retry a `4xx` other than `404`, `408` and `429`; it retries a
`5xx`.

## API versions

Every event carries the `apiVersion` of the destination that delivered it, and
a destination's API version is fixed when it is created. This release reads
**`2026-07-01`** only, and answers any other version with `400`. `2026-07-01`
is currently Photon's only webhook API version, and its status is still
`preview`, so its event format can still change.

Later releases read the API versions Photon has in their latest or maintenance
phase: a version is added when Photon promotes it to latest, and removed in the
release after it is deprecated, with a note in the changelog. Photon stops
delivering to a destination whose API version is deprecated, so move to a
destination on a newer version before then.

## Testing against the component

Apps that install the component register it with `convex-test` explicitly —
its schema and function modules are not part of the app's own glob:

```ts
// convex/setup.test.ts
import { convexTest } from "convex-test";
import photon from "@photon-ai/convex/test";
import schema from "./schema";

const modules = import.meta.glob("./**/*.ts");

export function initConvexTest() {
  const t = convexTest(schema, modules);
  photon.register(t);
  return t;
}
```

`register(t, name?)` defaults to `"photon"` — pass the name you installed
under if you changed it in `convex.config.ts`.

## Development

This is a single npm package: the root `package.json` owns both the component
and the example app in `example/`, and root `convex.json` points `functions` at
`example/convex`. There is no separate install inside `example/`.

```sh
npm install
npm run dev       # codegen + build in watch, alongside `convex dev`
```

`npm run dev` runs three things the component needs in order — component
codegen, the package build, then `convex dev --typecheck-components` against the
example app. To do a one-off instead:

```sh
npm run build:codegen   # convex codegen + tsc
npm test
npm run typecheck       # package and example/convex
npm run check           # biome
```

`src/component/_generated/` is committed. It holds `component.ts`, the
`ComponentApi` type the client re-exports, so regenerate and commit it whenever
a component validator changes — `npm run build:codegen` does both.

Publishing is documented in [PUBLISHING.md](./PUBLISHING.md).

## License

MIT © [Photon](https://photon.codes)
