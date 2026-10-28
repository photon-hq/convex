# Example app

A complete Convex app wiring up `@photon-ai/convex`: the webhook route and an
echo bot that answers every turn.

## Run it

This app has no `package.json` of its own — it is served by the repo root,
whose `convex.json` sets `"functions": "example/convex"`. Run everything from
the root:

```sh
pnpm install
pnpm run dev
```

The first run creates a deployment and regenerates `convex/_generated/` for
both the app and the component.

Then set the environment variables in the Convex dashboard:

```
PHOTON_PROJECT_ID=pho_prj_...
PHOTON_API_KEY=...
PHOTON_WEBHOOK_SECRET=whsec_...
```

and add a webhook destination to your Photon project, with API version
`2026-07-01` and the `message.received` event, pointing at:

```
https://<deployment>.convex.site/photon/webhook
```

Photon can't reach a local deployment directly: test deliveries against a
cloud deployment, or expose the local one through a public HTTPS tunnel.

## What each file does

| File | Role |
|---|---|
| `convex.config.ts` | Installs the component |
| `photon.ts` | One shared client; wires `onTurn` |
| `http.ts` | Mounts the webhook route |
| `agent.ts` | Turn handler — one invocation per conversational turn |
| `../../convex.json` | Root config: points `functions` here |

The echo agent in `agent.ts` is a placeholder for an LLM call. The important
part is the shape around it: re-check `isCancelled` after anything slow, then
`sendMessage` and `completeTurn`.
