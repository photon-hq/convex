# @photon-ai/convex

## 0.1.0

Initial release, for Photon Beta (2.0) only. Photon Stable keeps
`@spectrum-ts/convex`.

- Receives `message.received` webhooks, verified with Standard Webhooks, for
  webhook API version `2026-07-01`.
- Durable inbound pipeline: at-least-once dedupe, immediate or collapsed turns,
  and turn cancellation with carry-forward.
- Sends through the Photon API from the component itself — `sendMessage`,
  `markMessageRead` and `setSpaceTyping` — via an outbox with deterministic
  idempotency keys, pacing, and retry/backoff.
- `@photon-ai/convex/test` exports `register()` for apps testing against the
  component with `convex-test`.
