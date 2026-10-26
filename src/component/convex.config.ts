import { defineComponent } from "convex/server";

/**
 * The Photon component.
 *
 * Owns the durable inbound pipeline (dedupe → batch queue → debounced flush →
 * cancellation/carry-forward) and the outbox that drives deduped, paced sends
 * through the Photon API. Install it from the app's `convex/convex.config.ts`:
 *
 * ```ts
 * import { defineApp } from "convex/server";
 * import photon from "@photon-ai/convex/convex.config";
 *
 * const app = defineApp();
 * app.use(photon);
 * export default app;
 * ```
 *
 * The component itself declares no environment variables. Every secret is read
 * by app-owned code, from the *app's* deployment environment — set these in the
 * app's Convex dashboard, not the component's:
 *
 * - `PHOTON_WEBHOOK_SECRET` — verified in the webhook handler that
 *   `PhotonComponent.registerRoutes` mounts on the app's `httpRouter`. A
 *   component's HTTP actions cannot read app env vars, which is why the route
 *   lives there.
 * - `PHOTON_PROJECT_ID` / `PHOTON_API_KEY` (and the optional
 *   `PHOTON_BASE_URL`) — read by `PhotonComponent` in the app and passed to the
 *   component with every send. The component keeps the latest in its `config`
 *   table; send jobs are scheduled with only an outbox row id and read the key
 *   from there, so it stays out of scheduled-function arguments.
 */
const component = defineComponent("photon");

export default component;
