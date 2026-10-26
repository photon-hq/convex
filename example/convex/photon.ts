import { PhotonComponent } from "@photon-ai/convex";
import { components, internal } from "./_generated/api";

/**
 * One shared client for the whole app.
 *
 * Credentials come from the environment variables PHOTON_PROJECT_ID,
 * PHOTON_API_KEY and PHOTON_WEBHOOK_SECRET, plus the optional PHOTON_BASE_URL.
 * `onTurn` is passed as a function reference — the component turns it into a
 * function handle so it can call back into app code from its own scheduled
 * jobs.
 */
// Annotated explicitly to break a circular inference: this module builds the
// client from `internal.*`, while agent.ts imports `photon` back. Without the
// annotation TypeScript reports TS7022.
export const photon: PhotonComponent = new PhotonComponent(components.photon, {
  onTurn: internal.agent.respond, // called once per turn of received messages
});
