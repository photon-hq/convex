import { httpRouter } from "convex/server";
import { photon } from "./photon";

const http = httpRouter();

// POST https://<deployment>.convex.site/photon/webhook
// Point a Photon webhook destination at this URL and set
// PHOTON_WEBHOOK_SECRET in the Convex dashboard.
photon.registerRoutes(http);

export default http;
