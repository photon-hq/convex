import photon from "@photon-ai/convex/convex.config";
import { defineApp } from "convex/server";

const app = defineApp();
app.use(photon);

export default app;
