/// <reference types="vite/client" />

import type { GenericSchema, SchemaDefinition } from "convex/server";
import type { TestConvex } from "convex-test";
import schema from "./component/schema.js";

// `import.meta.glob` is a Vite transform, so this entry point ships as
// source: the caller's bundler has to rewrite it.
const modules = import.meta.glob("./component/**/*.ts");

/**
 * Register the component with a `convex-test` instance.
 * @param t - The test instance, e.g. from calling `convexTest`.
 * @param name - The component's name, as installed in `convex.config.ts`.
 */
export function register(
  t: TestConvex<SchemaDefinition<GenericSchema, boolean>>,
  name = "photon"
): void {
  t.registerComponent(name, schema, modules);
}

export default { register, schema, modules };
