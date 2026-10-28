import { execFileSync } from "node:child_process";
import {
  access,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

// Buildspace's package-stage workflow runs this twice from the same checkout:
// for the staging build and for its production candidate. Each run packs the
// package.json version plus RELEASE_SUFFIX into PACK_DESTINATION and restores
// package.json. The checks below are the only test the production candidate
// gets before it is stored.
const STABLE_VERSION = /^\d+\.\d+\.\d+$/;

const destination = process.env.PACK_DESTINATION;
if (!destination) {
  throw new Error("PACK_DESTINATION is required");
}
const original = await readFile("package.json", "utf8");
const manifest = JSON.parse(original);
if (!STABLE_VERSION.test(manifest.version)) {
  throw new Error("package.json must declare a stable X.Y.Z version");
}
const version = `${manifest.version}${process.env.RELEASE_SUFFIX ?? ""}`;

try {
  await writeFile(
    "package.json",
    `${JSON.stringify({ ...manifest, version }, null, 2)}\n`
  );
  execFileSync("npm", ["run", "build"], { stdio: "inherit" });
  execFileSync(
    "npm",
    ["pack", "--ignore-scripts", "--pack-destination", destination],
    { stdio: "inherit" }
  );
} finally {
  await writeFile("package.json", original);
}

const exportTargets = (entry) =>
  typeof entry === "string"
    ? [entry]
    : Object.values(entry).flatMap(exportTargets);

const archive = join(destination, `photon-ai-convex-${version}.tgz`);
await mkdir("node_modules/.cache", { recursive: true });
const unpacked = await mkdtemp(resolve("node_modules/.cache/pack-release-"));
try {
  execFileSync("tar", ["-xzf", archive, "-C", unpacked]);
  const root = join(unpacked, "package");
  const packed = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  if (packed.name !== manifest.name || packed.version !== version) {
    throw new Error(`The archive holds ${packed.name}@${packed.version}`);
  }
  // Every file an export names is in the archive, and both JavaScript entry
  // points load against this checkout's dependencies.
  await Promise.all(
    exportTargets(packed.exports).map((target) => access(join(root, target)))
  );
  const client = await import(
    pathToFileURL(join(root, packed.exports["."].default)).href
  );
  if (typeof client.PhotonComponent !== "function") {
    throw new Error("The packed client does not export PhotonComponent");
  }
  await import(
    pathToFileURL(join(root, packed.exports["./convex.config"].default)).href
  );
} finally {
  await rm(unpacked, { force: true, recursive: true });
}
