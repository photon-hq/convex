# Publishing

`@photon-ai/convex` is published with Photon's shared stage-and-promote
workflows from [buildspace](https://github.com/photon-hq/buildspace#package-stage-and-promote).
Every push to `main` publishes a staging build to npm, and a person promotes a
staging build to production. Production gets the exact files staging tested;
nothing is rebuilt.

## Staging: every push to `main`

`.github/workflows/publish.yml` (**Publish staging**) waits for `ci.yml` to pass
on the pushed commit, runs `pnpm install` and the checks once, then runs
`scripts/pack-release.mjs` twice from the same checkout:

- the staging build, `X.Y.Z-staging.<run id>.<attempt>`, which it publishes to
  npm under the `staging` dist-tag;
- the production candidate, `X.Y.Z`, which it stores beside the staging build
  in a GitHub prerelease named `convex-staging-<staging version>`.

Install a staging build with `npm install @photon-ai/convex@staging`.

## Versions

`X.Y.Z` is computed; the `version` in `package.json` is not read. Each staging
build starts from the latest `vX.Y.Z` release and takes the largest bump that
the pull requests merged to `main` since then ask for, by their titles
(Conventional Commits): `feat` bumps the minor version, `type!:` or a
`BREAKING CHANGE:` line in the description the major version, and anything else
the patch version. Below 1.0, a breaking change releases `1.0.0`. Merges between
two promotions don't add up. A `Release-As: X.Y.Z` line in a pull request's
description raises the version to at least `X.Y.Z`. With no release yet, the
first version is `0.1.0`.

## Production: Promote to production

1. In Actions, run **Promote to production** on `main`. Leave the version blank
   to promote what `staging` points to, or name a staging version:
   `gh workflow run promote.yml -f staging_version=<staging version>`.
2. Its first job only checks the build, and its summary lists the version and
   the commits since the current `latest`.
3. The run then waits for a reviewer of the `production` environment.
4. Once approved, it publishes the stored candidate under `latest`, tags the
   source commit `vX.Y.Z`, and creates the GitHub release.

## Setup

Done once, outside the code, in this order:

- **A public repository**: buildspace publishes to npmjs with `--provenance`,
  and npm doesn't support provenance from private repositories.
- **GitHub environments** (a repository admin, Settings → Environments):
  `staging` with a branch rule for `main`, and `production` with a required
  reviewer and a branch rule for `main`. Promotion refuses to run until
  `production` has a reviewer.
- **The package on npm** (an `@photon-ai` maintainer): npm trusts a publisher
  only for a package that already exists, so publish a placeholder version by
  hand first, from a checkout of `main`. Use `0.0.0`, not `0.1.0`: `0.1.0` is
  the first version the workflows compute, and promotion refuses a version
  that is already published with other contents. Until `0.1.0` is promoted,
  `latest` points to the placeholder.

  ```sh
  npm pkg set version=0.0.0
  npm publish --access public
  git checkout package.json
  ```

- **npm trusted publishers** (the same maintainer, on npmjs.com): one for
  `publish.yml` with environment `staging`, and one for `promote.yml` with
  environment `production`, each allowing `npm publish` and `npm dist-tag`.
  Both workflows publish through trusted publishing, with provenance and no
  token.

Renaming either workflow file or the repository breaks publishing: each
trusted publisher is bound to them, and has to be deleted and created again.

## What ships

`files` is `["dist", "src"]`. Both are required; `src` is not just for source
maps:

- `./test` resolves to `src/test.ts`, because its `import.meta.glob` has to be
  transformed by the consumer's bundler.

`src/component/_generated/` is committed and ships as-is. `component.ts` in that
directory is the `ComponentApi` type the client re-exports, so a stale copy
ships a type that no longer matches the component's validators. Regenerate with
`pnpm run build:codegen` (needs a Convex deployment) and commit it whenever a
component validator changes. CI regenerates it against a local anonymous backend
and fails when the committed copy is stale.
