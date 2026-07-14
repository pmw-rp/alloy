# CLAUDE.md

## Purpose of this fork

This repo is a fork of [grafana/alloy](https://github.com/grafana/alloy) maintained specifically to build **one Alloy binary** that contains every upstream component plus a set of custom, in-house components. `main` is the single integration branch — it always contains upstream Alloy plus every custom component below, and it's what actually gets built and shipped. There is no separate "vendor" or "clean" branch; `main` *is* the combined tree.

## Custom components maintained here

| Component | Path | Notes |
|---|---|---|
| `otelcol.exporter.kafka_router` | `internal/component/otelcol/exporter/kafka_router/` | Routes OTLP metrics/logs to Kafka topics via an ordered, templated route list with per-route fallback topics. Stability: `experimental`. |
| `discovery.redpanda` | `internal/component/discovery/redpanda/` | Enriches discovered pod targets with per-pod Redpanda cluster UUIDs and a stable pod ordinal, for multi-cluster sharding without gossip. Stability: `generally-available`. |
| `otelcol.processor.metricsbatcher` | `internal/component/otelcol/processor/metricsbatcher/` | Batches metrics while keeping all data points for the same (resource, metric name) group together, so a histogram series can't be split across batches. Stability: `experimental`. |

Update this table whenever a component is added, renamed, or removed — it's the source of truth for what this fork carries beyond upstream.

## Remotes

- `origin` — this fork, push/pull target for `main`.
- `upstream` — `https://github.com/grafana/alloy.git`, read-only. Fetched for new releases, never pushed to, nothing tracks it directly.

```sh
git remote add upstream https://github.com/grafana/alloy.git
git fetch upstream --tags
```

## Branch model

- `main` is the only long-lived branch. It tracks `origin/main`, **not** `upstream/main`.
- `main` permanently diverges from `upstream/main` — that divergence is the entire point of this fork, not a problem to fix. Being N commits ahead of `upstream/main` forever (and growing) is the expected, healthy state. Check the gap any time with:
  ```sh
  git rev-list --left-right --count HEAD...upstream/main
  ```
- Every change to a component (new component, or an edit to an existing one) happens on a short-lived branch cut from `main`, then merged back:
  ```sh
  git checkout -b component/<name> main
  # ... make changes, commit ...
  git checkout main
  git merge --no-ff component/<name>
  git push origin main
  git branch -d component/<name>
  ```
- Always use `--no-ff` so a component's arrival/change is one identifiable merge commit in `main`'s history, even if the branch itself had several commits.
- Branches are disposable scaffolding, not a permanent home for a component — delete them once merged. The component's real home is always `main`. Reuse the same branch name next time, or vary it per change; both are fine.

## Where custom component code lives

- Every custom component goes in a leaf directory under `internal/component/` that upstream Alloy does not have (e.g. `internal/component/otelcol/exporter/kafka_router/`). As long as the leaf directory name is unique to us, upstream will never create or touch files inside it — merging upstream in never conflicts with a component's own logic, no matter how much upstream changes around it.
- The one integration point where our changes and upstream's changes can collide is `internal/component/all/all.go` — the blank-import list that registers every component into the binary. One alphabetically-sorted import line per component. Conflicts here are shallow (both sides adding a nearby line) and always resolvable by keeping both.
- If a component pulls in a new dependency, `go.mod`/`go.sum` may also need attention — see "Upgrading the underlying Alloy version" below.
- The module path is left as `github.com/grafana/alloy` (unchanged) so that every file we haven't touched diffs identically against upstream — this is what keeps merges clean.

## Adding or changing a custom component

1. Branch off `main`: `git checkout -b component/<name> main`
2. Put the component's package in its own directory under `internal/component/...`.
   - If porting from another source tree, treat it as **read-only**: only ever copy from it, never modify or delete anything there.
   - Expect API drift if the source was written against an older Alloy version — don't assume it compiles as-is. Check how current sibling components in the same directory do things before porting code over. (Concrete example hit while adding `kafka_router`: component logging moved from go-kit's `level.Warn(logger).Log(...)` to calling methods directly on a stdlib `*slog.Logger` exposed as `component.Options.Logger` — see `internal/component/registry.go`.)
3. Add one import line to `internal/component/all/all.go`, alphabetically.
4. Verify it actually registers in the real combined binary — a package-level `go build` isn't sufficient proof:
   ```sh
   go build -o /tmp/alloy-test ./collector      # the real "alloy" main package
   go run ./internal/cmd/listcomponents | grep <component-name>
   ```
   Note: the shipped Alloy binary is built from `./collector` (an OpenTelemetry Collector Builder distribution whose root command is `flowcmd.RootCommand()`), **not** from a `./cmd/alloy` package — that path doesn't exist in this repo.
5. Commit on the component branch. Write the commit message as if the feature is new to this repo — describe what it does, don't reference the internal source tree it came from.
6. Merge to `main` (`--no-ff`), push, delete the branch (see "Branch model" above).
7. Update the component table at the top of this file.

## Upgrading the underlying Alloy version

1. `git fetch upstream --tags`
2. Merge a specific release tag into `main` — not `upstream/main` — so upgrades are deliberate and map to a real upstream changelog:
   ```sh
   git merge v1.18.0
   ```
3. Resolve conflicts. Expect them in at most two places:
   - `internal/component/all/all.go` — keep both sides' import lines, re-sort alphabetically.
   - `go.mod` — keep both sides' `require` lines. **Do not hand-resolve `go.sum` conflicts** — delete `go.sum` and regenerate with `go mod tidy`; it's a generated lockfile and line-based resolution on it is meaningless.
4. If any `require` line in `go.mod` changed, also run `make generate-otel-collector-distro` and confirm there's no further diff — CI's `check` job enforces this (see root `AGENTS.md`).
5. Rebuild and re-verify every custom component registers, as in step 4 of "Adding or changing a custom component."
6. Run `make lint` and the test suite before pushing.
7. Push `main` to `origin`, and tag the result per the "Tagging convention" below (e.g. `v1.18.0-rp`) for future bisecting and image builds.

Tip: `git config rerere.enabled true` — the same conflict shape (the `all.go` import block) recurs every upgrade; rerere remembers and reapplies prior resolutions automatically, so repeat conflicts get cheaper over time.

## Tagging convention

Tag the commit on `main` you're about to build/publish as `<upstream-release>-rp` (e.g. `v1.17.1-rp` for the commit that's `v1.17.1` plus our components). Push the tag: `git push origin <tag>`. This is the same string used as `VERSION` when building the image (see below), so `alloy --version` on a running container tells you exactly which upstream release plus which of our commits it was built from.

`main`'s base was originally cut from an arbitrary point on upstream's `main` branch (a pre-release dev snapshot), not a tagged release. It was later realigned with `git rebase --rebase-merges --onto v1.17.1 <old-base>` so the tag above would mean something concrete. That realignment was a one-time correction — see "Why merge, not rebase" below for why it isn't the normal upgrade path.

## Building and publishing an image

The Docker image build is fully self-contained — the `Dockerfile` builds the UI, compiles the binary, and assembles the runtime image in one multi-stage `docker build`. No local `go build` is needed first.

```sh
make alloy-image VERSION=v1.17.1-rp ALLOY_IMAGE=<registry>/<image>:<tag>
```

- `VERSION` is baked into the binary (`alloy --version`) and should match the tag from "Tagging convention" above. If `HEAD` has an exact-match `v*` git tag, `scripts/image-tag` infers `VERSION` automatically — passing it explicitly is just for clarity.
- `ALLOY_IMAGE` only controls the local image tag `docker build` produces; there's no `make push` target. Push manually once the build finishes:
  ```sh
  docker push <registry>/<image>:<tag>
  ```
- An already-built image can be retagged for a different registry without rebuilding: `docker tag <local-tag> <new-registry>/<image>:<tag>`.
- Requires `docker login` to whichever registry you're pushing to first.
- Registry: currently Docker Hub, under `paulmw/alloy`. This is expected to change — update this line when it does.

## Why merge, not rebase

`main` is merged forward from upstream release tags rather than rebased onto them. This means history on `main` is never rewritten: pushes are always plain pushes, never force-pushes, and anyone else with a clone can `git pull` normally. It also means each upstream sync is resolved once, rather than potentially re-resolving the same conflict once per historical commit that touches a shared file.
