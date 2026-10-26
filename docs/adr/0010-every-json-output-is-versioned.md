# ADR 0010: Every JSON output is versioned, and the build says what it is

Status: accepted (2026-10-19). Tracks #282 (the versioning part and item 1). Items 2 and 3 of #282 (a diagnostics cap and a GTID on alerts) are separate follow-ups; item 4 was done in #296.

## Context

Only two JSON outputs carry a contract version: `analyze --format json` (`report_version` 3) and `flashback --plan` (`plan_version` 2). `compare`, `trend`, `snapshot list|show` and every `workflow` JSON summary have none, so a script or agent cannot detect a breaking change; it can only notice that a key went missing. `workflow status` shows `manifest_version`, but that is the version of the manifest it read, not of the status document.

`binlogviz version` prints a banner and `binlogviz 0.26.1`. It has no commit and no machine-readable form, so an agent cannot confirm which build it is driving.

## Decision

1. **Every JSON document on stdout has a top-level integer version key.** The two existing keys keep their names, because renaming them would break every consumer:

   | Output | Version key | Current |
   |---|---|---|
   | `analyze --format json` (and saved snapshots) | `report_version` | 3 |
   | `flashback --plan` | `plan_version` | 2 |
   | `compare --format json` | `schema_version` | 1 |
   | `trend --format json` | `schema_version` | 1 |
   | `snapshot list --format json`, `snapshot show --format json` | `schema_version` | 1 |
   | `workflow validate|describe|status|clean|export --format json` | `schema_version` | 1 |
   | `version --format json` | `schema_version` | 1 |
| `flashback verify --format json` | `schema_version` | 1 |

   `schema_version` is the first key of the object. Each output owns its number and bumps it independently, only for a breaking change (a key removed, renamed, or changed in type or meaning). Adding a key is not breaking. A bump is recorded in CHANGELOG as breaking.
2. **One writer adds it.** `internal/jsonout.Marshal(version, payload)` encodes the payload, requires a JSON object that does not already have `schema_version`, and puts `schema_version` first. Commands do not add the key by hand. `TestEveryJSONOutputIsVersioned` runs each JSON-emitting command and fails if its stdout lacks the key, so a new output cannot ship without one.
3. **Compare and trend artifacts written by `workflow run` carry the same key**, because they go through the same renderer. The workflow summary reads those artifacts back into lenient structs that ignore unknown keys, so old and new artifacts both read; the ledger hashes files as written. `workflow status`, `resume` and `clean` apply the consumer rule to them (`workflow.CheckArtifactVersions`): a recorded compare or trend JSON artifact whose `schema_version` is not an integer from 1 to `compare.SchemaVersion` / `trend.SchemaVersion` exits 9 (`unsupported`, `details.reason: unsupported_artifact_version`, plus `artifact`, `step_kind`, `step_name`, `schema_version`, `supported_version`), the same convention as `report_version` and `manifest_version`. Nothing is printed on stdout, run or moved. An artifact without the key predates this ADR and is read as before. `clean` refuses too, because an older build must not decide what a newer run owns.
4. **Not in scope.** The one-line `{"error":…}` (ADR 0009) and `{"warning":…}` lines on stderr are diagnostics, not documents; their shape is fixed by ADR 0009 and does not change here. The workflow manifest keeps `manifest_version`.
5. **`version --format json`** prints `schema_version`, `version`, `commit`, `commit_time`, `modified` and `go_version`. The commit fields come from the Go build info (`vcs.revision`, `vcs.time`, `vcs.modified`), which `go build` embeds from a git checkout. When the binary has no `vcs.revision`, `commit` and `commit_time` fall back to `-ldflags "-X binlogviz/internal/version.Commit=<sha> -X binlogviz/internal/version.CommitTime=<RFC 3339 UTC>"`; build info always wins when present, and `modified` comes only from build info. Without either (for example `go run`, or a build outside a checkout with no ldflags), `commit` and `commit_time` are `""` and `modified` is `false`.

   **Release builds do need these ldflags on Linux.** The macOS release jobs build natively in the checkout, so build info is embedded. The Linux jobs build inside the manylinux2014 container with `go build -buildvcs=false` (added in v0.8.2 after the container's `go build` failed with `error obtaining VCS status: exit status 128`), which embeds no VCS info, so Linux release binaries up to v0.35.0 report `commit: ""` (#303). `.github/workflows/release.yml` now resolves `GITHUB_SHA` and its commit time (UTC) outside the container, passes them into every release `go build` as the ldflags above, and runs `scripts/release_smoke.sh` with `BINLOGVIZ_SMOKE_EXPECT_COMMIT=${GITHUB_SHA}` and `BINLOGVIZ_SMOKE_REQUIRE_COMMIT=1`, so a release whose binary does not report the tagged commit fails before publishing. CI's `verify` job builds the same way (`-buildvcs=false` plus the ldflags) and smoke-tests the archive against `github.sha` on every PR. `.goreleaser.yml` carries the same ldflags. The text form keeps its banner and `binlogviz <version>` line and adds `commit <sha> (<time>)` when the commit is known. `--format` other than `text` or `json` is a usage error (exit 3). `--version` is unchanged.

## Consequences

- Breaking only for a consumer that rejects unknown keys; no output loses or changes a key.
- `analyze` and `flashback --plan` output is byte-identical.
