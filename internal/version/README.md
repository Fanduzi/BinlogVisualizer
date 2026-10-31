# Build version stamping

The single source of truth for the binlogviz version string (ADR 0010).
`Version` is injected at build time via `-ldflags -X` (goreleaser or a
manual build) and defaults to `"dev"` without it. `Current` assembles the
running binary's build identity from the VCS info Go embeds, falling back
to the ldflags-injected commit fields when a build carries none
(`-buildvcs=false`, or a build outside a git checkout).

## Members

| File | Responsibility |
|------|----------------|
| `version.go` | `var Version = "dev"` — the ldflags injection point. |
| `build.go` | `Commit`/`CommitTime` fallback ldflags vars, the `Build` struct (`version`, `commit`, `commit_time`, `modified`, `go_version`), and `Current`, which prefers `vcs.revision`/`vcs.time`/`vcs.modified` build settings and uses the linker values only when the embedded revision is absent. |
| `build_test.go` | Verifies VCS-info precedence, the ldflags fallback, and the empty identity case. |

## Interfaces

| API | Contract |
|-----|----------|
| `Version` | The release version string injected via `-X binlogviz/internal/version.Version=...`; `"dev"` otherwise. |
| `Commit`, `CommitTime` | Fallback build identity a release injects with `-X`; used only when the binary carries no `vcs.*` build info. |
| `Build` | The running binary's identity: version, commit, commit time, modified flag, Go version. |
| `Current() Build` | Embedded `vcs.*` info first, then the linker `Commit`/`CommitTime`, else empty (`go run`, or a build with neither). `GoVersion` always comes from `runtime.Version()`. |

## Dependencies

- Upstream: standard library only (`runtime`, `runtime/debug`); `.goreleaser.yml` sets the ldflags.
- Downstream: `cmd/binlogviz` — `--version` prints `Version`, `version` prints the `Current` build, analyze options and snapshots stamp `binlogviz_version`, and the `version` command's schema is built from `Build`.
