// Package version holds the binlogviz version injected at build time.
// input: ldflags from goreleaser or manual builds.
// output: a single source of truth for the binlogviz version string.
// pos: build-time version injection used by CLI commands and release artifacts.
// note: if this file changes, update .goreleaser.yml ldflags and the version command.
package version

// Version is the binlogviz version. It is injected at build time via ldflags.
// When built without ldflags, it defaults to "dev".
var Version = "dev"
