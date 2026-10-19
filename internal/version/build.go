package version

import (
	"runtime"
	"runtime/debug"
)

// Commit and CommitTime are the fallback build identity a release passes with
// -ldflags "-X binlogviz/internal/version.Commit=<sha> -X
// binlogviz/internal/version.CommitTime=<RFC 3339>". They are used only when
// the binary carries no vcs.* build info, as with -buildvcs=false or a build
// outside a git checkout (ADR 0010).
var (
	Commit     = ""
	CommitTime = ""
)

// Build identifies the running binary: the release version plus the git
// commit it was built from (ADR 0010).
type Build struct {
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	CommitTime string `json:"commit_time"`
	Modified   bool   `json:"modified"`
	GoVersion  string `json:"go_version"`
}

// Current returns the running binary's Build. Commit and CommitTime come from
// the vcs.* build info Go embeds, else from the ldflags-injected Commit and
// CommitTime, else "" (go run, or a build with neither).
func Current() Build {
	b := Build{Version: Version, GoVersion: runtime.Version()}
	var settings []debug.BuildSetting
	if info, ok := debug.ReadBuildInfo(); ok {
		settings = info.Settings
	}
	return resolve(b, settings, Commit, CommitTime)
}

// resolve prefers the embedded build info and falls back to linker values.
func resolve(b Build, settings []debug.BuildSetting, linkCommit, linkCommitTime string) Build {
	b = fromSettings(b, settings)
	if b.Commit == "" {
		b.Commit = linkCommit
		b.CommitTime = linkCommitTime
	}
	return b
}

func fromSettings(b Build, settings []debug.BuildSetting) Build {
	for _, s := range settings {
		switch s.Key {
		case "vcs.revision":
			b.Commit = s.Value
		case "vcs.time":
			b.CommitTime = s.Value
		case "vcs.modified":
			b.Modified = s.Value == "true"
		}
	}
	return b
}
