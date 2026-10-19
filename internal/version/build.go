package version

import (
	"runtime"
	"runtime/debug"
)

// Build identifies the running binary: the release version plus the git
// commit Go embedded at build time (ADR 0010).
type Build struct {
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	CommitTime string `json:"commit_time"`
	Modified   bool   `json:"modified"`
	GoVersion  string `json:"go_version"`
}

// Current returns the running binary's Build. Commit and CommitTime are ""
// when the binary was built without VCS info (go run, or outside a checkout).
func Current() Build {
	b := Build{Version: Version, GoVersion: runtime.Version()}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return b
	}
	return fromSettings(b, info.Settings)
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
