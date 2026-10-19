package version

import (
	"runtime/debug"
	"testing"
)

func TestFromSettingsReadsVCS(t *testing.T) {
	got := fromSettings(Build{Version: "0.26.1"}, []debug.BuildSetting{
		{Key: "vcs", Value: "git"},
		{Key: "vcs.revision", Value: "0fcd52e1a2b3c4d5e6f708192a3b4c5d6e7f8091"},
		{Key: "vcs.time", Value: "2026-10-17T10:00:00Z"},
		{Key: "vcs.modified", Value: "true"},
	})
	want := Build{Version: "0.26.1", Commit: "0fcd52e1a2b3c4d5e6f708192a3b4c5d6e7f8091", CommitTime: "2026-10-17T10:00:00Z", Modified: true}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestFromSettingsWithoutVCS(t *testing.T) {
	got := fromSettings(Build{Version: "dev"}, []debug.BuildSetting{{Key: "-trimpath", Value: "true"}})
	if got.Commit != "" || got.CommitTime != "" || got.Modified {
		t.Fatalf("want empty VCS fields, got %+v", got)
	}
}
