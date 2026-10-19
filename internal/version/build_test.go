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

func TestFromSettingsFallsBackToLinkerValues(t *testing.T) {
	// -buildvcs=false (the manylinux release build) embeds no vcs.*; the
	// release passes the commit with -ldflags -X instead (ADR 0010).
	got := resolve(Build{Version: "0.27.0"}, []debug.BuildSetting{{Key: "-buildvcs", Value: "false"}},
		"d7b4fd2fdfe344062d1bb54ee6f67c00831671c3", "2026-10-19T01:34:16Z")
	want := Build{Version: "0.27.0", Commit: "d7b4fd2fdfe344062d1bb54ee6f67c00831671c3", CommitTime: "2026-10-19T01:34:16Z"}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestFromSettingsPrefersBuildInfoOverLinkerValues(t *testing.T) {
	got := resolve(Build{Version: "0.27.0"}, []debug.BuildSetting{
		{Key: "vcs.revision", Value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{Key: "vcs.time", Value: "2026-10-19T01:00:00Z"},
		{Key: "vcs.modified", Value: "false"},
	}, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "2026-10-18T00:00:00Z")
	if got.Commit != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || got.CommitTime != "2026-10-19T01:00:00Z" {
		t.Fatalf("build info must win over linker values, got %+v", got)
	}
}

func TestFromSettingsWithoutVCSOrLinkerValues(t *testing.T) {
	got := resolve(Build{Version: "dev"}, nil, "", "")
	if got.Commit != "" || got.CommitTime != "" || got.Modified {
		t.Fatalf("want empty VCS fields, got %+v", got)
	}
}
