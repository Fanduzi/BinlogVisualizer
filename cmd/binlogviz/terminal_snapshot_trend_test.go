package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotOutputModesPreserveEffects(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Setenv("BINLOGVIZ_ERROR_FORMAT", "")
	for _, mode := range terminalOutputModes() {
		t.Run(mode.name, func(t *testing.T) {
			report := writeSnapshotSourceReport(t, true)
			dir := t.TempDir()
			for _, step := range []struct {
				args   []string
				want   string
				exists string
				absent string
			}{
				{[]string{"snapshot", "save", report, "--name", "first", "--snapshot-dir", dir}, "Saved snapshot", "first.json", ""},
				{[]string{"snapshot", "rename", "first", "second", "--snapshot-dir", dir}, "Renamed snapshot", "second.json", "first.json"},
				{[]string{"snapshot", "delete", "second", "--snapshot-dir", dir}, "Deleted snapshot", "", "second.json"},
			} {
				args := append([]string{"--lang", "en"}, mode.flags...)
				args = append(args, step.args...)
				code, stdout, stderr := runRoot(t, args...)
				if code != 0 || stdout != "" {
					t.Fatalf("%v: exit=%d stdout=%q stderr=%q", step.args, code, stdout, stderr)
				}
				if mode.name == "normal" {
					if !strings.Contains(stderr, step.want) {
						t.Errorf("missing confirmation %q: %q", step.want, stderr)
					}
				} else if stderr != "" {
					t.Errorf("confirmation should be suppressed: %q", stderr)
				}
				if step.exists != "" {
					if _, err := os.Stat(filepath.Join(dir, step.exists)); err != nil {
						t.Errorf("expected snapshot %s: %v", step.exists, err)
					}
				}
				if step.absent != "" {
					if _, err := os.Stat(filepath.Join(dir, step.absent)); !os.IsNotExist(err) {
						t.Errorf("snapshot %s should be absent: %v", step.absent, err)
					}
				}
			}
		})
	}
}

func TestTrendOutputModesPreserveTimeOrder(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Setenv("BINLOGVIZ_ERROR_FORMAT", "")
	for _, mode := range terminalOutputModes() {
		t.Run(mode.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, fixture := range []trendSnapshotFixture{
				{Name: "later", Label: "Later", StartTime: "2026-03-21T10:00:00Z", EndTime: "2026-03-21T10:30:00Z", Rows: 3000, Txns: 10, Events: 20},
				{Name: "earlier", Label: "Earlier", StartTime: "2026-03-19T10:00:00Z", EndTime: "2026-03-19T10:30:00Z", Rows: 1000, Txns: 5, Events: 10},
			} {
				writeSnapshotFixture(t, dir, fixture.Name, trendSnapshotFixtureJSON(fixture))
			}
			args := append([]string{"--lang", "en"}, mode.flags...)
			args = append(args, "trend", "later", "earlier", "--snapshot-dir", dir, "--format", "json", "--order", "time")
			code, stdout, stderr := runRoot(t, args...)
			if code != 0 {
				t.Fatalf("exit=%d stderr=%q", code, stderr)
			}
			var result struct {
				Order     string `json:"order"`
				Reordered bool   `json:"reordered"`
				Points    []struct {
					Snapshot struct {
						Name string `json:"name"`
					} `json:"snapshot"`
				} `json:"points"`
			}
			if err := json.Unmarshal([]byte(stdout), &result); err != nil {
				t.Fatalf("invalid report JSON: %v: %q", err, stdout)
			}
			if result.Order != "time" || !result.Reordered || len(result.Points) != 2 || result.Points[0].Snapshot.Name != "earlier" || result.Points[1].Snapshot.Name != "later" {
				t.Errorf("wrong time order: %+v", result)
			}
			if mode.name == "normal" {
				if !strings.Contains(stderr, "reordered by window start_time") {
					t.Errorf("missing reorder notice: %q", stderr)
				}
			} else if stderr != "" {
				t.Errorf("reorder notice should be suppressed: %q", stderr)
			}
		})
	}
}
