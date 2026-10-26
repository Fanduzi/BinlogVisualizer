package binlogviz

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestReleaseWaitsForFlashbackE2E is production-readiness F3: a tag is built
// and published only after the tagged commit passes CI's flashback e2e.
// release.yml must call ci.yml as a job, every build must need that job, and
// ci.yml must stay callable and keep its flashback-e2e job.
func TestReleaseWaitsForFlashbackE2E(t *testing.T) {
	root := filepath.Join("..", "..")
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(root, ".github", "workflows", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	ci, release := read("ci.yml"), read("release.yml")

	if !regexp.MustCompile(`(?m)^on:\n(?:  .*\n|\s*#.*\n)*  workflow_call:`).MatchString(ci) {
		t.Fatal("ci.yml is not callable (no on.workflow_call), so release.yml cannot run it on the tagged commit")
	}
	if !strings.Contains(ciJob(t, ci, "flashback-e2e"), "BINLOGVIZ_FLASHBACK_E2E=1") {
		t.Fatal("ci.yml's flashback-e2e job no longer runs the MySQL round trip")
	}
	gate := ciJob(t, release, "ci")
	if !regexp.MustCompile(`(?m)^    uses: \./\.github/workflows/ci\.yml\s*$`).MatchString(gate) {
		t.Fatalf("release.yml's ci job does not call ./.github/workflows/ci.yml:\n%s", gate)
	}
	build := ciJob(t, release, "build")
	needs := regexp.MustCompile(`(?m)^    needs: \[([^\]]*)\]`).FindStringSubmatch(build)
	if needs == nil {
		t.Fatalf("release.yml's build job has no list-form needs:\n%s", build)
	}
	found := false
	for _, n := range strings.Split(needs[1], ",") {
		if strings.TrimSpace(n) == "ci" {
			found = true
		}
	}
	if !found {
		t.Fatalf("release.yml's build job does not need ci (needs: [%s]), so a tag can ship without the flashback e2e", needs[1])
	}
	if !strings.Contains(ciJob(t, release, "release"), "needs: [build") {
		t.Fatal("release.yml's github-release job no longer waits for build")
	}
}
