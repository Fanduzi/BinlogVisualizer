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

// The MySQL 8.0 and 8.4 differential job runs on every pull request. It is
// not continue-on-error, and a skip still fails the step (ADR 0020).
func TestDiffTestJobIsRequired(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	ci := string(b)
	job := ciJob(t, ci, "flashback-difftest")
	if !strings.Contains(job, "name: flashback difftest") {
		t.Fatal("ci.yml is missing the flashback difftest job")
	}
	if strings.Contains(job, "continue-on-error") {
		t.Fatal("flashback difftest must fail the workflow")
	}
	if strings.Count(job, `BINLOGVIZ_DIFFTEST: "1"`) < 2 || !strings.Contains(job, "mysql:8.0") || !strings.Contains(job, "mysql:8.4") {
		t.Fatal("flashback difftest does not run on MySQL 8.0 and 8.4")
	}
	if !strings.Contains(job, "TestDiffMySQLExpressions") || !strings.Contains(job, "TestFlashbackDiffMySQL") {
		t.Fatal("flashback difftest does not require the differential tests")
	}
}
