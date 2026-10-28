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

// The F3 lock above only means anything if the e2e job itself cannot be
// silenced: an if: on flashback-e2e skips it on tag pushes, continue-on-error
// turns its failure green, and if: always() on the downstream jobs lets them
// run over a failed ci gate.
func TestFlashbackE2EJobCannotBeSilenced(t *testing.T) {
	root := filepath.Join("..", "..")
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(root, ".github", "workflows", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	ci, release := read("ci.yml"), read("release.yml")

	e2e := ciJob(t, ci, "flashback-e2e")
	if regexp.MustCompile(`(?m)^    if:`).MatchString(e2e) {
		t.Fatalf("flashback-e2e has a job-level if: it can be skipped on a tag push while ci still counts green:\n%s", e2e)
	}
	if strings.Contains(e2e, "continue-on-error") {
		t.Fatal("flashback-e2e must fail the workflow, not continue-on-error")
	}
	for _, name := range []string{"build", "release"} {
		if strings.Contains(ciJob(t, release, name), "if: always()") {
			t.Fatalf("release.yml's %s job has if: always(), so it runs even when the ci gate failed", name)
		}
	}
}

// The vet-race and guard-SQL wiring is what makes the e2e gate the real one:
// vet and -race run on every PR, and the e2e job must keep both the failed-
// apply test (a broken apply still signals) and the on-server guard tests
// (the emitted SQL's message cut actually works on MySQL).
func TestVetRaceAndGuardWiringPresent(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	ci := string(b)
	vet := ciJob(t, ci, "vet-race")
	for _, want := range []string{"go vet ./...", "go test -race"} {
		if !strings.Contains(vet, want) {
			t.Fatalf("ci.yml's vet-race job lost %q:\n%s", want, vet)
		}
	}
	e2e := ciJob(t, ci, "flashback-e2e")
	for _, want := range []string{"TestFlashbackFailedApplyDropsAndSignals", "Guard SQL", "internal/guard", "OnServer"} {
		if !strings.Contains(e2e, want) {
			t.Fatalf("ci.yml's flashback-e2e job lost %q:\n%s", want, e2e)
		}
	}
}

// release_smoke checks the archive's reported commit but not `modified`, so
// the build job itself must prove the checkout is the tagged commit and is
// clean before either build mode runs.
func TestBuildVerifiesCheckoutMatchesTag(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	build := ciJob(t, string(b), "build")
	for _, want := range []string{"git rev-parse HEAD", "git status --porcelain"} {
		if !strings.Contains(build, want) {
			t.Fatalf("release.yml's build job lost the checkout verification %q:\n%s", want, build)
		}
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
	// The services are pinned patch releases and a step asserts each version:
	// a floating mysql:8.0 tag can drift to a server the checker fails on.
	for _, pin := range []string{"mysql:8.0.", "mysql:8.4."} {
		if !strings.Contains(job, "image: "+pin) {
			t.Fatalf("flashback difftest %s service is not pinned", pin)
		}
	}
	if !strings.Contains(job, "Assert the MySQL service versions") || !strings.Contains(job, "SELECT VERSION()") {
		t.Fatal("flashback difftest does not assert the server versions")
	}
}

// The nightly corpus run is a separate scheduled workflow with
// BINLOGVIZ_DIFFTEST_NIGHTLY=1 on the same pinned services.
func TestNightlyDiffTestWorkflow(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "difftest-nightly.yml"))
	if err != nil {
		t.Fatal(err)
	}
	wf := string(b)
	if !strings.Contains(wf, "schedule:") || !strings.Contains(wf, "cron:") {
		t.Fatal("difftest-nightly has no schedule")
	}
	if strings.Count(wf, `BINLOGVIZ_DIFFTEST_NIGHTLY: "1"`) < 2 {
		t.Fatal("difftest-nightly does not enlarge the corpus on both servers")
	}
	for _, pin := range []string{"mysql:8.0.", "mysql:8.4."} {
		if !strings.Contains(wf, "image: "+pin) {
			t.Fatalf("difftest-nightly %s service is not pinned", pin)
		}
	}
}

// TestReleaseWaitsForReleasePrep keeps the release-prep gate in front of the
// build: release.yml's build job must need release-prep, and release-prep
// must check the tag with full history (tags fetched) after its self-test.
// ci.yml must run the self-test on every PR.
func TestReleaseWaitsForReleasePrep(t *testing.T) {
	root := filepath.Join("..", "..")
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(root, ".github", "workflows", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	ci, release := read("ci.yml"), read("release.yml")

	needs := regexp.MustCompile(`(?m)^    needs: \[([^\]]*)\]`).FindStringSubmatch(ciJob(t, release, "build"))
	if needs == nil {
		t.Fatal("release.yml's build job has no list-form needs")
	}
	found := false
	for _, n := range strings.Split(needs[1], ",") {
		if strings.TrimSpace(n) == "release-prep" {
			found = true
		}
	}
	if !found {
		t.Fatalf("release.yml's build job does not need release-prep (needs: [%s]), so a tag can ship without notes, CHANGELOG or pins", needs[1])
	}
	prep := ciJob(t, release, "release-prep")
	for _, want := range []string{
		"fetch-depth: 0",
		"bash scripts/check-release-prep-test.sh",
		`bash scripts/check-release-prep.sh "${GITHUB_REF_NAME}" "${GITHUB_SHA}"`,
	} {
		if !strings.Contains(prep, want) {
			t.Fatalf("release.yml's release-prep job lost %q:\n%s", want, prep)
		}
	}
	if !strings.Contains(ciJob(t, ci, "release-prep-selftest"), "bash scripts/check-release-prep-test.sh") {
		t.Fatal("ci.yml no longer runs the release-prep self-test on every PR")
	}
}
