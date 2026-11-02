package binlogviz

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The deprecated-mode warning is 5.7-only; an 8.x skip is not compatibility evidence.
func TestMySQL57CompatibilityJobCannotBeSilenced(t *testing.T) {
	job := mysql57CompatibilityJob(t)
	if regexp.MustCompile(`(?m)^    if:`).MatchString(job) || strings.Contains(job, "continue-on-error") {
		t.Fatal("MySQL 5.7 compatibility must block CI and tagged release builds on failure")
	}
	for _, want := range []string{
		"image: mysql:5.7.44@sha256:dab0a802b44617303694fb17d166501de279c3031ddeb28c56ecf7fcab5ef0da",
		`--health-cmd="mysql --protocol=tcp`,
		`-e 'SELECT 1'`,
		`${{ job.services.mysql57.id }}`,
		`BINLOGVIZ_FLASHBACK_E2E: "1"`,
		`BINLOGVIZ_MYSQL: docker exec -i -e MYSQL_PWD=compat57`,
	} {
		if !strings.Contains(job, want) {
			t.Fatalf("MySQL 5.7 compatibility job lost %q", want)
		}
	}
	if regexp.MustCompile(`(?m)^\s+ports:`).MatchString(job) {
		t.Fatal("the compatibility client must use its service container, not a host database port")
	}
}

func TestMySQL57CompatibilityRequiresActualPass(t *testing.T) {
	job := mysql57CompatibilityJob(t)
	for _, want := range []string{
		"go test -count=1 -timeout 4m -v ./cmd/binlogviz",
		`-run '^TestFlashbackMySQL57DeprecatedMode$|^TestFlashbackMinimalMetadataMySQL57$'`,
		`if grep -E '^[[:space:]]*--- SKIP:' "$log"; then`,
		`echo "MySQL 5.7 compatibility test skipped" >&2` + "\n            exit 1",
		`for name in TestFlashbackMySQL57DeprecatedMode TestFlashbackMinimalMetadataMySQL57; do`,
		`if ! grep -q -E "^--- PASS: ${name} \\(" "$log"; then`,
		"set -euo pipefail",
	} {
		if !strings.Contains(job, want) {
			t.Fatalf("MySQL 5.7 compatibility job no longer requires real execution: missing %q", want)
		}
	}
}

func mysql57CompatibilityJob(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	return ciJob(t, string(body), "mysql57-compat")
}
