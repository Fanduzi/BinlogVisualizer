package binlogviz

import (
	"os"
	"os/exec"
	"testing"
)

// e2eDatadirPrefix is "<container>:" when the server's datadir lives in a
// container rather than on this filesystem (the MySQL 5.7 service job).
func e2eDatadirPrefix() string { return os.Getenv("BINLOGVIZ_E2E_DATADIR_PREFIX") }

// e2eCopy copies a binlog out of the server's datadir. A datadir the test user
// can read copies directly; the root-only datadir of the CI container needs
// sudo, which the runner grants without a password.
func e2eCopy(t *testing.T, src, dst string) {
	t.Helper()
	if e2eDatadirPrefix() != "" {
		if out, err := exec.Command("docker", "cp", src, dst).CombinedOutput(); err != nil {
			t.Fatalf("docker cp %s: %v\n%s", src, err, out)
		}
		if out, err := exec.Command("chmod", "a+r", dst).CombinedOutput(); err != nil {
			t.Fatalf("chmod %s: %v\n%s", dst, err, out)
		}
		return
	}
	sudo := false
	if out, err := exec.Command("cp", src, dst).CombinedOutput(); err != nil {
		if sudoOut, sudoErr := exec.Command("sudo", "cp", src, dst).CombinedOutput(); sudoErr != nil {
			t.Fatalf("cp %s: %v\n%s\n%s", src, err, out, sudoOut)
		}
		sudo = true
	}
	chmod := []string{"chmod", "a+r", dst}
	if sudo {
		chmod = append([]string{"sudo"}, chmod...)
	}
	if out, err := exec.Command(chmod[0], chmod[1:]...).CombinedOutput(); err != nil {
		t.Fatalf("chmod %s: %v\n%s", dst, err, out)
	}
}
