package analyzer

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// darwin's whole-process RSS jitters by more than the series this check
// isolates (freed pages linger in malloc zones), so a spilled run can read
// above an inlined one by noise alone. The heap comparison just above stays;
// the RSS comparison runs on Linux, where VmRSS is stable after the free.
func spillRSSComparable() bool { return false }

// ps reports the same current resident figure the Linux side reads from
// VmRSS, in KB; darwin has no /proc.
func processRSS(t *testing.T) uint64 {
	t.Helper()
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatal(err)
	}
	kb, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Fatalf("ps rss %q: %v", out, err)
	}
	return kb * 1024
}
