package analyzer

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func spillRSSComparable() bool { return !activitySpillRace }

func processRSS(t *testing.T) uint64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("VmRSS line %q", line)
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return kb * 1024
	}
	t.Fatal("VmRSS missing")
	return 0
}
