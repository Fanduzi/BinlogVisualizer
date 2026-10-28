//go:build linux && !race

package analyzer

import "testing"

// The plain build has to keep the RSS comparison. Flipping the const in
// activity_spill_norace_test.go drops it and this test fails. darwin has no
// comparable whole-process RSS, so the check lives on the platform CI runs.
func TestPlainBuildKeepsSpillRSSCheck(t *testing.T) {
	if activitySpillRace || !spillRSSComparable() {
		t.Fatal("plain build dropped the spill RSS check")
	}
}
