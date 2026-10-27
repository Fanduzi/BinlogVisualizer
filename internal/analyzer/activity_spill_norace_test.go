//go:build !race

package analyzer

import "testing"

const activitySpillRace = false

func spillRSSComparable() bool { return !activitySpillRace }

// The plain build has to keep the RSS comparison. Flipping the const above
// drops it and this test fails.
func TestPlainBuildKeepsSpillRSSCheck(t *testing.T) {
	if activitySpillRace || !spillRSSComparable() {
		t.Fatal("plain build dropped the spill RSS check")
	}
}
