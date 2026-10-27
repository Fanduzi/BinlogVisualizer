//go:build race

package analyzer

const activitySpillRace = true

func spillRSSComparable() bool { return !activitySpillRace }
