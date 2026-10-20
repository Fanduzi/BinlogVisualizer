package flashback

// SetReviewProbe runs probe at the end of each generated-column review, while
// the review's per-table state is still reachable. It returns a restore func.
func SetReviewProbe(probe func()) (restore func()) {
	old := reviewProbe
	reviewProbe = probe
	return func() { reviewProbe = old }
}
