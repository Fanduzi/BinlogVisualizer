package flashback

// SetReviewSample installs fn for TestGeneratedReviewRSS. fn runs in the
// review goroutine once the review has finished, while held is still reachable.
func SetReviewSample(fn func(held any)) { reviewSample = fn }
