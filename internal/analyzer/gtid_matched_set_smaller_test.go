package analyzer

import (
	"fmt"
	"strings"
	"testing"
)

// Interleaved writers on consecutive sequence numbers make the shared form
// smaller than the per-server one, so the per-server error may say it could
// need fewer.
func TestMatchedGTIDSetSharedSmallerWording(t *testing.T) {
	withLimit(t, 10)
	var all []string
	matched := map[string]bool{}
	keep := func(i int) { // writers 7, 8 alternate sequence numbers 1, 2, ...
		id := fmt.Sprintf("0-%d-%d", 6+i%2+1, i)
		all, matched[id] = append(all, id), true
	}
	for i := 1; i <= 8; i++ {
		keep(i)
	}
	for i := 0; i < 12; i++ { // the seen list overflows; the shared form drops while smaller
		all = append(all, fmt.Sprintf("0-9-%d", 100*(i+1)))
	}
	for i := 9; i <= 30; i++ { // the per-server form then overflows
		keep(i)
	}
	set, err := matchedSetFrom(t, all, matched)
	limit := limitOf(t, err)
	if limit.Reason != MatchedGTIDLimitPerServer || !limit.SharedSmaller || !set.sharedSmaller {
		t.Fatalf("want per_server with SharedSmaller, got %+v", limit)
	}
	if msg := err.Error(); !strings.Contains(msg, "the shared form, which can need fewer, was dropped") {
		t.Fatalf("message: %v", msg)
	}
}
