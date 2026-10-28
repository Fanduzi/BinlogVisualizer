package analyzer

import "testing"

// The reason a limit error names is pinned, not just the count: a
// conflict is not the reason while its exact form is still kept, a repeat
// that only touched unmatched transactions is never the reason, and a
// real repeat wins over another domain's per-server count.
func TestMatchedGTIDSetLimitReasonsArePinned(t *testing.T) {
	withLimit(t, 2)
	mysqlID := "24bc7850-2c16-11e6-a073-0242ac110002:1"
	for _, tc := range []struct {
		name    string
		all     []string
		matched map[string]bool
		reason  MatchedGTIDLimitReason
	}{
		{"conflict with the exact form kept stays plain",
			[]string{"0-7-1", "0-8-1", mysqlID},
			map[string]bool{"0-7-1": true, "0-8-1": true, mysqlID: true},
			MatchedGTIDLimitPlain},
		{"unmatched repeat stays plain",
			[]string{"0-7-1", "0-8-1", "1-9-1", "1-8-2", "1-9-3", "1-9-5"},
			map[string]bool{"1-9-1": true, "1-8-2": true, "1-9-3": true, "1-9-5": true},
			MatchedGTIDLimitPlain},
		{"repeat wins over another domain's per-server",
			[]string{"1-9-1", "1-9-2", "1-9-3", "1-9-4", "1-9-5", "0-7-1", "0-8-1"},
			map[string]bool{"1-9-4": true, "1-9-5": true, "0-7-1": true, "0-8-1": true},
			MatchedGTIDLimitRepeat},
	} {
		limit := limitOf(t, func() error { _, err := matchedSetFrom(t, tc.all, tc.matched); return err }())
		if limit.Reason != tc.reason {
			t.Fatalf("%s: got %+v", tc.name, limit)
		}
	}
}
