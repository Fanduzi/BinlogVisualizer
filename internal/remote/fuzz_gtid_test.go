// Run: go test -fuzz=FuzzParseStartGTID -fuzztime=20s ./internal/remote
package remote

import (
	"strconv"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func FuzzParseStartGTID(f *testing.F) {
	for _, value := range []string{
		// MySQL uuid:intervals forms.
		"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
		"3E11FA47-71CA-11E1-9E33-C80AA9429562:5-5",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7-10",
		"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,724EAAD3-5B72-11E6-9F5C-0242AC110002:4-9",
		// Bare uuid:N shorthand, expanded to uuid:1-N.
		"3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
		"3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
		// MariaDB domain-server-seq forms.
		"0-1-5", "1-1-1,2-2-3", "0-1234-56789", "0-1-0", "1-0-0",
		"1-1-0", "0-1-0,1-0-0,2-3-4",
		// Malformed near-misses: errors are correct behavior.
		"", " ", "uuid:1", "3E11FA47-71CA-11E1-9E33-C80AA9429562",
		"3E11FA47-71CA-11E1-9E33-C80AA9429562:0", "not-a-gtid",
		"0-0-0", "00-00-00", "0-0-0,0-0-1", "1-2-3,,2-3-4",
		"a-b-c", ":1-5", "uuid:1-", "1-5",
		"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,0-1-2",
		"0-1-5,3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
		"3E11FA47-71CA-11E1-9E33-C80AA9429562:", "0-1-", "-1-5",
	} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		set, flavor, err := ParseStartGTID(value)
		if err != nil {
			return
		}
		if set == nil {
			t.Fatalf("ParseStartGTID(%q) returned a nil set without error", value)
		}
		if flavor != mysql.MySQLFlavor && flavor != mysql.MariaDBFlavor {
			t.Fatalf("ParseStartGTID(%q) returned unknown flavor %q", value, flavor)
		}
		// ParseStartGTID rejects empty flag text, even for an empty set.
		text := set.String()
		if text == "" {
			if !set.IsEmpty() {
				t.Fatalf("non-empty GTID set from %q serialized to empty text", value)
			}
			return
		}
		roundTrip, err := mysql.ParseGTIDSet(flavor, text)
		if err != nil || !set.Equal(roundTrip) {
			t.Fatalf("GTID set round-trip changed %q via %q: %v", value, text, err)
		}

		// The CLI interprets a bare uuid:N as 1-N, unlike the set parser.
		expected := set
		sid, point, bare := strings.Cut(text, ":")
		if flavor == mysql.MySQLFlavor && bare && !strings.ContainsAny(point, ":,-") {
			n, parseErr := strconv.ParseUint(point, 10, 64)
			if parseErr == nil && n > 0 {
				expected, err = mysql.ParseMysqlGTIDSet(sid + ":1-" + point)
				if err != nil {
					t.Fatalf("invalid expected shorthand %q: %v", text, err)
				}
			}
		}
		set2, flavor2, err := ParseStartGTID(text)
		if err != nil {
			t.Fatalf("String() %q of %q does not re-parse: %v", text, value, err)
		}
		if flavor2 != flavor {
			t.Fatalf("flavor drifted: %q -> %q gives %q, want %q", value, text, flavor2, flavor)
		}
		if !expected.Equal(set2) {
			t.Fatalf("CLI reparse changed %q via %q to %q, want %q", value, text, set2.String(), expected.String())
		}
	})
}
