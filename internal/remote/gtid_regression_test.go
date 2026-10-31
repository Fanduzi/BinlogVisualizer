package remote

import "testing"

func TestParseStartGTIDRejectsUnserializableMariaDBSet(t *testing.T) {
	for _, value := range []string{
		"0-0-0", "00-00-00", "0-0-0,1-2-3", "1-2-3,,2-3-4", "0-0-0,0-0-1",
	} {
		t.Run(value, func(t *testing.T) {
			if set, flavor, err := ParseStartGTID(value); err == nil {
				t.Fatalf("ParseStartGTID(%q) = %v, %q, nil; want an error", value, set, flavor)
			}
		})
	}
}

func TestParseStartGTIDPreservesMariaDBZeroComponents(t *testing.T) {
	for _, value := range []string{
		"0-1-0", "1-0-0", "1-1-0", "0-1-0,1-0-0,2-3-4",
	} {
		t.Run(value, func(t *testing.T) {
			set, flavor, err := ParseStartGTID(value)
			if err != nil || flavor != "mariadb" || set == nil {
				t.Fatalf("ParseStartGTID(%q) = %v, %q, %v", value, set, flavor, err)
			}
			if got := set.String(); got != value {
				t.Fatalf("ParseStartGTID(%q).String() = %q", value, got)
			}
		})
	}
}

func TestParseStartGTIDPreservesMySQLMultipleIntervals(t *testing.T) {
	const sid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	for _, suffix := range []string{"1-2:5", "1:3", "0:2"} {
		t.Run(suffix, func(t *testing.T) {
			value := sid + ":" + suffix
			set, flavor, err := ParseStartGTID(value)
			if err != nil || flavor != "mysql" || set == nil {
				t.Fatalf("ParseStartGTID(%q) = %v, %q, %v", value, set, flavor, err)
			}
			if got := set.String(); got != value {
				t.Fatalf("ParseStartGTID(%q).String() = %q, want %q", value, got, value)
			}
		})
	}
}
