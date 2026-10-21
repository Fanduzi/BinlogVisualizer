package displaytz

import (
	"testing"
	"time"
)

func TestStoredInstantsAcrossDST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	// 2026-03-08 is the New York spring-forward. 2026-11-01 is the fall-back.
	// The two 01:30 local times on the fall-back day are different instants.
	cases := []struct {
		in, want string
	}{
		{"2026-03-08T06:30:00Z", "2026-03-08T01:30:00-05:00"},
		{"2026-03-08T07:30:00Z", "2026-03-08T03:30:00-04:00"},
		{"2026-11-01T05:30:00Z", "2026-11-01T01:30:00-04:00"},
		{"2026-11-01T06:30:00Z", "2026-11-01T01:30:00-05:00"},
	}
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases[:4] {
		got := Stored(tc.in, ny)
		if got != tc.want {
			t.Fatalf("Stored(%s, NewYork) = %s, want %s", tc.in, got, tc.want)
		}
		back, err := time.Parse(time.RFC3339, got)
		if err != nil {
			t.Fatal(err)
		}
		want, err := time.Parse(time.RFC3339, tc.in)
		if err != nil {
			t.Fatal(err)
		}
		if !back.Equal(want) {
			t.Fatalf("Stored changed the instant: %s -> %s", tc.in, got)
		}
	}
	got := Stored("2026-08-29T06:30:00Z", kolkata)
	if got != "2026-08-29T12:00:00+05:30" {
		t.Fatalf("Stored Kolkata = %s", got)
	}
	if Stored("2026-08-29T06:30:00Z", nil) != "2026-08-29T06:30:00Z" {
		t.Fatal("nil location rewrote a stored timestamp")
	}
	if Stored("not-a-time", ny) != "not-a-time" {
		t.Fatal("unparsable timestamp was rewritten")
	}
}

func TestParseRejectsLocal(t *testing.T) {
	for _, value := range []string{"Local", "local", "LOCAL", " Local "} {
		loc, err := Parse(true, value)
		if err == nil || loc != nil {
			t.Fatalf("Parse(%q) = %v, %v; Local is the process zone", value, loc, err)
		}
	}
	loc, err := Parse(true, "Asia/Shanghai")
	if err != nil || loc == nil || loc.String() != "Asia/Shanghai" {
		t.Fatalf("Asia/Shanghai: %v %v", loc, err)
	}
	utc, err := Parse(true, "UTC")
	if err != nil || utc != time.UTC {
		t.Fatalf("UTC = %v %v, want time.UTC", utc, err)
	}
	unset, err := Parse(false, "")
	if err != nil || unset != nil {
		t.Fatalf("unset = %v %v, want nil", unset, err)
	}
}
