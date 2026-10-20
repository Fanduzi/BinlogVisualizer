package gtidtoken

import (
	"reflect"
	"testing"
)

// Whitespace next to ".." is rejected on both sides; around the token it is
// trimmed (#383 QA: "0-7-1 ..5" was accepted, "0-7-1.. 5" refused).
func TestParseMariaDBWhitespace(t *testing.T) {
	for _, value := range []string{"0-7-1..5", " 0-7-1..5 ", "\t0-7-1..5\n"} {
		if got, ok := ParseMariaDB(value); !ok || got != (MariaDB{0, 7, 1, 5}) {
			t.Fatalf("%q: %+v %v", value, got, ok)
		}
	}
	for _, value := range []string{"0-7-1 ..5", "0-7-1.. 5", "0-7-1 .. 5", "0-7-1..\t5", "0 -7-1", "0-7- 1", "0-7-5..4", "0-7-1..2..3", "0-7-1..", "0-7", "0-7-+1"} {
		if got, ok := ParseMariaDB(value); ok {
			t.Fatalf("%q accepted: %+v", value, got)
		}
	}
}

func TestMergeMariaDB(t *testing.T) {
	got, ok := MergeMariaDB([]string{"0-7-3", "0-7-1", "0-7-2..2", "0-8-2", "1-7-9..10,0-7-5..6", "0-7-6..8", "0-10-1"})
	want := []string{"0-7-1..3", "0-7-5..8", "0-8-2", "0-10-1", "1-7-9..10"}
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v %v, want %v", got, ok, want)
	}
	if got, ok := MergeMariaDB([]string{"0-7-18446744073709551614..18446744073709551615", "0-7-1"}); !ok || !reflect.DeepEqual(got, []string{"0-7-1", "0-7-18446744073709551614..18446744073709551615"}) {
		t.Fatalf("max seq: %v %v", got, ok)
	}
	// A range inside an earlier one keeps the outer end (#384 QA F1).
	if got, ok := MergeMariaDB([]string{"0-7-1..10,0-7-3..4", "0-7-2"}); !ok || !reflect.DeepEqual(got, []string{"0-7-1..10"}) {
		t.Fatalf("contained range: %v %v", got, ok)
	}
	if _, ok := MergeMariaDB([]string{"0-7-1", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1"}); ok {
		t.Fatal("MySQL token parsed as MariaDB")
	}
}
