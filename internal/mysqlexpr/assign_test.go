package mysqlexpr

import "testing"

// Column-assignment coercions the generated-column checker models: YEAR's
// padded display, BINARY's zero padding, temporal fraction rounding, and
// charset-introducer literals a dumped expression cannot be trusted with.

func TestGeneratedYearPadsToFourDigits(t *testing.T) {
	y := SchemaCol{Name: "y", base: "year"}
	g := SchemaCol{Name: "g", base: "varchar", Expr: "CONCAT(`y`)", prec: 10, hasPrec: true}
	assertGenOutcome(t, g, []SchemaCol{y, g}, []string{"y", "g"}, []string{"0", "'0000'"}, "match")
	assertGenOutcome(t, g, []SchemaCol{y, g}, []string{"y", "g"}, []string{"2024", "'2024'"}, "match")
	assertGenOutcome(t, g, []SchemaCol{y, g}, []string{"y", "g"}, []string{"0", "'0'"}, "mismatch")
}

func TestGeneratedBinaryPadsWithNul(t *testing.T) {
	b := SchemaCol{Name: "b", base: "varbinary"}
	g := SchemaCol{Name: "g", base: "binary", Expr: "(`b`)", prec: 10, hasPrec: true}
	// The stored image is right-padded with 0x00 to the column width.
	assertGenOutcome(t, g, []SchemaCol{b, g}, []string{"b", "g"},
		[]string{"'ab'", "X'61620000000000000000'"}, "match")
	// Longer than the column is a sql_mode-dependent truncation.
	assertGenOutcome(t, g, []SchemaCol{b, g}, []string{"b", "g"},
		[]string{"X'6162636465666768696A6B'", "X'6162636465666768696A'"}, "unverified")
	// VARBINARY never pads.
	v := SchemaCol{Name: "v", base: "varbinary", Expr: "(`b`)", prec: 10, hasPrec: true}
	assertGenOutcome(t, v, []SchemaCol{b, v}, []string{"b", "v"}, []string{"'ab'", "'ab'"}, "match")
}

func TestGeneratedDatetimeRoundsFraction(t *testing.T) {
	d := SchemaCol{Name: "d", base: "datetime", prec: 6, hasPrec: true}
	g := SchemaCol{Name: "g", base: "datetime", Expr: "(`d`)", prec: 3, hasPrec: true}
	assertGenOutcome(t, g, []SchemaCol{d, g}, []string{"d", "g"},
		[]string{"'2020-01-02 03:04:05.123999'", "'2020-01-02 03:04:05.124'"}, "match")
	assertGenOutcome(t, g, []SchemaCol{d, g}, []string{"d", "g"},
		[]string{"'2020-01-02 03:04:05.123499'", "'2020-01-02 03:04:05.123'"}, "match")
	// Short of the column fsp pads with zeros.
	assertGenOutcome(t, g, []SchemaCol{d, g}, []string{"d", "g"},
		[]string{"'2020-01-02 03:04:05.1'", "'2020-01-02 03:04:05.100'"}, "match")
	// A round-up that carries into the seconds is not claimed.
	assertGenOutcome(t, g, []SchemaCol{d, g}, []string{"d", "g"},
		[]string{"'2020-01-02 03:04:05.9999'", "'2020-01-02 03:04:06.000'"}, "unverified")
	// fsp 0 drops a small fraction; rounding into the seconds is not claimed.
	z := SchemaCol{Name: "z", base: "datetime", Expr: "(`d`)"}
	assertGenOutcome(t, z, []SchemaCol{d, z}, []string{"d", "z"},
		[]string{"'2020-01-02 03:04:05.400000'", "'2020-01-02 03:04:05'"}, "match")
	assertGenOutcome(t, z, []SchemaCol{d, z}, []string{"d", "z"},
		[]string{"'2020-01-02 03:04:05.600000'", "'2020-01-02 03:04:06'"}, "unverified")
}

func TestGeneratedIntroducerLiteralIsNotEvaluated(t *testing.T) {
	// A _charset'..' quoted literal in a dumped expression can already be
	// mangled, so the checker declines the whole expression rather than
	// claim a value.
	g := SchemaCol{Name: "g", base: "varchar", Expr: "UPPER(_latin1'é')", prec: 10, hasPrec: true}
	assertGenOutcome(t, g, []SchemaCol{g}, []string{"g"}, []string{"'É'"}, "unverified")
	// A hex or binary literal names its bytes exactly, so the same
	// introducer is byte-faithful and the expression is evaluated.
	h := SchemaCol{Name: "h", base: "int", Expr: "CHAR_LENGTH(UPPER(_latin1 X'E9'))"}
	assertGenOutcome(t, h, []SchemaCol{h}, []string{"h"}, []string{"1"}, "match")
	b := SchemaCol{Name: "b", base: "int", Expr: "LENGTH(_binary X'41')"}
	assertGenOutcome(t, b, []SchemaCol{b}, []string{"b"}, []string{"1"}, "match")
	// The same literal form still parses when it is the logged image itself.
	if v, ok := parseLoggedValue("_latin1 X'E9'"); !ok || v.kind != genText {
		t.Fatalf("logged introducer literal did not parse: %v %v", v, ok)
	}
}
