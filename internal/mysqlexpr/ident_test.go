package mysqlexpr

import "testing"

func TestDigitTokenFollowsMySQLLex(t *testing.T) {
	cases := []struct {
		sql     string
		text    string
		isIdent bool
	}{
		{"2fa_codes", "2fa_codes", true},
		{"9_t", "9_t", true},
		{"09_t", "09_t", true},
		{"1e", "1e", true},
		{"1ea", "1ea", true},
		{"1e+", "1e", true},
		{"1", "1", false},
		{"10", "10", false},
		{"1e5", "1e5", false},
		{"1e+10", "1e+10", false},
		{"1E-2", "1E-2", false},
		{"0x1f", "0x1f", false},
		{"0b10", "0b10", false},
		{"0x1g", "0x1g", true},
	}
	for _, tc := range cases {
		p := NewReader(tc.sql, Dialect{})
		text, isIdent := p.DigitToken()
		if text != tc.text || isIdent != tc.isIdent {
			t.Errorf("%s: got %q ident=%v, want %q ident=%v", tc.sql, text, isIdent, tc.text, tc.isIdent)
		}
	}
}

func TestIdentQuoteWithoutANSISetsErr(t *testing.T) {
	p := NewReader(`"t"`, Dialect{})
	id, ok := p.Ident()
	if ok || !p.Err() || id.Text != "" {
		t.Fatalf("ok=%v err=%v id=%q, want unreadable", ok, p.Err(), id.Text)
	}
}

func TestAccountAfterKeepsQuotes(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"CREATE USER 'repl'@'%'", `'repl'@'%'`},
		{"CREATE USER IF NOT EXISTS 'repl'@'%'", `'repl'@'%'`},
		{"ALTER USER 'repl'@'localhost' IDENTIFIED BY 'x'", `'repl'@'localhost'`},
		{"DROP USER IF EXISTS 'repl'@'%'", `'repl'@'%'`},
		{"CREATE USER 'a'@'b', 'c'@'d'", `'a'@'b'`},
	}
	for _, tc := range cases {
		if got := AccountAfter(tc.sql, "USER", Dialect{}); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.sql, got, tc.want)
		}
	}
}

// A double-quoted string with ANSI_QUOTES off is a string literal the
// lexer skips; a mutant that stopped skipping it would return it as the
// name.
func TestDoubleQuotedStringIsSkippedNotAName(t *testing.T) {
	p := NewReader(`ALTER USER "x" IDENTIFIED BY 'p'`, Dialect{})
	word, ok := p.nextWord()
	if !ok || word.Text != "ALTER" {
		t.Fatalf("first word %q ok=%v", word.Text, ok)
	}
	word, ok = p.nextWord()
	if !ok || word.Text != "USER" {
		t.Fatalf("second word %q ok=%v", word.Text, ok)
	}
	word, ok = p.nextWord()
	if !ok || word.Text != "IDENTIFIED" {
		t.Fatalf(`"x" was taken as a name or the skip stopped: %q ok=%v`, word.Text, ok)
	}
}
