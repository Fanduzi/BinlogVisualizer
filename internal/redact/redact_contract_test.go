package redact_test

import (
	"strings"
	"testing"

	"binlogviz/internal/redact"
)

func TestCredentialsProtectAuthenticationSecrets(t *testing.T) {
	tests := []struct {
		name, sql, want string
	}{
		{"ordinary SQL", "SELECT name FROM shop.users", "SELECT name FROM shop.users"},
		{"identified by", "ALTER USER 'u' IDENTIFIED BY 'secret'", "ALTER USER 'u' IDENTIFIED BY <secret>"},
		{"mixed case", "ALTER USER 'u' IdEnTiFiEd bY 'secret'", "ALTER USER 'u' IdEnTiFiEd bY <secret>"},
		{"password hash", "CREATE USER 'u' IDENTIFIED BY PASSWORD '*ABCD'", "CREATE USER 'u' IDENTIFIED BY PASSWORD <secret>"},
		{"random password", "CREATE USER 'u' IDENTIFIED BY RANDOM PASSWORD", "CREATE USER 'u' IDENTIFIED BY RANDOM PASSWORD"},
		{"plugin hash", "CREATE USER 'u' IDENTIFIED WITH mysql_native_password AS '*ABCD'", "CREATE USER 'u' IDENTIFIED WITH mysql_native_password AS <secret>"},
		{"bare plugin hash", "CREATE USER 'u' IDENTIFIED WITH mysql_native_password BY *ABCD", "CREATE USER 'u' IDENTIFIED WITH mysql_native_password BY <secret>"},
		{"password call", "CREATE USER 'u' IDENTIFIED VIA mysql_native_password USING PASSWORD('secret')", "CREATE USER 'u' IDENTIFIED VIA mysql_native_password USING <secret>"},
		{"plugin chain", "CREATE USER 'u' IDENTIFIED VIA unix_socket OR mysql_native_password USING PASSWORD('secret')", "CREATE USER 'u' IDENTIFIED VIA unix_socket OR mysql_native_password USING <secret>"},
		{"quoted plugin", "CREATE USER 'u' IDENTIFIED WITH `mysql_native_password` AS '*ABCD'", "CREATE USER 'u' IDENTIFIED WITH `mysql_native_password` AS <secret>"},
		{"set password", "SET PASSWORD = 'secret'", "SET PASSWORD = <secret>"},
		{"nested password call", "SET PASSWORD FOR 'u'@'localhost' = PASSWORD(CONCAT('a', CONCAT('b', 'c')))", "SET PASSWORD FOR 'u'@'localhost' = <secret>"},
		{"doubled quote", "ALTER USER 'u' IDENTIFIED BY 'it''s secret'", "ALTER USER 'u' IDENTIFIED BY <secret>"},
		{"backslash quote", `ALTER USER 'u' IDENTIFIED BY 'it\'s secret'`, "ALTER USER 'u' IDENTIFIED BY <secret>"},
		{"empty secret", "ALTER USER 'u' IDENTIFIED BY ''", "ALTER USER 'u' IDENTIFIED BY <secret>"},
		{"multiple accounts", "CREATE USER 'a' IDENTIFIED BY 'one', 'b' IDENTIFIED BY 'two'", "CREATE USER 'a' IDENTIFIED BY <secret>, 'b' IDENTIFIED BY <secret>"},
		{"identifier prefixes", "SELECT identified_by, password_hash, myset FROM accounts", "SELECT identified_by, password_hash, myset FROM accounts"},
		{"user variable", "SET @password = 'ordinary value'", "SET @password = 'ordinary value'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redact.Credentials(tt.sql); got != tt.want {
				t.Fatalf("Credentials(%q) = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}

func TestCredentialsLeaveSecretlessRulesUnchanged(t *testing.T) {
	for _, sql := range []string{
		"CREATE USER 'u' IDENTIFIED WITH auth_socket",
		"CREATE USER 'u' IDENTIFIED VIA mysql_native_password USING",
		"SET PASSWORD FOR 'u'@'localhost'",
	} {
		t.Run(sql, func(t *testing.T) {
			if got := redact.Credentials(sql); got != sql {
				t.Fatalf("Credentials(%q) = %q, want unchanged", sql, got)
			}
		})
	}
}

func TestStatementRedactsWholeMultilineInputWithoutTruncating(t *testing.T) {
	prefix := strings.Repeat("x", 4096)
	sql := prefix + "\nALTER USER 'u' IDENTIFIED BY 'first line\nsecond line';\nSELECT 1;"
	want := prefix + "\nALTER USER 'u' IDENTIFIED BY <secret>;\nSELECT 1;"
	if got := redact.Statement(sql); got != want {
		t.Fatalf("Statement returned %d bytes; want the complete %d-byte redacted statement", len(got), len(want))
	}
}

func TestDisplayKeepsPrintableTextWithoutTerminalControls(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{"empty", "", ""},
		{"plain", "ordinary SQL", "ordinary SQL"},
		{"unicode", "caf\u00e9 \u4e2d\u6587 \U0001f600", "caf\u00e9 \u4e2d\u6587 \U0001f600"},
		{"controls", "a\nb\tc\rd\x00e", "abcde"},
		{"escape", "red \x1b[31mtext\x1b[0m", "red [31mtext[0m"},
		{"invalid UTF-8", "a\xffb\xc3c", "abc"},
		{"unicode and controls", "\u4e2d\n\u6587\x1b[31m", "\u4e2d\u6587[31m"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redact.Display(tt.input); got != tt.want {
				t.Fatalf("Display(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
