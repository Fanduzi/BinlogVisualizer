package flashback

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

// #250: a DDL refusal redacts credentials before the statement is cut to one line.
func TestDDLRefusalRedactsCredentials(t *testing.T) {
	const hash = "$A$005$!I48l]A05OT1A+v1AY01Qu0yHwSECRETHASH"
	statements := []string{
		"CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '" + hash + "'",
		"ALTER USER 'app'@'%' IDENTIFIED BY 'plainSECRETHASH'",
		"CREATE USER 'x'@'%'\n  IDENTIFIED BY 'multiSECRETHASH' PASSWORD EXPIRE",
		"SET PASSWORD FOR 'app'@'%' = 'setSECRETHASH'",
	}
	for _, statement := range statements {
		ddl := flashDDLError(model.NormalizedEvent{EventType: "DDL", QuerySQL: statement}, "'repl'@'%'").Error()
		if strings.Contains(ddl, "SECRETHASH") || strings.Contains(ddl, "I48l") {
			t.Fatalf("flashback DDL refusal leaks: %s", ddl)
		}
		if !strings.Contains(ddl, "<secret>") && strings.Contains(strings.SplitN(statement, "\n", 2)[0], "IDENTIFIED") {
			t.Fatalf("no redaction marker: %s", ddl)
		}
	}
}
