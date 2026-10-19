package analyzer

import (
	"strings"
	"testing"
)

// #250: every error that quotes a statement redacts credentials first,
// before the statement is cut to one line.
func TestRefusalMessagesRedactCredentials(t *testing.T) {
	const hash = "$A$005$!I48l]A05OT1A+v1AY01Qu0yHwSECRETHASH"
	statements := []string{
		"CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '" + hash + "'",
		"ALTER USER 'app'@'%' IDENTIFIED BY 'plainSECRETHASH'",
		"CREATE USER 'x'@'%'\n  IDENTIFIED BY 'multiSECRETHASH' PASSWORD EXPIRE",
		"SET PASSWORD FOR 'app'@'%' = 'setSECRETHASH'",
	}
	for _, statement := range statements {
		unclassified := unclassifiedQueryError(statement).Error()
		if strings.Contains(unclassified, "SECRETHASH") || strings.Contains(unclassified, "I48l") {
			t.Fatalf("unclassified QUERY leaks: %s", unclassified)
		}
	}
}
