// Package remote parses --start-gtid into a go-mysql GTID set and a flavor.
// input: the flag text, MySQL uuid:intervals or MariaDB domain-server-seq form.
// output: the executed-set to send, and the protocol flavor that form implies.
// pos: flag-value parser for the remote package; the value means "the set already consumed" (ADR 0021).
// note: if this file changes, update this header and README.md.
package remote

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// ParseStartGTID parses a --start-gtid value. The set is what the replica has
// already consumed: the stream begins at the first transaction after it.
// A bare uuid:N (one SID, one point) expands to uuid:1-N, matching "start
// after GTID N". The parsed format selects the flavor, so MariaDB sets reach
// the @slave_connect_state path and MySQL sets reach COM_BINLOG_DUMP_GTID.
func ParseStartGTID(value string) (mysql.GTIDSet, string, error) {
	text := strings.TrimSpace(value)
	if text == "" {
		return nil, "", fmt.Errorf("empty GTID set")
	}
	if single, ok := expandSingleGTID(text); ok {
		text = single
	}
	if set, err := mysql.ParseMysqlGTIDSet(text); err == nil {
		return set, mysql.MySQLFlavor, nil
	}
	if set, err := mysql.ParseMariadbGTIDSet(text); err == nil {
		// go-mysql serializes an all-zero GTID as an empty list element.
		// Check the original entries as merging can hide an all-zero GTID.
		for _, part := range strings.Split(text, ",") {
			gtid, parseErr := mysql.ParseMariadbGTID(part)
			if parseErr != nil || gtid.String() == "" {
				return nil, "", fmt.Errorf("invalid MariaDB GTID set %q: empty GTID", value)
			}
		}
		return set, mysql.MariaDBFlavor, nil
	}
	return nil, "", fmt.Errorf("%q is neither a MySQL GTID set (uuid:intervals) nor a MariaDB one (domain-server-seq)", value)
}

// expandSingleGTID rewrites "uuid:N" as "uuid:1-N". Anything else (multi-SID,
// interval ranges, MariaDB triples) is left alone: its text already means the
// executed set literally.
func expandSingleGTID(text string) (string, bool) {
	if strings.ContainsAny(text, ", ") || strings.Count(text, ":") != 1 {
		return "", false
	}
	colon := strings.LastIndexByte(text, ':')
	if colon <= 0 || colon == len(text)-1 {
		return "", false
	}
	suffix := text[colon+1:]
	if !isDigits(suffix) {
		return "", false
	}
	if suffix == "0" {
		return "", false
	}
	return text[:colon+1] + "1-" + suffix, true
}

func isDigits(value string) bool {
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return value != ""
}
