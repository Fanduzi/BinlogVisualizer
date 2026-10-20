// Package guard: the flashback apply guard.
// input: the generated columns an undo script omits (db, table, column), and the statement pieces the planner emits.
// output: the guard header (version check, information_schema check, the one 200-byte message cut, read-only lock and token), the per-statement token condition, and Write, which seals one script (session token, running digest, trailer that does not commit). Verify checks that script. A raw piece is followed by GET DIAGNOSTICS, so a failure outside the procedure is kept. An undo CALL is followed by an @@error_count check; the handler already kept the statement error. The message cut exists only in the emitted SQL; its tests run that SQL on MySQL 8.0 and 5.7 (guard_test.go).
// pos: flashback-only module. Analyze does not call it. The planner walks omitted columns and calls New, then Write.
// note: if this file changes, update this header, CONTEXT.md (Apply guard), docs/adr/0006-guard-module.md and docs/adr/0013-flashback-apply-integrity.md.
package guard

import (
	"fmt"
	"strings"
)

// Col is one generated column an undo script omits.
type Col struct {
	Schema string
	Table  string
	Column string
}

// Guard is the apply guard of one script. The zero value is no guard:
// every method then returns "" or its input unchanged.
type Guard struct {
	header string
}

// New builds the guard for the omitted generated columns, in order.
func New(cols []Col) Guard {
	return Guard{header: renderGeneratedGuard(cols)}
}

// Header is the SQL printed once after the session SET lines.
func (g Guard) Header() string { return g.header }

// BlockLock is the header lock (COMMIT, read-only, unlock only when the token
// matches). It runs once, inside the header, before any undo. A later block
// uses BlockGate: repeating this lock would COMMIT a prefix of the undo.
func (g Guard) BlockLock() string {
	if g.header == "" {
		return ""
	}
	return guardLockSQL
}

// BlockGate is the per-block read-only gate. It does not COMMIT. A block that
// is inside this script's transaction and holds the token runs DO 0, so the
// header's unlock stands. Anything else runs SET SESSION TRANSACTION READ ONLY,
// which names no server variable. Empty when the script has no guard.
func (g Guard) BlockGate() string {
	if g.header == "" {
		return ""
	}
	// Client SQL, not a CALL. A block cut off from the header has no procedure,
	// and the session must still be read-only. PREPARE cannot wrap this.
	return "SET @binlogviz_gate_sql = IF(@binlogviz_txn <=> 1 AND @binlogviz_ok <=> '" + guardTokenMark + "', 'DO 0', 'SET SESSION TRANSACTION READ ONLY');\n" +
		"PREPARE binlogviz_fb_gate FROM @binlogviz_gate_sql;\n" +
		"EXECUTE binlogviz_fb_gate;\n" +
		"DEALLOCATE PREPARE binlogviz_fb_gate;\n" +
		appliedRecordSQL()
}

// RowCond is added to every undo statement. A token that does not match this
// session is ERROR 1242 and sets @binlogviz_bad, not a silent zero-row match.
func (g Guard) RowCond() string {
	if g.header == "" {
		return ""
	}
	return guardTokenCond
}

// seal replaces the token placeholder. Tests of the header SQL call it. Write
// is the script seal; this does not hash the script.
func (g Guard) seal(script, token string) string {
	if g.header == "" || token == "" {
		return script
	}
	return strings.ReplaceAll(script, guardTokenMark, token)
}

const guardMismatchLead = "binlogviz: schema file does not match the target"
const guardOldServerMsg = "binlogviz: target server is older than MySQL 5.7.0 or MariaDB 10.2 and is not supported for apply"
const guardNotRunMsg = "binlogviz: generated-column guard did not run"
const guardReadOnlyMsg = "binlogviz: this session is already read-only so the script cannot write. Disconnect and apply the script again in a new session"
const guardMismatchHint = ". This session is now read-only: disconnect, fix the schema file, and apply the script again in a new session."

// guardTokenMark is replaced by this script's token after rendering. The
// guard sets @binlogviz_ok to the token only when it ran in this session and
// matched; every block and every undo statement checks it.
const guardTokenMark = "\x00binlogviz-token\x00"

// A match returns 1. MySQL runs only that branch, so the subquery is not
// evaluated and a real row still matches. A mismatch assigns @binlogviz_bad
// and then errors with 1242 (subquery returns more than one row). The
// assignment survives --force, and the trailer rolls the transaction back.
const guardTokenCond = "CASE WHEN @binlogviz_ok <=> '" + guardTokenMark + "' THEN 1 ELSE (SELECT @binlogviz_bad := 1 UNION ALL SELECT 1) END"
const guardValueLimit = 200

// Prepared statement names. The suffix keeps them apart from names a DBA
// might already use in the session.
const (
	guardStmtMsg    = "binlogviz_fb_msg_x9q"
	guardStmtRO     = "binlogviz_fb_ro_x9q"
	guardStmtUnlock = "binlogviz_fb_unlock_x9q"
)

// guardServerCheckSQL parses VERSION() numerically. MariaDB is detected from
// "MariaDB" in the string; its "5.5.5-" handshake prefix is dropped. A -suffix is
// ignored. @binlogviz_old is 1 below MySQL 5.7.0 or MariaDB 10.2.
// @binlogviz_ro names the session read-only variable that server has:
// MySQL 5.7.20+ and MariaDB 11.1+ have transaction_read_only, MySQL 5.6.5–5.7.19
// and MariaDB 10.x–11.0 have tx_read_only. That name is only used to read the
// session flag before the lock, so a wrong name cannot unlock anything.
func guardServerCheckSQL() string {
	return "SET @binlogviz_maria = IF(LOCATE('MariaDB', @@version) > 0, 1, 0);\n" +
		"SET @binlogviz_ver = SUBSTRING_INDEX(IF(@binlogviz_maria = 1 AND @@version LIKE '5.5.5-%', SUBSTRING(@@version, 7), @@version), '-', 1);\n" +
		"SET @binlogviz_major = CAST(SUBSTRING_INDEX(@binlogviz_ver, '.', 1) AS UNSIGNED);\n" +
		"SET @binlogviz_minor = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(@binlogviz_ver, '.', 2), '.', -1) AS UNSIGNED);\n" +
		"SET @binlogviz_patch = CAST(SUBSTRING_INDEX(@binlogviz_ver, '.', -1) AS UNSIGNED);\n" +
		"SET @binlogviz_old = IF(@binlogviz_maria = 1, IF(@binlogviz_major > 10 OR (@binlogviz_major = 10 AND @binlogviz_minor >= 2), 0, 1), IF(@binlogviz_major > 5 OR (@binlogviz_major = 5 AND @binlogviz_minor >= 7), 0, 1));\n" +
		"SET @binlogviz_ro = IF(@binlogviz_maria = 1, IF(@binlogviz_major > 11 OR (@binlogviz_major = 11 AND @binlogviz_minor >= 1), 'transaction_read_only', 'tx_read_only'), IF(@binlogviz_major > 5 OR (@binlogviz_major = 5 AND @binlogviz_minor > 7) OR (@binlogviz_major = 5 AND @binlogviz_minor = 7 AND @binlogviz_patch >= 20), 'transaction_read_only', 'tx_read_only'));\n"
}

// guardLockSQL fails closed. It commits so the next statement is outside the
// transaction the information_schema reads open when autocommit is 0, then
// sets the session read-only with SET SESSION TRANSACTION READ ONLY, which
// names no variable and exists on MySQL 5.6.5+ and MariaDB 10.0+. Only the
// prepared @binlogviz_unlock_sql can make the session writable again, and it
// is SET SESSION TRANSACTION READ WRITE only when @binlogviz_ok holds this
// script's token, which the guard sets only after the check ran and matched on
// a supported server whose session was writable. A header-less block, a block
// after a reconnect, and a block from another script that is run after this
// one all stay read-only. If PREPARE fails (max_prepared_stmt_count) or any
// guard step fails, the session stays read-only and every write fails with
// ERROR 1792. A reconnect inside a block is covered by the token check in each
// undo statement, not by this lock.
const guardLockSQL = "COMMIT;\nSET SESSION TRANSACTION READ ONLY;\n" +
	"SET @binlogviz_unlock_sql = IF(@binlogviz_ok <=> '" + guardTokenMark + "', 'SET SESSION TRANSACTION READ WRITE', 'DO 0');\n" +
	"PREPARE " + guardStmtUnlock + " FROM @binlogviz_unlock_sql;\n" +
	"EXECUTE " + guardStmtUnlock + ";\n" +
	"DEALLOCATE PREPARE " + guardStmtUnlock + ";\n"

// guardSafeLabel keeps a column name from being split by MySQL's sql_mode
// parser (commas) or by the list separator used below. Every "|" becomes "/",
// so a name can never form " | " with a neighbouring separator (a name ending
// in " |" or starting with "| " did, and the byte cut then kept part of it).
func guardSafeLabel(name string) string {
	name = strings.ReplaceAll(name, "|", "/")
	return strings.ReplaceAll(name, ",", ";")
}

// renderGeneratedGuard fails the apply before any transaction when a column
// this script omitted is not GENERATED on the target. One check lists every
// mismatch. The session is always switched to read-only first; a match on a
// supported server switches it back, so sql_mode and the read-only flag end
// unchanged. A mismatch, an unsupported server, or a guard step that failed
// prints the reason, stays read-only, then fails SET sql_mode.
func renderGeneratedGuard(cols []Col) string {
	if len(cols) == 0 {
		return ""
	}
	var arms []string
	for i, col := range cols {
		name := guardTable(col.Schema, col.Table) + "." + col.Column
		arms = append(arms, fmt.Sprintf(
			"SELECT %d AS n, %s AS q, %s AS safe_q FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s AND (EXTRA LIKE '%%STORED GENERATED%%' OR EXTRA LIKE '%%VIRTUAL GENERATED%%'))",
			i+1, sqlQuote(name), sqlQuote(guardSafeLabel(name)), sqlQuote(col.Schema), sqlQuote(col.Table), sqlQuote(col.Column),
		))
	}
	lead := sqlQuote(guardMismatchLead)
	var b strings.Builder
	b.WriteString("-- Guard: every generated column omitted below must be GENERATED on the target. Apply stops here when the schema file does not match. A client that continues is left read-only, so later writes fail. Apply requires MySQL 5.7 or newer; on MariaDB the script stops before this guard. Each transaction below is read-only unless this guard ran in the same session and matched.\n")
	b.WriteString("SET @binlogviz_checked = 0;\n")
	b.WriteString("SET @binlogviz_group_concat_max_len = @@SESSION.group_concat_max_len;\n")
	b.WriteString("SET SESSION group_concat_max_len = 1048576;\n")
	b.WriteString("SELECT GROUP_CONCAT(q ORDER BY n SEPARATOR ', '),\n")
	b.WriteString("       GROUP_CONCAT(safe_q ORDER BY n SEPARATOR ' | '),\n")
	b.WriteString("       COUNT(*), 1\n")
	b.WriteString("  INTO @binlogviz_mismatch, @binlogviz_safe, @binlogviz_n, @binlogviz_checked\n")
	b.WriteString("  FROM (\n    ")
	b.WriteString(strings.Join(arms, "\n    UNION ALL\n    "))
	b.WriteString("\n  ) AS binlogviz_gen;\n")
	b.WriteString("SET SESSION group_concat_max_len = @binlogviz_group_concat_max_len;\n")
	b.WriteString(guardServerCheckSQL())
	// Read the session flag before the lock so a match can leave it as it was.
	b.WriteString("SET @binlogviz_ro_was = NULL;\n")
	b.WriteString("SET @binlogviz_ro_sql = CONCAT('SET @binlogviz_ro_was = @@SESSION.', @binlogviz_ro);\n")
	fmt.Fprintf(&b, "PREPARE %s FROM @binlogviz_ro_sql;\nEXECUTE %s;\nDEALLOCATE PREPARE %s;\n", guardStmtRO, guardStmtRO, guardStmtRO)
	fmt.Fprintf(&b, "SET @binlogviz_guard_sql = IF(@binlogviz_old <=> 0, IF(@binlogviz_checked <=> 1, IF(@binlogviz_mismatch IS NULL, IF(@binlogviz_ro_was <=> 1, CONCAT('SELECT ', QUOTE(%s)), 'DO 0'), CONCAT('SELECT ', QUOTE(CONCAT(%s, ': ', @binlogviz_mismatch, %s)))), CONCAT('SELECT ', QUOTE(%s))), CONCAT('SELECT ', QUOTE(%s)));\n", sqlQuote(guardReadOnlyMsg), lead, sqlQuote(guardMismatchHint), sqlQuote(guardNotRunMsg), sqlQuote(guardOldServerMsg))
	fmt.Fprintf(&b, "PREPARE %s FROM @binlogviz_guard_sql;\nEXECUTE %s;\nDEALLOCATE PREPARE %s;\n", guardStmtMsg, guardStmtMsg, guardStmtMsg)
	b.WriteString("SET @binlogviz_mode = @@SESSION.sql_mode;\n")
	fmt.Fprintf(&b, "SET @binlogviz_full = CONCAT(%s, ': ', IFNULL(@binlogviz_safe, ''));\n", lead)
	fmt.Fprintf(&b, "SET @binlogviz_head = CONCAT(%s, ' (', @binlogviz_n, ' columns): ');\n", lead)
	// Cut by bytes (#192): pick whole names on the binary copy, then take
	// that many characters from the original. The script runs under
	// SET NAMES utf8mb4, so the binary copy is utf8mb4.
	fmt.Fprintf(&b, "SET @binlogviz_room = %d - LENGTH(@binlogviz_head);\n", guardValueLimit)
	b.WriteString("SET @binlogviz_bin = CAST(IFNULL(@binlogviz_safe, '') AS BINARY);\n")
	b.WriteString("SET @binlogviz_win = LEFT(CONCAT(@binlogviz_bin, CAST(' | ' AS BINARY)), GREATEST(@binlogviz_room + 3, 0));\n")
	b.WriteString("SET @binlogviz_bcut = IF(LENGTH(@binlogviz_bin) <= @binlogviz_room, @binlogviz_bin, IF(LOCATE(CAST(' | ' AS BINARY), @binlogviz_win) = 0, CAST('' AS BINARY), LEFT(@binlogviz_win, LENGTH(@binlogviz_win) - LENGTH(SUBSTRING_INDEX(@binlogviz_win, CAST(' | ' AS BINARY), -1)) - 3)));\n")
	b.WriteString("SET @binlogviz_cut = IF(@binlogviz_safe IS NULL, '', LEFT(@binlogviz_safe, CHAR_LENGTH(CONVERT(@binlogviz_bcut USING utf8mb4))));\n")
	b.WriteString("SET @binlogviz_short = CONCAT(@binlogviz_head, @binlogviz_cut);\n")
	fmt.Fprintf(&b, "SET @binlogviz_mode = IF(@binlogviz_mismatch IS NULL, @binlogviz_mode, IF(CHAR_LENGTH(@binlogviz_full) <= %d AND LENGTH(@binlogviz_full) <= %d, @binlogviz_full, IF(@binlogviz_cut <> '' AND CHAR_LENGTH(@binlogviz_short) <= %d AND LENGTH(@binlogviz_short) <= %d, @binlogviz_short, CONCAT(%s, ' (', @binlogviz_n, ' columns)'))));\n", guardValueLimit, guardValueLimit, guardValueLimit, guardValueLimit, lead)
	// A match in a session that is already read-only (an earlier failed
	// script) cannot write either; say so instead of a bare ERROR 1792.
	fmt.Fprintf(&b, "SET @binlogviz_mode = IF(@binlogviz_mismatch IS NULL AND @binlogviz_ro_was <=> 1, %s, @binlogviz_mode);\n", sqlQuote(guardReadOnlyMsg))
	// An unsupported server or a check that did not run fails SET sql_mode too.
	fmt.Fprintf(&b, "SET @binlogviz_mode = IF(@binlogviz_old <=> 0, IF(@binlogviz_checked <=> 1, @binlogviz_mode, %s), %s);\n", sqlQuote(guardNotRunMsg), sqlQuote(guardOldServerMsg))
	// Set this script's token only on a match that ran on a supported server,
	// and only when the session was writable before; a NULL anywhere keeps
	// the session read-only and every undo statement a no-op.
	b.WriteString("SET @binlogviz_ok = IF(@binlogviz_old <=> 0 AND @binlogviz_checked <=> 1 AND @binlogviz_mismatch IS NULL AND NOT (@binlogviz_ro_was <=> 1), '" + guardTokenMark + "', NULL);\n")
	b.WriteString(guardLockSQL)
	b.WriteString("SET SESSION sql_mode = @binlogviz_mode;\n")
	return b.String()
}

// guardTable is the db.table label in the mismatch list. Same empty cases as
// the planner's table label, so a row with a blank schema or table prints the
// same name it did before the package split.
func guardTable(schema, table string) string {
	switch {
	case schema != "" && table != "":
		return schema + "." + table
	case table != "":
		return table
	case schema != "":
		return schema
	default:
		return "selected range"
	}
}

func sqlQuote(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '\'':
			b.WriteByte('\\')
			b.WriteByte(s[i])
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case 0:
			b.WriteString(`\0`)
		default:
			b.WriteByte(s[i])
		}
	}
	b.WriteByte('\'')
	return b.String()
}
