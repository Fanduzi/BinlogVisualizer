// Package analyzer collects selected row images for undo SQL.
// input: retained normalized events that already passed time, position, GTID, schema, table, and DML filters, plus flashback images captured by the parser.
// output: one SQL script that reverses those row changes, or one error and no script when a selected row cannot be rendered exactly, a seen table definition cannot be read, a schema file does not match the binlog columns, a schema-file generated value contradicts the expression, or the selected range contains DDL. Generated columns learned from schema SQL or parsed CREATE/ALTER are omitted from INSERT and UPDATE SET. A schema-file omission is checked by a guard after the session SET lines: apply fails before any transaction when that column is not generated on the target. A mismatch commits and sets the session read-only, and that lock is repeated before each transaction, so a client that continues cannot write. The lock is SET SESSION TRANSACTION READ ONLY, which names no server variable, and a prepared READ WRITE undoes it only when @binlogviz_ok holds this script's token, which the guard sets only after the check ran and matched on MySQL 5.7+ or MariaDB 10.2+ in a writable session, so a failed step, including PREPARE, a header-less block, another script's block, or a reconnect leaves the session read-only. Every undo statement also checks the token, so statements run in a session without it change no row. The token is a hash of the rendered script. A match in a session that is already read-only fails the guard with a disconnect hint. Older servers fail that guard with a clear message and stay read-only. The failing sql_mode value names every mismatched column without a comma, and a long list keeps the count and the first names. Each guard arm is SELECT ... FROM DUAL, which MySQL 5.7 accepts. An expression that cannot be checked exactly is omitted with one stderr warning and a header comment when nothing contradicts it. A no-primary-key WHERE keeps generated columns. A selected table with no definition is warned and still printed. An ENUM index of 0, a zero month or day, and an omitted generated column logged as NULL are each wrapped in a sql_mode save and restore that drops only the flags that would reject the stored value, plus TRADITIONAL.
// pos: optional collector on Analyzer. It runs only when Options.Flashback is set.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// flashBind is the table definition in effect for one undo row.
// deferCheck waits until every schema of an unqualified table has been seen.
type flashBind struct {
	gen         map[string]struct{}
	cols        []schemaCol
	deferCheck  bool
	fromPending bool
}

type flashGroup struct {
	key   string
	gtid  string
	path  string
	pos   int64
	rows  []model.FlashRow
	binds []flashBind
}

type flashSplit struct {
	gtid    string
	kept    bool
	skipped bool
}

func (a *Analyzer) noteFlashback(ev model.NormalizedEvent) {
	if a == nil || !a.opts.Flashback || a.flashErr != nil {
		return
	}
	if ev.EventType == "DDL" {
		a.flashErr = flashDDLError(ev)
		return
	}
	if ev.EventType != "ROWS" {
		return
	}
	if len(ev.FlashRows) == 0 {
		a.flashErr = fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{
			"Table": flashTable(ev.Schema, ev.Table),
		}))
		return
	}
	for _, row := range ev.FlashRows {
		if row.ProblemKind != "" {
			a.flashErr = flashRowError(row)
			return
		}
	}
	if a.flashGen.unknown(ev.Schema, ev.Table) {
		a.flashErr = fmt.Errorf("%s", i18n.Tf("error.flashbackGenerated", map[string]any{
			"Table": flashTable(ev.Schema, ev.Table),
		}))
		return
	}
	bind := flashBind{}
	cols, fromFile, pending, ok := a.flashGen.definition(ev.Schema, ev.Table)
	if !ok {
		a.noteFlashUnknown(flashTable(ev.Schema, ev.Table))
	} else if fromFile {
		bind.cols = cols
		bind.fromPending = pending
		bind.deferCheck = pending
		if pending {
			// The schema is not final until every table map for this name is seen.
		} else if err := a.acceptSchemaRows(ev.FlashRows, cols); err != nil {
			a.flashErr = err
			return
		} else {
			bind.gen = generatedNameSet(cols)
		}
	} else {
		bind.gen = generatedNameSet(cols)
	}
	a.appendFlashRows(ev, ev.FlashRows, bind)
	a.noteFlashbackKept(ev)
}

func (a *Analyzer) acceptSchemaRows(rows []model.FlashRow, cols []schemaCol) error {
	for _, row := range rows {
		if err := a.acceptSchema(row, cols); err != nil {
			return err
		}
	}
	return nil
}

func (a *Analyzer) acceptSchema(row model.FlashRow, cols []schemaCol) error {
	warns, err := validateSchemaRow(row, cols)
	if err != nil {
		return err
	}
	a.noteSchemaWarnings(warns)
	return nil
}

func (a *Analyzer) noteSchemaWarnings(warns []string) {
	for _, warn := range warns {
		dup := false
		for _, have := range a.flashSchemaWarn {
			if have == warn {
				dup = true
				break
			}
		}
		if !dup {
			a.flashSchemaWarn = append(a.flashSchemaWarn, warn)
		}
	}
}

func (a *Analyzer) clearPendingUse(table string) {
	for gi := range a.flashGroups {
		group := &a.flashGroups[gi]
		for i := range group.binds {
			if !group.binds[i].fromPending || !strings.EqualFold(group.rows[i].Table, table) {
				continue
			}
			group.binds[i] = flashBind{}
		}
	}
}

func (a *Analyzer) finishSchema() error {
	for gi := range a.flashGroups {
		group := &a.flashGroups[gi]
		for i := range group.binds {
			bind := &group.binds[i]
			if !bind.deferCheck {
				continue
			}
			if err := a.acceptSchema(group.rows[i], bind.cols); err != nil {
				return err
			}
			bind.gen = generatedNameSet(bind.cols)
			bind.deferCheck = false
		}
	}
	return a.reviewGenerated()
}

func (a *Analyzer) appendFlashRows(ev model.NormalizedEvent, rows []model.FlashRow, bind flashBind) {
	copied := append([]model.FlashRow(nil), rows...)
	binds := make([]flashBind, len(copied))
	for i := range binds {
		binds[i] = bind
		if bind.gen != nil {
			binds[i].gen = copyNames(bind.gen)
		}
		if bind.cols != nil {
			binds[i].cols = cloneSchemaCols(bind.cols)
		}
	}
	key := ev.TxnKey
	if n := len(a.flashGroups); n > 0 && key != "" && a.flashGroups[n-1].key == key {
		a.flashGroups[n-1].rows = append(a.flashGroups[n-1].rows, copied...)
		a.flashGroups[n-1].binds = append(a.flashGroups[n-1].binds, binds...)
		return
	}
	path := ev.TxnStartPath
	pos := ev.TxnStartPos
	if path == "" {
		path = ev.BinlogPath
	}
	if pos == 0 {
		pos = ev.PositionStart
	}
	a.flashGroups = append(a.flashGroups, flashGroup{
		key:   key,
		gtid:  ev.TxnGTID,
		path:  path,
		pos:   pos,
		rows:  copied,
		binds: binds,
	})
}

func (a *Analyzer) noteFlashbackKept(ev model.NormalizedEvent) {
	a.noteFlashSplit(ev.TxnKey, ev.TxnGTID, true)
}

func (a *Analyzer) noteFlashbackSkipped() {
	if a == nil || a.txnBuilder == nil {
		return
	}
	gtid, _, _ := a.txnBuilder.touchLocation()
	a.noteFlashSplit(a.txnBuilder.CurrentTxnKey(), gtid, false)
}

func (a *Analyzer) noteFlashSplit(key, gtid string, kept bool) {
	if a == nil || key == "" {
		return
	}
	if a.flashSplit == nil {
		a.flashSplit = map[string]*flashSplit{}
	}
	split, ok := a.flashSplit[key]
	if !ok {
		split = &flashSplit{}
		a.flashSplit[key] = split
		a.flashSplitOrder = append(a.flashSplitOrder, key)
	}
	if gtid != "" {
		split.gtid = gtid
	}
	if kept {
		split.kept = true
	} else {
		split.skipped = true
	}
}

func (a *Analyzer) noteFlashUnknown(table string) {
	for _, have := range a.flashUnknown {
		if have == table {
			return
		}
	}
	a.flashUnknown = append(a.flashUnknown, table)
}

// FlashbackWarnings reports tables whose definitions were not seen, then
// transactions a table or --dml filter undid only in part.
// Empty when flashback has nothing to print.
func (a *Analyzer) FlashbackWarnings() []string {
	if a == nil {
		return nil
	}
	var out []string
	out = append(out, a.flashGen.schemaWarnings()...)
	out = append(out, a.flashSchemaWarn...)
	for _, table := range a.flashUnknown {
		out = append(out, i18n.Tf("warning.flashbackGenerated", map[string]any{"Table": table}))
	}
	for _, key := range a.flashSplitOrder {
		split := a.flashSplit[key]
		if split == nil || !split.kept || !split.skipped {
			continue
		}
		gtid := split.gtid
		if gtid == "" {
			gtid = "GTID unavailable"
		}
		out = append(out, i18n.Tf("warning.flashbackSplit", map[string]any{"GTID": gtid}))
	}
	return out
}

// FlashbackSQL returns undo SQL for the selected row changes.
// A non-nil error means the script must not be applied; the string is empty.
func (a *Analyzer) FlashbackSQL() (string, error) {
	if a == nil {
		return "", fmt.Errorf("%s", i18n.T("error.flashbackCapture", map[string]any{"Table": "selected range"}))
	}
	if a.flashErr != nil {
		return "", a.flashErr
	}
	if err := a.finishSchema(); err != nil {
		return "", err
	}
	var kept []flashGroup
	for _, group := range a.flashGroups {
		if len(group.rows) > 0 {
			kept = append(kept, group)
		}
	}
	if len(kept) == 0 {
		return "", nil
	}
	sql, err := renderFlashbackSQL(kept, a.flashGenNotes)
	if err != nil {
		return "", err
	}
	return sql, nil
}

// reviewGenerated checks schema-file generated columns against every logged image.
// A contradiction refuses the script. An expression that cannot be checked exactly
// is omitted, with a warning, when nothing contradicts it.
func (a *Analyzer) reviewGenerated() error {
	if a.flashReviewed {
		return a.flashReviewErr
	}
	a.flashReviewed = true
	a.flashReviewErr = a.collectGeneratedReview()
	return a.flashReviewErr
}

func (a *Analyzer) collectGeneratedReview() error {
	type bucket struct {
		schema string
		table  string
		cols   []schemaCol
		images []loggedImage
	}
	var order []string
	groups := map[string]*bucket{}
	for _, group := range a.flashGroups {
		for i, row := range group.rows {
			if i >= len(group.binds) {
				continue
			}
			cols := group.binds[i].cols
			sig := generatedSig(cols)
			if sig == "" {
				continue
			}
			key := strings.ToLower(row.Schema) + "\x00" + strings.ToLower(row.Table) + "\x00" + sig
			b := groups[key]
			if b == nil {
				b = &bucket{schema: row.Schema, table: row.Table, cols: cols}
				groups[key] = b
				order = append(order, key)
			}
			if row.Before != nil {
				b.images = append(b.images, loggedImage{columns: row.Columns, values: row.Before})
			}
			if row.After != nil {
				b.images = append(b.images, loggedImage{columns: row.Columns, values: row.After})
			}
		}
	}
	var mismatches, notes, warns []string
	for _, key := range order {
		b := groups[key]
		table := flashTable(b.schema, b.table)
		for _, col := range b.cols {
			if !col.generated {
				continue
			}
			example, unknown := generatedColumnOutcome(col, b.images, b.cols)
			if example != "" {
				mismatches = append(mismatches, generatedMismatch(table, col, example).Error())
				continue
			}
			if !unknown {
				continue
			}
			notes = append(notes, unverifiedGeneratedComment(table, col))
			warns = append(warns, unverifiedGeneratedWarning(table, col))
		}
	}
	if len(mismatches) > 0 {
		return fmt.Errorf("%s", strings.Join(mismatches, "\n"))
	}
	a.noteSchemaWarnings(warns)
	a.flashGenNotes = append(a.flashGenNotes, notes...)
	return nil
}

func generatedSig(cols []schemaCol) string {
	var b strings.Builder
	found := false
	for _, col := range cols {
		if !col.generated {
			continue
		}
		found = true
		b.WriteString(strings.ToLower(col.name))
		b.WriteByte(0)
		b.WriteString(col.expr)
		b.WriteByte(0)
	}
	if !found {
		return ""
	}
	return b.String()
}

func flashRowError(row model.FlashRow) error {
	table := flashTable(row.Schema, row.Table)
	switch row.ProblemKind {
	case model.FlashProblemNames:
		return fmt.Errorf("%s", i18n.Tf("error.flashbackNames", map[string]any{"Table": table}))
	case model.FlashProblemImage:
		return fmt.Errorf("%s", i18n.Tf("error.flashbackImage", map[string]any{"Table": table}))
	case model.FlashProblemType:
		column := row.ProblemColumn
		if column == "" {
			column = "?"
		}
		return fmt.Errorf("%s", i18n.Tf("error.flashbackType", map[string]any{
			"Table":  table,
			"Column": column,
			"Type":   row.ProblemType,
		}))
	default:
		return fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": table}))
	}
}

func flashDDLError(ev model.NormalizedEvent) error {
	statement := strings.TrimSpace(ev.QuerySQL)
	if statement == "" {
		statement = "DDL"
	}
	statement = oneLine(statement, 120)
	return fmt.Errorf("%s", i18n.Tf("error.flashbackDDL", map[string]any{
		"Table":     flashTable(ev.Schema, ev.Table),
		"Statement": statement,
	}))
}

func flashTable(schema, table string) string {
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

func oneLine(value string, limit int) string {
	value = strings.TrimSpace(value)
	if i := strings.IndexAny(value, "\r\n"); i >= 0 {
		value = strings.TrimSpace(value[:i])
	}
	if limit > 0 && len(value) > limit {
		value = value[:limit] + "..."
	}
	return value
}

func renderFlashbackSQL(groups []flashGroup, notes []string) (string, error) {
	var b strings.Builder
	b.WriteString("-- flashback reverses the selected row changes, last transaction first.\n")
	b.WriteString("-- TIMESTAMP literals are the UTC wall clock of the stored instant. Review this script before applying it.\n")
	for _, note := range notes {
		b.WriteString(note)
		b.WriteByte('\n')
	}
	b.WriteString("SET NAMES utf8mb4;\n")
	b.WriteString("SET time_zone = '+00:00';\n")
	b.WriteString("SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '');\n")
	guard := renderGeneratedGuard(groups)
	if guard != "" {
		b.WriteByte('\n')
		b.WriteString(guard)
	}
	// With a guard, every undo statement also checks this script's token, so
	// a statement run in a session where this script's guard did not match
	// (a reconnect inside a block, a block from another script) changes no row.
	tokenCond := ""
	if guard != "" {
		tokenCond = guardTokenCond
	}
	for i := len(groups) - 1; i >= 0; i-- {
		group := groups[i]
		b.WriteByte('\n')
		gtid := group.gtid
		if gtid == "" {
			gtid = "GTID unavailable"
		}
		fmt.Fprintf(&b, "-- gtid: %s\n", oneLine(gtid, 0))
		file := filepath.Base(group.path)
		if file == "" || file == "." {
			file = "binlog"
		}
		fmt.Fprintf(&b, "-- binlog: %s:%d\n", oneLine(file, 0), group.pos)
		if guard != "" {
			// SET sql_mode, COMMIT, START TRANSACTION, and SET GTID_NEXT do not
			// clear this session flag. Repeat the same lock outside a
			// transaction so this block cannot start writable: it is read-only
			// unless the header guard ran in this session and matched.
			b.WriteString(guardLockSQL)
		}
		b.WriteString("START TRANSACTION;\n")
		for j := len(group.rows) - 1; j >= 0; j-- {
			var gen map[string]struct{}
			if j < len(group.binds) {
				gen = group.binds[j].gen
			}
			statement, err := renderUndoStatement(group.rows[j], gen, tokenCond)
			if err != nil {
				return "", err
			}
			b.WriteString(statement)
			b.WriteByte('\n')
		}
		b.WriteString("COMMIT;\n")
	}
	out := b.String()
	if guard != "" {
		// The token is a hash of the script with a fixed placeholder, so the
		// same input always prints the same script and two different scripts
		// get different tokens.
		sum := sha256.Sum256([]byte(out))
		out = strings.ReplaceAll(out, guardTokenMark, hex.EncodeToString(sum[:8]))
	}
	return out, nil
}

// renderUndoStatement prints one undo statement. A non-empty cond is added to
// the statement so it changes no row unless cond is true: an INSERT becomes
// INSERT ... SELECT ... FROM DUAL WHERE cond, and UPDATE and DELETE add
// AND cond to the WHERE.
func renderUndoStatement(row model.FlashRow, gen map[string]struct{}, cond string) (string, error) {
	table := quoteIdent(row.Schema) + "." + quoteIdent(row.Table)
	var statement string
	var err error
	switch row.Op {
	case "DELETE":
		cols, vals := assignColumns(row.Columns, row.Before, gen)
		if len(cols) == 0 {
			return "", generatedRowError(row)
		}
		if cond != "" {
			statement = "INSERT INTO " + table + " (" + quoteColumnList(cols) + ") SELECT " + strings.Join(vals, ", ") + " FROM DUAL WHERE " + cond + ";"
		} else {
			statement = "INSERT INTO " + table + " (" + quoteColumnList(cols) + ") VALUES (" + strings.Join(vals, ", ") + ");"
		}
	case "UPDATE":
		cols, vals := assignColumns(row.Columns, row.Before, gen)
		if len(cols) == 0 {
			return "", generatedRowError(row)
		}
		sets := make([]string, len(cols))
		for i := range cols {
			sets[i] = quoteIdent(cols[i]) + " = " + vals[i]
		}
		statement, err = undoWhere(row, "UPDATE "+table+" SET "+strings.Join(sets, ", "), row.After, cond)
	default:
		statement, err = undoWhere(row, "DELETE FROM "+table, row.After, cond)
	}
	if err != nil {
		return "", err
	}
	if drops := relaxedModes(row, gen); len(drops) > 0 {
		statement = sqlModeWrap(statement, drops)
	}
	return statement, nil
}

// relaxedModes lists the sql_mode flags that would reject a value this row
// stored legally under the session that wrote it. Each flag only turns a
// stored value into an error, so dropping it for this one statement changes
// no restored value:
//   - ENUM index 0 (the error member) needs the strict modes off.
//   - A zero month or day needs NO_ZERO_DATE and NO_ZERO_IN_DATE off. Strict
//     stays on, so every other value in the statement is still checked.
//   - A generated column left out of INSERT or UPDATE SET is recomputed by
//     the server. When its logged value is NULL the expression may divide by
//     zero, which ERROR_FOR_DIVISION_BY_ZERO turns into ERROR 1365. Without
//     that flag x/0 is NULL, the value the binlog holds.
//
// TRADITIONAL is removed with any of them: MySQL re-expands that token into
// all of the flags above.
func relaxedModes(row model.FlashRow, gen map[string]struct{}) []string {
	var drops []string
	if row.NonStrict {
		drops = append(drops, "STRICT_ALL_TABLES", "STRICT_TRANS_TABLES")
	}
	if row.ZeroDate {
		drops = append(drops, "NO_ZERO_DATE", "NO_ZERO_IN_DATE")
	}
	if row.Op != "INSERT" && omittedGeneratedNull(row.Columns, row.Before, gen) {
		drops = append(drops, "ERROR_FOR_DIVISION_BY_ZERO")
	}
	if len(drops) > 0 {
		drops = append(drops, "TRADITIONAL")
	}
	return drops
}

func omittedGeneratedNull(columns, values []string, gen map[string]struct{}) bool {
	for i, column := range columns {
		if isGenerated(gen, column) && i < len(values) && values[i] == "NULL" {
			return true
		}
	}
	return false
}

// sqlModeWrap runs one statement with the listed sql_mode flags removed and
// restores the session mode immediately after it. @binlogviz_sql_mode holds
// the mode that was in effect, including the script's earlier
// NO_BACKSLASH_ESCAPES removal.
func sqlModeWrap(statement string, drops []string) string {
	expr := "CONCAT(',', @@SESSION.sql_mode, ',')"
	for _, flag := range drops {
		expr = "REPLACE(" + expr + ", '," + flag + ",', ',')"
	}
	return "SET @binlogviz_sql_mode = @@SESSION.sql_mode;\n" +
		"SET SESSION sql_mode = TRIM(BOTH ',' FROM " + expr + ");\n" +
		statement + "\n" +
		"SET SESSION sql_mode = @binlogviz_sql_mode;"
}

func assignColumns(columns, values []string, gen map[string]struct{}) ([]string, []string) {
	cols := make([]string, 0, len(columns))
	vals := make([]string, 0, len(values))
	for i, column := range columns {
		if isGenerated(gen, column) {
			continue
		}
		cols = append(cols, column)
		val := "NULL"
		if i < len(values) {
			val = values[i]
		}
		vals = append(vals, val)
	}
	return cols, vals
}

func isGenerated(gen map[string]struct{}, name string) bool {
	if len(gen) == 0 {
		return false
	}
	_, ok := gen[strings.ToLower(name)]
	return ok
}

func generatedRowError(row model.FlashRow) error {
	return fmt.Errorf("%s", i18n.Tf("error.flashbackGeneratedRow", map[string]any{
		"Table": flashTable(row.Schema, row.Table),
	}))
}

func undoWhere(row model.FlashRow, head string, image []string, cond string) (string, error) {
	pk := !row.NoPK && len(row.PK) > 0
	indexes := row.PK
	if !pk {
		indexes = make([]int, len(row.Columns))
		for i := range indexes {
			indexes[i] = i
		}
	}
	parts := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(row.Columns) || index >= len(image) {
			continue
		}
		parts = append(parts, quoteIdent(row.Columns[index])+" <=> "+image[index])
	}
	if len(parts) == 0 {
		return "", generatedRowError(row)
	}
	if cond != "" {
		parts = append(parts, cond)
	}
	statement := head + " WHERE " + strings.Join(parts, " AND ")
	if !pk {
		statement += " LIMIT 1"
		return "-- no primary key on " + flashTable(row.Schema, row.Table) + "; this matches every column and LIMIT 1\n" + statement + ";", nil
	}
	return statement + ";", nil
}

type guardCol struct {
	schema string
	table  string
	column string
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
const guardTokenCond = "@binlogviz_ok <=> '" + guardTokenMark + "'"
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
	"SET @binlogviz_unlock_sql = IF(" + guardTokenCond + ", 'SET SESSION TRANSACTION READ WRITE', 'DO 0');\n" +
	"PREPARE " + guardStmtUnlock + " FROM @binlogviz_unlock_sql;\n" +
	"EXECUTE " + guardStmtUnlock + ";\n" +
	"DEALLOCATE PREPARE " + guardStmtUnlock + ";\n"

// guardSafeLabel keeps a column name from being split by MySQL's sql_mode
// parser (commas) or by the list separator used below.
func guardSafeLabel(name string) string {
	name = strings.ReplaceAll(name, " | ", " / ")
	return strings.ReplaceAll(name, ",", ";")
}

// trimGuardList keeps only whole names that fit in room runes.
// The SQL in renderGeneratedGuard implements this same cut.
func trimGuardList(list string, room int) string {
	if room < 0 {
		room = 0
	}
	runes := []rune(list)
	if len(runes) <= room {
		return list
	}
	cut := string(runes[:room])
	extended := []rune(list + " | ")
	if room+3 <= len(extended) && string(extended[room:room+3]) == " | " {
		return cut
	}
	idx := strings.LastIndex(cut, " | ")
	if idx < 0 {
		return ""
	}
	return cut[:idx]
}

// guardErrorValue is the sql_mode string the guard assigns on a mismatch.
// It stays within guardValueLimit runes and bytes, and it contains no comma.
func guardErrorValue(safeNames []string) string {
	list := strings.Join(safeNames, " | ")
	full := guardMismatchLead + ": " + list
	if utf8.RuneCountInString(full) <= guardValueLimit && len(full) <= guardValueLimit {
		return full
	}
	head := fmt.Sprintf("%s (%d columns): ", guardMismatchLead, len(safeNames))
	short := head + trimGuardList(list, guardValueLimit-utf8.RuneCountInString(head))
	if utf8.RuneCountInString(short) <= guardValueLimit && len(short) <= guardValueLimit {
		return short
	}
	return fmt.Sprintf("%s (%d columns)", guardMismatchLead, len(safeNames))
}

// renderGeneratedGuard fails the apply before any transaction when a column
// this script omitted is not GENERATED on the target. One check lists every
// mismatch. The session is always switched to read-only first; a match on a
// supported server switches it back, so sql_mode and the read-only flag end
// unchanged. A mismatch, an unsupported server, or a guard step that failed
// prints the reason, stays read-only, then fails SET sql_mode.
func renderGeneratedGuard(groups []flashGroup) string {
	cols := omittedGeneratedCols(groups)
	if len(cols) == 0 {
		return ""
	}
	var arms []string
	for i, col := range cols {
		name := flashTable(col.schema, col.table) + "." + col.column
		arms = append(arms, fmt.Sprintf(
			"SELECT %d AS n, %s AS q, %s AS safe_q FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s AND (EXTRA LIKE '%%STORED GENERATED%%' OR EXTRA LIKE '%%VIRTUAL GENERATED%%'))",
			i+1, sqlQuote(name), sqlQuote(guardSafeLabel(name)), sqlQuote(col.schema), sqlQuote(col.table), sqlQuote(col.column),
		))
	}
	lead := sqlQuote(guardMismatchLead)
	var b strings.Builder
	b.WriteString("-- Guard: every generated column omitted below must be GENERATED on the target. Apply stops here when the schema file does not match. A client that continues is left read-only, so later writes fail. Apply requires MySQL 5.7 or MariaDB 10.2 or newer. Each transaction below is read-only unless this guard ran in the same session and matched.\n")
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
	fmt.Fprintf(&b, "SET @binlogviz_room = %d - CHAR_LENGTH(@binlogviz_head);\n", guardValueLimit)
	b.WriteString("SET @binlogviz_cut = IF(@binlogviz_safe IS NULL, '', LEFT(@binlogviz_safe, GREATEST(@binlogviz_room, 0)));\n")
	b.WriteString("SET @binlogviz_cut = IF(@binlogviz_safe IS NULL OR CHAR_LENGTH(@binlogviz_safe) <= @binlogviz_room, IFNULL(@binlogviz_safe, ''), IF(SUBSTRING(CONCAT(@binlogviz_safe, ' | '), CHAR_LENGTH(@binlogviz_cut) + 1, 3) = ' | ', @binlogviz_cut, IF(LOCATE(' | ', @binlogviz_cut) = 0, '', LEFT(@binlogviz_cut, GREATEST(CHAR_LENGTH(@binlogviz_cut) - CHAR_LENGTH(SUBSTRING_INDEX(@binlogviz_cut, ' | ', -1)) - 3, 0)))));\n")
	b.WriteString("SET @binlogviz_short = CONCAT(@binlogviz_head, @binlogviz_cut);\n")
	fmt.Fprintf(&b, "SET @binlogviz_mode = IF(@binlogviz_mismatch IS NULL, @binlogviz_mode, IF(CHAR_LENGTH(@binlogviz_full) <= %d AND LENGTH(@binlogviz_full) <= %d, @binlogviz_full, IF(CHAR_LENGTH(@binlogviz_short) <= %d AND LENGTH(@binlogviz_short) <= %d, @binlogviz_short, CONCAT(%s, ' (', @binlogviz_n, ' columns)'))));\n", guardValueLimit, guardValueLimit, guardValueLimit, guardValueLimit, lead)
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

func omittedGeneratedCols(groups []flashGroup) []guardCol {
	var out []guardCol
	seen := map[string]struct{}{}
	for _, group := range groups {
		for i, row := range group.rows {
			if i >= len(group.binds) {
				continue
			}
			for _, col := range group.binds[i].cols {
				if !col.generated {
					continue
				}
				key := strings.ToLower(row.Schema) + "\x00" + strings.ToLower(row.Table) + "\x00" + strings.ToLower(col.name)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, guardCol{schema: row.Schema, table: row.Table, column: col.name})
			}
		}
	}
	return out
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

func quoteColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoteIdent(column)
	}
	return strings.Join(quoted, ", ")
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
