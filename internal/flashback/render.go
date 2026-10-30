// Package flashback renders undo SQL for the planner's kept groups.
// input: flash groups with row images, schema binds, and review notes.
// output: the reverse-order script written through guard pieces, or one error and no script.
// pos: the writer half of the planner; planner.go keeps the selection and intake half.
// note: if this file changes, update this header and README.md.
package flashback

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"binlogviz/internal/guard"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/sqlemit"
)

// renderFlashbackSQL renders groups held in memory (tests and small inputs).
func renderFlashbackSQL(groups []flashGroup, notes []string) (string, error) {
	var b strings.Builder
	err := writeFlashbackScript(&b, func(fn func(*flashGroup) error) error {
		for i := len(groups) - 1; i >= 0; i-- {
			group := groups[i]
			group.id, group.chunk = int64(i+1), 0
			if err := fn(&group); err != nil {
				return err
			}
		}
		return nil
	}, notes, omittedGeneratedCols(groups), false)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

// writeFlashbackScript renders the script twice through guard.Write: once to
// choose the session token, once to seal the digest. Nothing reaches w when
// a statement cannot be rendered. The published script does not commit
// (ADR 0013, ADR 0014).
func writeFlashbackScript(w io.Writer, backward func(func(*flashGroup) error) error, notes []string, cols []guard.Col, postImage bool) error {
	g := guard.New(cols)
	return g.Write(w, func(emit func(guard.Piece) error) error {
		return renderFlashbackTo(emit, backward, notes, g, postImage)
	})
}

func renderFlashbackTo(emit func(guard.Piece) error, backward func(func(*flashGroup) error) error, notes []string, g guard.Guard, postImage bool) error {
	var b strings.Builder
	b.WriteString("-- flashback reverses the selected row changes, last transaction first.\n")
	b.WriteString("-- TIMESTAMP literals are the UTC wall clock of the stored instant. Review this script before applying it.\n")
	b.WriteString("-- This file does not commit. Apply it with: binlogviz flashback verify <file> | mysql --default-character-set=utf8mb4\n")
	b.WriteString("-- The undo is one XA transaction. BEGIN, START TRANSACTION, and DDL while it is open error and do not commit. Apply needs CREATE, CREATE ROUTINE, and the ALTER-ROUTINE privilege on binlogviz.*, SELECT, INSERT, UPDATE, DELETE on the tables the undo changes, and the stock mysql client (DELIMITER).\n")
	b.WriteString("-- verify names the finish procedure for that run and drops it at the end. Drop leftover helpers with DROP DATABASE binlogviz.\n")
	for _, note := range notes {
		b.WriteString(note)
		if !strings.HasSuffix(note, "\n") {
			b.WriteByte('\n')
		}
	}
	if postImage {
		b.WriteString("-- --conflict guard: each undo matches the logged after image, so a row changed after the batch is left unchanged (0 rows affected). An undo that moves a primary key back changes no row when another row holds that key.\n")
	}
	if err := emitComments(emit, b.String()); err != nil {
		return err
	}
	// The mode strip is first. Its payload is quoted without backslashes, so a
	// session that already has NO_BACKSLASH_ESCAPES can parse it and execute
	// it. Later payloads may use backslash escapes.
	// These run before CREATE PROCEDURE, so they cannot CALL it. A failure
	// here leaves no procedure.
	for i, stmt := range []string{
		// Saved here, before CREATE PROCEDURE, and restored after the finish.
		// PAD_CHAR_TO_FULL_LENGTH makes CHAR equality pad under NO PAD
		// collations, so a keyed undo matches 0 rows and still commits (#347).
		"SET @binlogviz_saved_mode = @@SESSION.sql_mode, SESSION sql_mode = REPLACE(REPLACE(@@SESSION.sql_mode, 'PAD_CHAR_TO_FULL_LENGTH', ''), 'NO_BACKSLASH_ESCAPES', '')",
		"SET NAMES utf8mb4",
		"SET time_zone = '+00:00'",
		"SET @binlogviz_bad = NULL",
		"SET @binlogviz_txn = 0",
		"SET @binlogviz_next_mode = NULL",
	} {
		text := stmt + ";\n"
		// A newline in this payload is quoted with a backslash. The mode
		// strip is first and may run while NO_BACKSLASH_ESCAPES is still on.
		if i == 0 {
			text = stmt + ";"
		}
		if err := emit(guard.Piece{Kind: guard.PieceRaw, Text: text}); err != nil {
			return err
		}
		if i == 0 {
			// Right after the mode strip, so its quoting can use backslashes.
			if err := emit(guard.Piece{Kind: guard.PieceRaw, Text: guard.MariaDBStopSQL()}); err != nil {
				return err
			}
		}
	}
	if h := g.Header(); h != "" {
		if err := emit(guard.Piece{Kind: guard.PieceRaw, Text: h}); err != nil {
			return err
		}
	}
	// One XA transaction for the whole undo (ADR 0014). XA START and the
	// finish procedure cannot be prepared. A later BEGIN, START TRANSACTION,
	// or DDL errors inside the XA and does not commit it.
	if err := emit(guard.Piece{Kind: guard.PieceRaw, Text: guard.ProcedureSQL()}); err != nil {
		return err
	}
	if err := emit(guard.Piece{Kind: guard.PieceRaw, Text: guard.UndoOpenSQL()}); err != nil {
		return err
	}
	if err := emit(guard.Piece{Kind: guard.PieceStmt, Text: "SET @binlogviz_txn = 1"}); err != nil {
		return err
	}
	tokenCond := g.RowCond()
	// Parts of one transaction arrive last part first. The block comment
	// opens at the first part seen. There is no per-block COMMIT.
	open := int64(-1)
	return backward(func(group *flashGroup) error {
		if group.id != open {
			open = group.id
			if err := writeBlockHead(emit, group, g); err != nil {
				return err
			}
		}
		for j := len(group.rows) - 1; j >= 0; j-- {
			var gen map[string]struct{}
			if j < len(group.binds) {
				gen = group.binds[j].gen
			}
			statement, err := renderUndo(group.rows[j], gen, tokenCond, postImage)
			if err != nil {
				return err
			}
			if err := emitSQL(emit, statement); err != nil {
				return err
			}
		}
		return nil
	})
}

func emitComments(emit func(guard.Piece) error, text string) error {
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if err := emit(guard.Piece{Kind: guard.PieceComment, Text: line + "\n"}); err != nil {
			return err
		}
	}
	return nil
}

// emitSQL splits a renderer string into comments and statements. A statement
// is one line or several, and ends at a semicolon that ends a line.
func emitSQL(emit func(guard.Piece) error, sql string) error {
	var b strings.Builder
	flush := func() error {
		s := strings.TrimSpace(b.String())
		b.Reset()
		if s == "" {
			return nil
		}
		// A routine runs with the sql_mode stored at CREATE PROCEDURE, so a
		// SET of sql_mode has to read @@SESSION.sql_mode here, at the client.
		// Mode 0 applies @binlogviz_next_mode inside the same CALL as the undo.
		if strings.HasPrefix(s, "SET ") && strings.Contains(s, "sql_mode") {
			if !strings.HasSuffix(s, ";") {
				s += ";"
			}
			return emit(guard.Piece{Kind: guard.PieceRaw, Text: s + "\n"})
		}
		return emit(guard.Piece{Kind: guard.PieceStmt, Text: s})
	}
	for _, line := range strings.Split(sql, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" {
			continue
		}
		if strings.HasPrefix(trim, "--") {
			if err := flush(); err != nil {
				return err
			}
			if err := emit(guard.Piece{Kind: guard.PieceComment, Text: trim + "\n"}); err != nil {
				return err
			}
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
		if strings.HasSuffix(strings.TrimRight(line, " \t"), ";") {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func writeBlockHead(emit func(guard.Piece) error, group *flashGroup, g guard.Guard) error {
	gtid := group.gtid
	if gtid == "" {
		gtid = "GTID unavailable"
	}
	file := filepath.Base(group.path)
	if file == "" || file == "." {
		file = "binlog"
	}
	if err := emit(guard.Piece{Kind: guard.PieceComment, Text: "-- gtid: " + mysqlexpr.OneLine(gtid, 0) + "\n"}); err != nil {
		return err
	}
	if err := emit(guard.Piece{Kind: guard.PieceComment, Text: fmt.Sprintf("-- binlog: %s:%d\n", mysqlexpr.OneLine(file, 0), group.pos)}); err != nil {
		return err
	}
	gate := g.BlockGate()
	if gate == "" {
		return nil
	}
	// The old per-block lock began with COMMIT, which would persist a prefix.
	// A block that is not inside this script's transaction (no header, a
	// reconnect, another script's token) becomes read-only instead. The gate
	// prepares its own statement, so it is raw: a prepared statement cannot
	// contain PREPARE.
	return emit(guard.Piece{Kind: guard.PieceRaw, Text: gate})
}

// renderUndoStatement prints one undo statement. A non-empty cond is added to
// the statement: an INSERT becomes INSERT ... SELECT ... FROM DUAL WHERE cond,
// and UPDATE and DELETE add AND cond to the WHERE. The token condition errors
// with 1242 when it does not match. A conflict-guard condition matches no row.
func renderUndoStatement(row model.FlashRow, gen map[string]struct{}, cond string) (string, error) {
	return renderUndo(row, gen, cond, false)
}

// renderUndo is renderUndoStatement with the --conflict guard switch. With
// postImage, an UPDATE or DELETE undo matches every logged after-image
// column, a DELETE undo inserts only when that row is still absent (the key,
// or every inserted column when there is no key), and an UPDATE undo that
// moves the key back runs only when no other row holds it.
func renderUndo(row model.FlashRow, gen map[string]struct{}, cond string, postImage bool) (string, error) {
	table := quoteIdent(row.Schema) + "." + quoteIdent(row.Table)
	var statement string
	var err error
	switch row.Op {
	case "DELETE":
		cols, vals := assignColumns(row.Columns, row.Before, gen)
		if len(cols) == 0 {
			return "", generatedRowError(row)
		}
		var conds []string
		if postImage {
			// A no-PK insert has no key. Match the inserted image so a second
			// apply finds the row and skips, instead of inserting it again.
			match := keyMatch(row, row.Before)
			if row.NoPK || len(row.PK) == 0 {
				match = insertedImageMatch(cols, vals)
			}
			if match != "" {
				conds = append(conds, "NOT EXISTS (SELECT 1 FROM "+table+" WHERE "+match+")")
			}
		}
		if cond != "" {
			conds = append(conds, cond)
		}
		if len(conds) > 0 {
			statement = "INSERT INTO " + table + " (" + quoteColumnList(cols) + ") SELECT " + strings.Join(vals, ", ") + " FROM DUAL WHERE " + strings.Join(conds, " AND ") + ";"
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
		where := cond
		if postImage {
			where = joinConds(movedKeyFreeCond(row, table), cond)
		}
		statement, err = undoWhereImage(row, "UPDATE "+table+" SET "+strings.Join(sets, ", "), row.After, where, postImage)
	default:
		statement, err = undoWhereImage(row, "DELETE FROM "+table, row.After, cond, postImage)
	}
	if err != nil {
		return "", err
	}
	if drops := relaxedModes(row, gen); len(drops) > 0 {
		statement = sqlModeWrap(statement, drops)
	}
	return tagUndo(statement, undoMark(row, postImage)), nil
}

// undoMark is the two-character tag inside the digested statement. E requires
// ROW_COUNT() = 1, including a no-PK LIMIT 1 UPDATE or DELETE. T counts an
// INSERT undo without a row check. G is --conflict guard: 1 row counts, 0
// rows is a skip, any other count rolls the undo back.
func undoMark(row model.FlashRow, postImage bool) string {
	if postImage {
		return "G"
	}
	if row.Op == "DELETE" {
		return "T"
	}
	return "E"
}

// tagUndo marks the digested statement. The mark is two characters, so
// deleting it changes the digest.
func tagUndo(statement, mark string) string {
	tag := mark + "\n"
	if i := strings.LastIndex(statement, "\n"); i >= 0 {
		return statement[:i+1] + tag + statement[i+1:]
	}
	return tag + statement
}

// movedKeyFreeCond is the --conflict guard condition for an UPDATE undo that
// moves the primary key back (#270). When another row holds the old key, the
// move-back would fail with ERROR 1062 and roll back the whole block; with
// this condition it changes no row, like any other row changed later. The row
// being undone is excluded, so a key change the collation treats as equal
// ('abc' to 'ABC') is still undone. The inner SELECT is a derived table with
// LIMIT so MySQL materializes it: a plain subquery on the updated table is
// ERROR 1093. It is "" when the key did not change or there is no key.
func movedKeyFreeCond(row model.FlashRow, table string) string {
	if row.NoPK || len(row.PK) == 0 || !keyMoved(row) {
		return ""
	}
	return "NOT EXISTS (SELECT 1 FROM (SELECT 1 FROM " + table + " WHERE " + keyMatch(row, row.Before) + " AND NOT (" + keyMatch(row, row.After) + ") LIMIT 1) AS binlogviz_key)"
}

// keyMoved reports whether an UPDATE logged a different primary-key value.
func keyMoved(row model.FlashRow) bool {
	for _, i := range row.PK {
		if i < 0 || i >= len(row.Before) || i >= len(row.After) {
			return false
		}
		if row.Before[i] != row.After[i] {
			return true
		}
	}
	return false
}

func joinConds(a, b string) string {
	return sqlemit.JoinConds(a, b)
}

func relaxedModes(row model.FlashRow, gen map[string]struct{}) []string {
	return sqlemit.RelaxedModes(row, gen)
}

func omittedGeneratedNull(columns, values []string, gen map[string]struct{}) bool {
	return sqlemit.OmittedGeneratedNull(columns, values, gen)
}

// sqlModeWrap names the sql_mode for one statement. The client computes it
// from the real session mode. Mode 0 applies @binlogviz_next_mode for that
// CALL only: a routine ignores a session sql_mode set before CALL, and MySQL
// restores the session mode when the routine returns.
func sqlModeWrap(statement string, drops []string) string {
	return "SET @binlogviz_next_mode = " + sqlemit.DroppedModeExpr(drops) + ";\n" + statement
}

func assignColumns(columns, values []string, gen map[string]struct{}) ([]string, []string) {
	return sqlemit.AssignColumns(columns, values, gen)
}

func isGenerated(gen map[string]struct{}, name string) bool {
	return sqlemit.IsGenerated(gen, name)
}

func generatedRowError(row model.FlashRow) error {
	return &generatedRowRefusal{schema: row.Schema, table: row.Table}
}

func undoWhere(row model.FlashRow, head string, image []string, cond string) (string, error) {
	return undoWhereImage(row, head, image, cond, false)
}

// insertedImageMatch is every column the INSERT writes. A no-PK guard undo
// uses it so a re-run matches the row it already put back.
func insertedImageMatch(cols, vals []string) string {
	parts := make([]string, 0, len(cols))
	for i := range cols {
		if i >= len(vals) {
			break
		}
		parts = append(parts, quoteIdent(cols[i])+" <=> "+vals[i])
	}
	return strings.Join(parts, " AND ")
}

// keyMatch is the primary-key match for image.
func keyMatch(row model.FlashRow, image []string) string {
	return sqlemit.KeyMatch(row, image)
}

// undoWhereImage matches the primary key, or with allColumns (the --conflict
// guard) every logged column of image, so a row changed later matches nothing.
func undoWhereImage(row model.FlashRow, head string, image []string, cond string, allColumns bool) (string, error) {
	statement, ok := sqlemit.WhereImage(row, head, image, cond, allColumns)
	if !ok {
		return "", generatedRowError(row)
	}
	return statement, nil
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
	return sqlemit.QuoteColumnList(columns)
}

func quoteIdent(name string) string {
	return sqlemit.QuoteIdent(name)
}

func omittedGeneratedCols(groups []flashGroup) []guard.Col {
	var out []guard.Col
	seen := map[string]struct{}{}
	for _, group := range groups {
		out = appendGuardCols(out, seen, group)
	}
	return out
}

// appendGuardCols adds the generated columns of one group not yet in seen.
func appendGuardCols(out []guard.Col, seen map[string]struct{}, group flashGroup) []guard.Col {
	for i, row := range group.rows {
		if i >= len(group.binds) {
			continue
		}
		for _, col := range group.binds[i].cols {
			if !col.Generated {
				continue
			}
			key := strings.ToLower(row.Schema) + "\x00" + strings.ToLower(row.Table) + "\x00" + strings.ToLower(col.Name)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, guard.Col{Schema: row.Schema, Table: row.Table, Column: col.Name})
		}
	}
	return out
}
