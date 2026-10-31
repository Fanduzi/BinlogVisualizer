// Package redo renders staged records into the forward script.
// input: staged records in stream order, merged column facts, the shared literal emitter, and the open-group state.
// output: USE/BEGIN/DML/DDL/XA/boundary lines in source order, then COMMIT trailers for groups still open at the end.
// pos: the emit half of the planner. Statement assembly goes through internal/sqlemit and cell literals through internal/binlog so undo and redo share one formatter (ADR 0022).
// note: if this file changes, update this header and module README.md.
package redo

import (
	"fmt"
	"io"
	"strings"

	"binlogviz/internal/binlog"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/sqlemit"
)

// emitState tracks which transaction groups are open in the emitted script
// so BEGIN is emitted lazily before a group's first statement and every
// still-open group is committed at the end. w is nil during the counting
// pass, which only tracks state. split non-nil rotates the output across
// part files at statement-count boundaries (ADR 0025); begun marks the
// groups that logged BEGIN so a continuing file re-emits it. pending defers
// a group's BEGIN to its first emitted statement so a file never ends with
// an empty BEGIN;COMMIT stub, pendFlush holds groups whose deferred BEGIN a
// foreign record implicitly closed, and commentBuf holds comment lines
// until the next statement's boundary decision lands them in its file.
type emitState struct {
	w          io.Writer
	split      *fileSplit
	open       map[string]bool
	closed     map[string]bool
	xa         map[string]bool
	gtidByKey  map[string]string
	begun      map[string]bool
	pending    map[string]bool
	pendFlush  []string
	commentBuf []string
	order      []string
	useCtx     string
	useSet     bool
}

// track replays group bookkeeping for one record without emitting. The
// counting pass calls it for every record; the emit pass calls it for the
// records it handles.
func (st *emitState) track(rec *stagedRecord) {
	if rec.key == "" {
		return
	}
	st.closeForeign(rec.key)
	switch rec.kind {
	case recGTID:
		// A GTID event is the group's head comment; it does not open the
		// group in the output.
		if rec.gtid != "" {
			st.gtidByKey[rec.key] = rec.gtid
		}
		return
	case recClose:
		st.closed[rec.key] = true
		delete(st.open, rec.key)
		return
	case recStmt:
		// A QUERY statement is implicit-commit work (DDL, ADMIN,
		// unclassified): it is never wrapped in BEGIN/COMMIT in the source,
		// so it opens nothing in the output. An XA verb only marks the
		// group as XA-managed.
		if rec.xaVerb {
			st.xa[rec.key] = true
		}
		return
	case recXAPair:
		st.xa[rec.key] = true
		return
	}
	// recBegin, recComment and recRows are content of a real transaction
	// group: they open it for the still-open check. Only recBegin emits
	// BEGIN — a group that never logged BEGIN emits its rows bare.
	st.openGroup(rec.key)
}

// closeForeign closes every group still open when a different group's
// record arrives: the binlog is single-stream, so its work was implicitly
// committed when this group started — no trailer, no cut warning. A group
// whose BEGIN is still deferred gets it flushed by the next drainPend so
// the BEGIN still precedes the record that closed it (split mode).
func (st *emitState) closeForeign(key string) {
	if key == "" {
		return
	}
	for k := range st.open {
		if k == key {
			continue
		}
		if st.pending[k] {
			st.pendFlush = append(st.pendFlush, k)
			delete(st.pending, k)
		}
		delete(st.open, k)
		st.closed[k] = true
	}
}

// openGroup marks a group open so a later close or the end-of-input trailer
// matches it. A group already closed stays closed (its later close record
// is skipped), and an XA-managed group never enters the open set at all.
func (st *emitState) openGroup(key string) {
	if st.open[key] || st.closed[key] || st.xa[key] {
		return
	}
	st.open[key] = true
	st.order = append(st.order, key)
}

// stillOpen lists the groups open at the end of the input, labeled by GTID
// when one was seen. Order is first-open order.
func (st *emitState) stillOpen() []string {
	var out []string
	for _, key := range st.order {
		if !st.open[key] {
			continue
		}
		label := st.gtidByKey[key]
		if label == "" {
			label = "GTID unavailable"
		}
		out = append(out, label)
	}
	return out
}

func writeScriptHead(w io.Writer) error {
	for _, line := range []string{
		"SET SESSION sql_mode = REPLACE(REPLACE(@@SESSION.sql_mode, 'PAD_CHAR_TO_FULL_LENGTH',''), 'NO_BACKSLASH_ESCAPES','');",
		"SET NAMES utf8mb4;",
		"SET time_zone = '+00:00';",
	} {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	return nil
}

// commentLines renders the text a recGTID or recComment record emits,
// extracted so split mode can buffer the lines for the boundary decision.
func commentLines(rec *stagedRecord) []string {
	var lines []string
	switch rec.kind {
	case recGTID:
		if rec.gtid != "" {
			lines = append(lines, "-- gtid: "+rec.gtid)
		}
		if rec.path != "" {
			lines = append(lines, fmt.Sprintf("-- binlog: %s:%d", rec.path, rec.pos))
		}
	case recComment:
		for _, line := range strings.Split(rec.text, "\n") {
			lines = append(lines, "-- "+line)
		}
	}
	return lines
}

// emitRecord renders one staged record. Group bookkeeping matches track.
func (a *Planner) emitRecord(st *emitState, rec *stagedRecord) error {
	switch rec.kind {
	case recGTID, recComment:
		st.track(rec)
		for _, line := range commentLines(rec) {
			if err := st.line(line); err != nil {
				return err
			}
		}
		return nil
	case recBegin:
		// The source logged BEGIN here, so it is emitted unconditionally —
		// even for a group every statement of which the selection dropped.
		// Split mode marks deferred groups in emitAll and never reaches this
		// for a keyed BEGIN.
		st.track(rec)
		if st.begun != nil && rec.key != "" {
			st.begun[rec.key] = true
		}
		return st.statement("BEGIN")
	case recStmt:
		st.track(rec)
		if err := st.useSchema(rec.schema); err != nil {
			return err
		}
		return st.statement(rec.text)
	case recXAPair:
		st.track(rec)
		if rec.text == "" {
			return st.line("-- XA PREPARE marker (xid unavailable)")
		}
		if err := st.statement("XA END " + rec.text); err != nil {
			return err
		}
		return st.statement("XA PREPARE " + rec.text)
	case recClose:
		return a.emitClose(st, rec)
	case recRows:
		return a.emitRows(st, rec)
	}
	return nil
}

func (a *Planner) emitClose(st *emitState, rec *stagedRecord) error {
	if rec.key != "" {
		// An XA group closes through its own COMMIT/ROLLBACK verbs; a stray
		// XID adds nothing. A group already implicitly committed by a later
		// BEGIN's close-out gets no second COMMIT.
		if st.xa[rec.key] || (st.closed[rec.key] && !st.open[rec.key]) {
			st.track(rec)
			return nil
		}
	}
	st.track(rec)
	return st.statement(rec.text)
}

// useSchema emits a USE statement when a verbatim statement's default
// database differs from the context last emitted, so unqualified text
// lands in the schema it ran under (ADR 0022).
func (st *emitState) useSchema(schema string) error {
	if schema == "" {
		return nil
	}
	if st.useSet && st.useCtx == schema {
		return nil
	}
	st.useSet = true
	st.useCtx = schema
	return st.statement("USE " + sqlemit.QuoteIdent(schema))
}

func (st *emitState) statement(sql string) error {
	return st.line(sql + ";")
}

func (st *emitState) line(sql string) error {
	w := st.w
	if st.split != nil && st.split.w != nil {
		w = st.split.w
	}
	if w == nil {
		return nil
	}
	_, err := fmt.Fprintln(w, sql)
	return err
}

// emitAll drives the staged record stream through emitRecord, rotating the
// output file at statement-count boundaries when st.split is set (ADR 0025).
// Split mode buffers comment records until the next statement's boundary
// decision and defers BEGIN to a group's first emitted statement; the order
// below mirrors PrepareSplit's counting pass exactly — implicit close-out
// and its BEGIN flushes precede the rotation decision, the group's own
// deferred BEGIN lands wherever the record lands.
func (a *Planner) emitAll(st *emitState) error {
	return a.spill.forward(func(rec *stagedRecord) error {
		a.settleBinds(rec)
		if st.split != nil {
			switch {
			case rec.kind == recGTID || rec.kind == recComment:
				// Buffer comments until the next statement's boundary
				// decision so they land in the same file as what they
				// describe.
				st.track(rec)
				st.commentBuf = append(st.commentBuf, commentLines(rec)...)
				return nil
			case rec.kind == recBegin && rec.key != "":
				// A keyed BEGIN is deferred to the group's first emitted
				// statement; it emits nothing and counts nothing here —
				// flushPending counts it where it lands. It must not flush
				// the comment buffer, which belongs with that statement.
				st.closeForeign(rec.key)
				if err := st.drainPend(); err != nil {
					return err
				}
				st.track(rec)
				if st.begun != nil {
					st.begun[rec.key] = true
				}
				st.pending[rec.key] = true
				return nil
			}
		}
		st.closeForeign(rec.key)
		if err := st.drainPend(); err != nil {
			return err
		}
		if err := st.rotateFor(rec); err != nil {
			return err
		}
		if err := st.flushComments(); err != nil {
			return err
		}
		if err := st.flushPending(rec.key); err != nil {
			return err
		}
		if err := a.emitRecord(st, rec); err != nil {
			return err
		}
		if st.split != nil {
			st.split.count += recStatementCount(rec)
		}
		return nil
	})
}

// drainPend emits BEGIN for groups implicitly closed while their BEGIN was
// deferred. It is a no-op outside split mode.
func (st *emitState) drainPend() error {
	for range st.pendFlush {
		if err := st.statement("BEGIN"); err != nil {
			return err
		}
		if st.split != nil {
			st.split.count++
		}
	}
	st.pendFlush = st.pendFlush[:0]
	return nil
}

// flushComments writes the buffered comment lines into the current file —
// called after the rotation decision so they land with their statement.
func (st *emitState) flushComments() error {
	for _, line := range st.commentBuf {
		if err := st.line(line); err != nil {
			return err
		}
	}
	st.commentBuf = st.commentBuf[:0]
	return nil
}

// flushPending emits the deferred BEGIN of key's group, if any — the group
// begins in whichever file holds its first emitted statement.
func (st *emitState) flushPending(key string) error {
	if !st.pending[key] {
		return nil
	}
	delete(st.pending, key)
	if err := st.statement("BEGIN"); err != nil {
		return err
	}
	if st.split != nil {
		st.split.count++
	}
	return nil
}

// recStatementCount is the Prepare counting basis (countEmissions): row
// records count their rows, statement records count one, comments zero.
func recStatementCount(rec *stagedRecord) int64 {
	switch rec.kind {
	case recRows:
		return int64(len(rec.rows))
	case recStmt, recXAPair, recBegin, recClose:
		return 1
	}
	return 0
}

// splitNeedsRotate reports whether rec belongs in the next file. A group
// close always lands with its last statement (a file may exceed max by one
// COMMIT), and a record bigger than max stays whole.
func (st *emitState) splitNeedsRotate(max, count int64, rec *stagedRecord) bool {
	n := recStatementCount(rec)
	if n == 0 || count == 0 || count+n <= max {
		return false
	}
	return rec.kind != recClose || !st.open[rec.key]
}

// rotateFor closes the file boundary decision for one record — after the
// implicit close-out of other groups, which may otherwise look open. A
// deferred BEGIN belonging to rec's group lands in the same file rec does,
// so it counts toward the decision.
func (st *emitState) rotateFor(rec *stagedRecord) error {
	sp := st.split
	if sp == nil {
		return nil
	}
	st.closeForeign(rec.key)
	extra := int64(0)
	if st.pending[rec.key] {
		extra = 1
	}
	if !st.splitNeedsRotate(sp.max, sp.count+extra, rec) {
		return nil
	}
	return st.rotate(rec)
}

// rotate ends the current file at a boundary: still-open groups commit,
// the next file re-opens them with BEGIN so every part is independently
// appliable (ADR 0025). A group whose BEGIN is still deferred has emitted
// nothing in this file — it does not span, so it gets no COMMIT here and no
// continuation in the next file.
func (st *emitState) rotate(rec *stagedRecord) error {
	sp := st.split
	for _, key := range st.order {
		if st.open[key] && !st.pending[key] {
			if err := st.statement("COMMIT"); err != nil {
				return err
			}
		}
	}
	if err := sp.seal(); err != nil {
		return err
	}
	sp.count = 0
	return sp.open(st)
}

// emitTrailers commits every group still open at the end of the input, in
// first-open order. A group whose BEGIN is still deferred emits it first —
// the mirror of the source's BEGIN;COMMIT pair. The matching warnings were
// built by countEmissions.
func (st *emitState) emitTrailers() error {
	for _, key := range st.order {
		if !st.open[key] {
			continue
		}
		if st.pending[key] {
			delete(st.pending, key)
			if err := st.statement("BEGIN"); err != nil {
				return err
			}
		}
		if err := st.statement("COMMIT"); err != nil {
			return err
		}
	}
	return nil
}

// emitRows renders one row event's DML. Every cell literal goes through the
// shared emitter; names and the facts MINIMAL lacks come from the settled
// bind (ADR 0022).
func (a *Planner) emitRows(st *emitState, rec *stagedRecord) error {
	st.track(rec)
	for i := range rec.rows {
		row := rec.rows[i]
		if row.ProblemKind != "" {
			return rowProblemError(row, a.refusalTable(row.Schema, row.Table), "")
		}
		if err := a.emitRow(st, rec.rows[i], rec.binds[i]); err != nil {
			return err
		}
	}
	return nil
}

// rowFacts resolves the names and merged column descriptors for one row.
// FULL metadata supplies both; MINIMAL fills them from the resolved
// definition. The comparable-fact check already ran at accept time, so the
// merge here cannot hide a contradiction.
func (a *Planner) rowFacts(row model.RedoRow, bind redoBind, flavor string) ([]string, []model.FlashCol, map[string]struct{}, error) {
	metas := make([]model.FlashCol, len(row.Cols))
	copy(metas, row.Cols)
	if !positionalNames(row) {
		return row.Columns, metas, bind.gen, nil
	}
	if bind.cols == nil {
		return nil, nil, nil, namesError(row, a.refusalTable(row.Schema, row.Table), flavor)
	}
	columns := make([]string, len(bind.cols))
	for i, col := range bind.cols {
		columns[i] = col.Name
	}
	if len(metas) < len(columns) {
		grown := make([]model.FlashCol, len(columns))
		copy(grown, metas)
		metas = grown
	}
	for i := range columns {
		if i < len(bind.cols) && i < len(metas) {
			mysqlexpr.FillFlashCol(bind.cols[i], &metas[i])
		}
	}
	return columns, metas, bind.gen, nil
}

func (a *Planner) emitRow(st *emitState, row model.RedoRow, bind redoBind) error {
	columns, metas, gen, err := a.rowFacts(row, bind, "")
	if err != nil {
		return err
	}
	before, err := rowLiterals(row, metas, row.Before, row.BeforeDocs)
	if err != nil {
		return err
	}
	after, err := rowLiterals(row, metas, row.After, row.AfterDocs)
	if err != nil {
		return err
	}
	flash := model.FlashRow{
		Schema:    row.Schema,
		Table:     row.Table,
		Op:        row.Op,
		Columns:   columns,
		Before:    before,
		After:     after,
		PK:        row.PK,
		NoPK:      row.NoPK,
		NonStrict: row.NonStrict,
		ZeroDate:  row.ZeroDate,
	}
	table := mysqlexpr.SQLTableName(row.Schema, row.Table)
	var statement string
	var ok bool
	switch row.Op {
	case "INSERT":
		cols, vals := sqlemit.AssignColumns(columns, after, gen)
		if len(cols) == 0 {
			return generatedRowError(row)
		}
		statement = "INSERT INTO " + table + " (" + sqlemit.QuoteColumnList(cols) + ") VALUES (" + strings.Join(vals, ", ") + ");"
	case "UPDATE":
		cols, vals := sqlemit.AssignColumns(columns, after, gen)
		if len(cols) == 0 {
			return generatedRowError(row)
		}
		sets := make([]string, len(cols))
		for i := range cols {
			sets[i] = sqlemit.QuoteIdent(cols[i]) + " = " + vals[i]
		}
		statement, ok = sqlemit.WhereImage(flash, "UPDATE "+table+" SET "+strings.Join(sets, ", "), before, "", false)
		if !ok {
			return rowMatchError(row)
		}
	default: // DELETE
		statement, ok = sqlemit.WhereImage(flash, "DELETE FROM "+table, before, "", false)
		if !ok {
			return rowMatchError(row)
		}
	}
	if drops := relaxedModes(row, after, gen); len(drops) > 0 {
		return st.write(modeWrap(statement, drops))
	}
	return st.line(statement)
}

// rowLiterals renders one image's cells through the shared emitter. The
// binary JSON documents captured beside the image drive JSON columns.
func rowLiterals(row model.RedoRow, metas []model.FlashCol, image []any, docs [][]byte) ([]string, error) {
	lits := make([]string, len(image))
	for i, value := range image {
		if i >= len(metas) {
			return nil, refuse("capture", redoTable(row.Schema, row.Table), fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{
				"Table": redoTable(row.Schema, row.Table),
			})))
		}
		var doc []byte
		if i < len(docs) {
			doc = docs[i]
		}
		lit, problem := binlog.EmitLiteral(metas[i], value, doc)
		if problem != "" {
			return nil, literalProblemError(row, i, problem)
		}
		lits[i] = lit
	}
	return lits, nil
}

// relaxedModes lists the sql_mode flags that would reject a value this row
// stored legally. The redo half of sqlemit.RelaxedModes: the after-image is
// what INSERT and UPDATE write, so its generated-NULL check reads that one.
func relaxedModes(row model.RedoRow, after []string, gen map[string]struct{}) []string {
	var drops []string
	if row.NonStrict {
		drops = append(drops, "STRICT_ALL_TABLES", "STRICT_TRANS_TABLES")
	}
	if row.ZeroDate {
		drops = append(drops, "NO_ZERO_DATE", "NO_ZERO_IN_DATE")
	}
	if row.Op != "DELETE" && sqlemit.OmittedGeneratedNull(row.Columns, after, gen) {
		drops = append(drops, "ERROR_FOR_DIVISION_BY_ZERO")
	}
	if len(drops) > 0 {
		drops = append(drops, "TRADITIONAL")
	}
	return drops
}

// modeWrap wraps one statement in the save/relax/restore sql_mode triple
// undo uses, applied directly around the statement.
func modeWrap(statement string, drops []string) []string {
	return []string{
		"SET @binlogviz_redo_sql_mode = @@SESSION.sql_mode;",
		"SET SESSION sql_mode = " + sqlemit.DroppedModeExpr(drops) + ";",
		statement,
		"SET SESSION sql_mode = @binlogviz_redo_sql_mode;",
	}
}

func (st *emitState) write(lines []string) error {
	for _, line := range lines {
		if err := st.line(line); err != nil {
			return err
		}
	}
	return nil
}

// generatedRowError refuses a row whose writable columns are all generated:
// nothing remains to INSERT or SET.
func generatedRowError(row model.RedoRow) error {
	return &Refusal{Code: "generated_row", Table: redoTable(row.Schema, row.Table), Err: fmt.Errorf("%s", i18n.Tf("error.redoGeneratedRow", map[string]any{
		"Table": redoTable(row.Schema, row.Table),
	}))}
}

// rowMatchError refuses an UPDATE or DELETE whose WHERE could match no
// column (an all-generated or empty before-image).
func rowMatchError(row model.RedoRow) error {
	return refuse("row_image", redoTable(row.Schema, row.Table), fmt.Errorf("%s", i18n.Tf("error.redoRowMatch", map[string]any{
		"Table": redoTable(row.Schema, row.Table),
	})))
}
