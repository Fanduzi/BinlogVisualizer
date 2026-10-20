// Package flashback plans undo SQL: selection, refusals, the spill, conflict
// hashes, generated-column review, and the streaming writer (ADR 0012 step 4).
// input: normalized events from the analyzer, schema SQL, and the conflict policy.
// output: a script streamed from the spill, or one refusal and no script; --plan JSON at plan_version 2.
// pos: the planner. Analyzer holds one *Planner and does not keep flashback fields.
// note: if this file changes, update this header and README.md.
package flashback

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"binlogviz/internal/clierr"
	"binlogviz/internal/guard"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/redact"
	"binlogviz/internal/schemacatalog"
)

// Options is the flashback half of an analyzer run.
type Options struct {
	SchemaSQL     string
	SchemaFileDB  string
	Conflict      string
	Plan          bool
	ConflictLimit int
}

// Planner owns every flashback field that used to sit on *analyzer.Analyzer.
type Planner struct {
	opts Options

	flashCur              *flashGroup
	flashSpill            *flashSpill
	flashKeys             map[uint64]uint64
	flashConflicts        int
	flashPending          map[conflictRef]pendingConflict
	flashPendingSeq       int64
	flashConflictsSettled bool
	flashConflictList     []FlashbackConflict
	flashGroupID          int64
	flashSeq              int64
	flashClearSeq         map[string]int64
	flashPendingNames     map[string][]string
	flashChecked          bool
	flashErr              error
	flashCat              *schemacatalog.Catalog
	flashUnknown          []string
	flashSchemaWarn       []model.FlashbackWarning
	flashGenNotes         []string
	flashReviewed         bool
	flashReviewErr        error
	flashSplit            map[string]*flashSplit
	flashSplitOrder       []string
	flashSplitDone        []string

	// conflictTxn and conflictGTID are the open transaction while Observe
	// runs, before that event is applied to the transaction builder.
	conflictTxn  string
	conflictGTID string

	// spillLimit is BINLOGVIZ_SPILL_BYTES, or a test ceiling. 0 is unlimited.
	// memLimit is BINLOGVIZ_MEMORY_BYTES, or a test ceiling. 0 consults
	// GOMEMLIMIT when that is finite (ADR 0013).
	spillLimit int64
	memLimit   int64
}

// New starts a planner. Schema SQL is loaded now, matching the analyzer reset.
func New(opts Options) *Planner {
	spill, spillErr := spillLimitFromEnv()
	mem, memErr := memoryLimitFromEnv()
	p := &Planner{opts: opts, flashCat: schemacatalog.New(opts.SchemaFileDB), spillLimit: spill, memLimit: mem}
	if spillErr != nil {
		p.flashErr = spillErr
	} else if memErr != nil {
		p.flashErr = memErr
	}
	if strings.TrimSpace(opts.SchemaSQL) != "" {
		p.flashCat.LoadFile(opts.SchemaSQL)
	}
	return p
}

// Close removes the spill. The analyzer calls it on reset.
func (a *Planner) Close() { a.closeFlashSpill() }

// HeldRows is how many rows of the open transaction are still in memory.
func (a *Planner) HeldRows() int {
	if a == nil || a.flashCur == nil {
		return 0
	}
	return len(a.flashCur.rows)
}

// Observe records schema changes and conflict candidates for one scanned
// event, selected or not. txnKey and gtid are the transaction that is open
// before this event is applied.
func (a *Planner) Observe(ev model.NormalizedEvent, txnKey, gtid string) {
	if a == nil {
		return
	}
	a.conflictTxn, a.conflictGTID = txnKey, gtid
	a.checkFlashConflict(ev)
	dropped := a.flashCat.ObserveTable(ev.Schema, ev.Table)
	if ev.EventType == "DDL" {
		dropped = append(dropped, a.flashCat.ObserveDDL(ev.Schema, ev.QuerySQL)...)
	}
	for _, table := range dropped {
		a.clearPendingUse(table)
	}
}

// Note records one retained event.
func (a *Planner) Note(ev model.NormalizedEvent) { a.noteFlashback(ev) }

// NoteSkipped records that the open transaction dropped a row event.
func (a *Planner) NoteSkipped(txnKey, gtid string) {
	if a == nil {
		return
	}
	a.noteFlashSplit(txnKey, gtid, false)
}

// WriteTo renders the script. The count is the undo row count.
func (a *Planner) WriteTo(w io.Writer) (int64, error) {
	n, err := a.WriteFlashback(w)
	return int64(n), err
}

// flashBind is the table definition in effect for one undo row.
// deferCheck waits until every schema of an unqualified table has been seen.
type flashBind struct {
	gen         map[string]struct{}
	cols        []mysqlexpr.SchemaCol
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
	seqs  []int64
	// id names the transaction group; chunk numbers its parts. A large
	// transaction is spilled in parts of ChunkRows rows (#258).
	id    int64
	chunk int
}

// ChunkRows bounds the rows of one transaction held in memory.
const ChunkRows = 2048

type flashSplit struct {
	gtid    string
	kept    bool
	skipped bool
}

func (a *Planner) noteFlashback(ev model.NormalizedEvent) {
	if a == nil || a.flashErr != nil {
		return
	}
	if ev.EventType == "DDL" {
		label := a.refusalTable(ev.Schema, ev.Table)
		table := ""
		if ev.Table != "" {
			table = label
		}
		a.flashErr = refuse("ddl_in_range", table, flashDDLError(ev, label))
		return
	}
	if ev.EventType != "ROWS" {
		return
	}
	if len(ev.FlashRows) == 0 {
		table := a.refusalTable(ev.Schema, ev.Table)
		a.flashErr = refuse("row_capture", table, fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{
			"Table": table,
		})))
		return
	}
	for _, row := range ev.FlashRows {
		if row.ProblemKind != "" {
			a.flashErr = flashRowError(row, a.refusalTable(row.Schema, row.Table))
			return
		}
	}
	res := a.flashCat.Resolve(ev.Schema, ev.Table)
	label := refusalLabel(res, ev.Schema, ev.Table)
	bind := flashBind{}
	switch res.Kind {
	case schemacatalog.Refused:
		a.flashErr = refuse("generated_unknown", label, fmt.Errorf("%s", i18n.Tf("error.flashbackGenerated", map[string]any{
			"Table": label,
		})))
		return
	case schemacatalog.Missing, schemacatalog.Ambiguous:
		a.noteFlashUnknown(flashTable(ev.Schema, ev.Table))
	default: // schemacatalog.Exact, schemacatalog.Fold
		if !res.CheckRows {
			bind.gen = mysqlexpr.GeneratedNameSet(res.Cols)
			break
		}
		bind.cols = res.Cols
		bind.fromPending = res.Deferred
		bind.deferCheck = res.Deferred
		if res.Deferred {
			// The schema is not final until every table map for this name is seen.
		} else if err := a.acceptSchemaRows(label, ev.FlashRows, res.Cols); err != nil {
			a.flashErr = err
			return
		} else {
			bind.gen = mysqlexpr.GeneratedNameSet(res.Cols)
		}
	}
	a.appendFlashRows(ev, ev.FlashRows, bind)
	a.noteFlashbackKept(ev)
}

func (a *Planner) acceptSchemaRows(label string, rows []model.FlashRow, cols []mysqlexpr.SchemaCol) error {
	for _, row := range rows {
		if err := a.acceptSchema(label, row, cols); err != nil {
			return err
		}
	}
	return nil
}

func (a *Planner) acceptSchema(label string, row model.FlashRow, cols []mysqlexpr.SchemaCol) error {
	warns, err := mysqlexpr.ValidateSchemaRow(label, row, cols)
	if err != nil {
		return refuse("schema_mismatch", label, err)
	}
	a.noteSchemaWarnings(warns)
	return nil
}

// appendWarningOnce appends w unless an equal warning is already in list.
func appendWarningOnce(list []model.FlashbackWarning, w model.FlashbackWarning) []model.FlashbackWarning {
	for _, have := range list {
		if have == w {
			return list
		}
	}
	return append(list, w)
}

func (a *Planner) noteSchemaWarnings(warns []model.FlashbackWarning) {
	for _, warn := range warns {
		a.flashSchemaWarn = appendWarningOnce(a.flashSchemaWarn, warn)
	}
}

// clearPendingUse drops the deferred definition from every row recorded so
// far for table. Spilled rows cannot be edited, so the cut-off is a row
// sequence number that settleBinds applies when a group is read back.
func (a *Planner) clearPendingUse(table string) {
	key := strings.ToLower(table)
	names := a.flashPendingNames[key]
	if len(names) == 0 {
		return
	}
	if a.flashClearSeq == nil {
		a.flashClearSeq = map[string]int64{}
	}
	a.flashClearSeq[key] = a.flashSeq
	for _, name := range names {
		a.noteFlashUnknown(name)
	}
	delete(a.flashPendingNames, key)
}

// settleBinds applies table clears and finished deferred checks to a group
// read from the spill or held in memory. It is idempotent.
func (a *Planner) settleBinds(group *flashGroup) {
	for i := range group.binds {
		bind := &group.binds[i]
		if bind.fromPending && i < len(group.seqs) && i < len(group.rows) {
			if cut, ok := a.flashClearSeq[strings.ToLower(group.rows[i].Table)]; ok && group.seqs[i] < cut {
				*bind = flashBind{}
				continue
			}
		}
		if bind.deferCheck && a.flashChecked {
			bind.gen = mysqlexpr.GeneratedNameSet(bind.cols)
			bind.deferCheck = false
		}
	}
}

// eachFlashGroup visits every kept group, in binlog order or last first.
// Resident memory is one group: the rest are read back from the spill.
func (a *Planner) eachFlashGroup(backward bool, fn func(*flashGroup) error) error {
	visit := func(g *flashGroup) error {
		if len(g.rows) == 0 {
			return nil
		}
		a.settleBinds(g)
		return fn(g)
	}
	if backward {
		if a.flashCur != nil {
			if err := visit(a.flashCur); err != nil {
				return err
			}
		}
		return a.flashSpill.backward(visit)
	}
	if err := a.flashSpill.forward(visit); err != nil {
		return err
	}
	if a.flashCur != nil {
		return visit(a.flashCur)
	}
	return nil
}

func (a *Planner) finishSchema() error {
	if !a.flashChecked {
		err := a.eachFlashGroup(false, func(group *flashGroup) error {
			for i := range group.binds {
				if !group.binds[i].deferCheck {
					continue
				}
				if err := a.acceptSchema(a.refusalTable(group.rows[i].Schema, group.rows[i].Table), group.rows[i], group.binds[i].cols); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		a.flashChecked = true
	}
	return a.reviewGenerated()
}

// appendFlashRows adds rows to the open group. A new transaction key spills
// the previous group, so only one transaction stays in memory (#258).
func (a *Planner) appendFlashRows(ev model.NormalizedEvent, rows []model.FlashRow, bind flashBind) {
	copied := append([]model.FlashRow(nil), rows...)
	binds := make([]flashBind, len(copied))
	seqs := make([]int64, len(copied))
	for i := range binds {
		binds[i] = bind
		if bind.gen != nil {
			binds[i].gen = mysqlexpr.CopyNames(bind.gen)
		}
		if bind.cols != nil {
			binds[i].cols = mysqlexpr.CloneSchemaCols(bind.cols)
		}
		seqs[i] = a.flashSeq
		a.flashSeq++
		if bind.fromPending {
			a.notePendingName(copied[i])
		}
	}
	a.noteSelectedKeys(ev.TxnKey, copied)
	key := ev.TxnKey
	if cur := a.flashCur; cur != nil && key != "" && cur.key == key {
		cur.rows = append(cur.rows, copied...)
		cur.binds = append(cur.binds, binds...)
		cur.seqs = append(cur.seqs, seqs...)
		if len(cur.rows) >= ChunkRows {
			next := &flashGroup{key: cur.key, gtid: cur.gtid, path: cur.path, pos: cur.pos, id: cur.id, chunk: cur.chunk + 1}
			if err := a.spillCurrent(); err != nil {
				a.flashErr = err
				return
			}
			a.flashCur = next
		}
		return
	}
	if err := a.spillCurrent(); err != nil {
		a.flashErr = err
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
	a.flashGroupID++
	a.flashCur = &flashGroup{
		id:    a.flashGroupID,
		key:   key,
		gtid:  ev.TxnGTID,
		path:  path,
		pos:   pos,
		rows:  copied,
		binds: binds,
		seqs:  seqs,
	}
}

func (a *Planner) notePendingName(row model.FlashRow) {
	key := strings.ToLower(row.Table)
	name := flashTable(row.Schema, row.Table)
	for _, have := range a.flashPendingNames[key] {
		if have == name {
			return
		}
	}
	if a.flashPendingNames == nil {
		a.flashPendingNames = map[string][]string{}
	}
	a.flashPendingNames[key] = append(a.flashPendingNames[key], name)
}

func (a *Planner) spillCurrent() error {
	cur := a.flashCur
	if cur == nil {
		return nil
	}
	a.flashCur = nil
	if len(cur.rows) == 0 {
		return nil
	}
	if a.flashSpill == nil {
		spill, err := newFlashSpill()
		if err != nil {
			return clierr.Wrap(clierr.KindOutput, fmt.Errorf("flashback: cannot create temp file: %w", err))
		}
		a.flashSpill = spill
	}
	if err := a.flashSpill.append(cur); err != nil {
		return clierr.Wrap(clierr.KindOutput, fmt.Errorf("flashback: cannot write temp file: %w", err))
	}
	if err := a.overBudget(0); err != nil {
		return err
	}
	return a.checkMemory()
}

func (a *Planner) closeFlashSpill() {
	a.flashSpill.close()
	a.flashSpill = nil
	a.flashCur = nil
}

func (a *Planner) noteFlashbackKept(ev model.NormalizedEvent) {
	a.noteFlashSplit(ev.TxnKey, ev.TxnGTID, true)
}

func (a *Planner) noteFlashSplit(key, gtid string, kept bool) {
	if a == nil || key == "" {
		return
	}
	if a.flashSplit == nil {
		a.flashSplit = map[string]*flashSplit{}
	}
	split, ok := a.flashSplit[key]
	if !ok {
		a.evictFlashSplits()
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

// flashSplitWindow is how many recent transactions keep their kept/skipped
// marks. A transaction's marks all arrive while it is open or just closed
// (GTID buffering delays "kept" to the close), so older entries are final.
// Final split transactions move to flashSplitDone; the rest are dropped, so
// this bookkeeping does not grow with the binlog (#258).
const flashSplitWindow = 64

func (a *Planner) evictFlashSplits() {
	for len(a.flashSplitOrder) >= flashSplitWindow {
		key := a.flashSplitOrder[0]
		a.flashSplitOrder = a.flashSplitOrder[1:]
		if split := a.flashSplit[key]; split != nil && split.kept && split.skipped {
			a.flashSplitDone = append(a.flashSplitDone, split.gtid)
		}
		delete(a.flashSplit, key)
	}
}

func (a *Planner) noteFlashUnknown(table string) {
	for _, have := range a.flashUnknown {
		if have == table {
			return
		}
	}
	a.flashUnknown = append(a.flashUnknown, table)
}

// FlashbackWarnings is FlashbackWarningList as the stderr lines flashback prints.
func (a *Planner) Warnings() []string {
	list := a.WarningList()
	if list == nil {
		return nil
	}
	out := make([]string, len(list))
	for i, w := range list {
		out[i] = w.Line()
	}
	return out
}

// FlashbackWarningList reports schema-file notes, tables whose definitions
// were not seen, then transactions a table or --dml filter undid only in part.
// Empty when flashback has nothing to print.
func (a *Planner) WarningList() []model.FlashbackWarning {
	if a == nil {
		return nil
	}
	var out []model.FlashbackWarning
	out = append(out, a.flashCat.Warnings()...)
	out = append(out, a.flashSchemaWarn...)
	for _, table := range a.flashUnknown {
		out = append(out, model.NewFlashWarning(model.WarnTableDefinitionMissing, "warning.flashbackGenerated", map[string]any{"Table": table}))
	}
	gtids := append([]string(nil), a.flashSplitDone...)
	for _, key := range a.flashSplitOrder {
		split := a.flashSplit[key]
		if split == nil || !split.kept || !split.skipped {
			continue
		}
		gtids = append(gtids, split.gtid)
	}
	for _, gtid := range gtids {
		if gtid == "" {
			gtid = "GTID unavailable"
		}
		out = append(out, model.NewFlashWarning(model.WarnPartialTransaction, "warning.flashbackSplit", map[string]any{"GTID": gtid}))
	}
	return out
}

// FlashbackSQL returns undo SQL for the selected row changes.
// A non-nil error means the script must not be applied; the string is empty.
// It holds the whole script in memory; the CLI uses WriteFlashback.
func (a *Planner) SQL() (string, error) {
	var b strings.Builder
	if _, err := a.WriteFlashback(&b); err != nil {
		return "", err
	}
	return b.String(), nil
}

// PrepareFlashback runs every check that can refuse the script and returns
// the number of undo rows. Zero rows means there is nothing to print.
// FlashbackWarnings is complete after it returns.
func (a *Planner) Prepare() (int, error) {
	rows, err := a.prepareChecks()
	if err != nil {
		return 0, err
	}
	if err := a.conflictError(); err != nil {
		return 0, err
	}
	return rows, nil
}

// prepareChecks is PrepareFlashback without the conflict refusal.
func (a *Planner) prepareChecks() (int, error) {
	if a == nil {
		return 0, refuse("row_capture", "selected range", fmt.Errorf("%s", i18n.T("error.flashbackCapture", map[string]any{"Table": "selected range"})))
	}
	if a.flashErr != nil {
		return 0, a.flashErr
	}
	if err := a.finishSchema(); err != nil {
		return 0, err
	}
	rows := 0
	err := a.eachFlashGroup(false, func(g *flashGroup) error {
		rows += len(g.rows)
		return nil
	})
	return rows, err
}

// WriteFlashback streams the undo script to w and returns the undo row count.
// Every refusal happens before the first byte is written: the script is
// rendered once into the guard hash, which also proves every statement
// renders, and then once to w. Memory does not grow with the binlog (#258).
func (a *Planner) WriteFlashback(w io.Writer) (int, error) {
	rows, err := a.Prepare()
	if err != nil || rows == 0 {
		return 0, err
	}
	var cols []guard.Col
	seen := map[string]struct{}{}
	if err := a.eachFlashGroup(false, func(g *flashGroup) error {
		cols = appendGuardCols(cols, seen, *g)
		return nil
	}); err != nil {
		return 0, err
	}
	err = writeFlashbackScript(w, func(fn func(*flashGroup) error) error {
		return a.eachFlashGroup(true, fn)
	}, a.flashGenNotes, cols, a.postImageGuard())
	var rowErr *generatedRowRefusal
	if errors.As(err, &rowErr) {
		return 0, rowErr.named(a.refusalTable(rowErr.schema, rowErr.table))
	}
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// joinHint appends the once-per-run hand-restore note to a refusal line,
// keeping it one line ("Error: ..."). zh text needs no separating space.
func joinHint(line, hint string) string {
	if strings.HasSuffix(line, "。") {
		return line + hint
	}
	return line + " " + hint
}

func generatedSig(cols []mysqlexpr.SchemaCol) string {
	var b strings.Builder
	found := false
	for _, col := range cols {
		if !col.Generated {
			continue
		}
		found = true
		b.WriteString(strings.ToLower(col.Name))
		b.WriteByte(0)
		b.WriteString(col.Expr)
		b.WriteByte(0)
	}
	if !found {
		return ""
	}
	return b.String()
}

func flashRowError(row model.FlashRow, table string) error {
	switch row.ProblemKind {
	case model.FlashProblemNames:
		return refuse("row_metadata", table, fmt.Errorf("%s", i18n.Tf("error.flashbackNames", map[string]any{"Table": table})))
	case model.FlashProblemImage:
		return refuse("row_image", table, fmt.Errorf("%s", i18n.Tf("error.flashbackImage", map[string]any{"Table": table})))
	case model.FlashProblemType:
		column := row.ProblemColumn
		if column == "" {
			column = "?"
		}
		// The column follows the table name, so the plain binlog name is kept.
		named := flashTable(row.Schema, row.Table)
		return refuse("column_type", named, fmt.Errorf("%s", i18n.Tf("error.flashbackType", map[string]any{
			"Table":  named,
			"Column": column,
			"Type":   row.ProblemType,
		})))
	default:
		return refuse("row_capture", table, fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": table})))
	}
}

func flashDDLError(ev model.NormalizedEvent, table string) error {
	statement := strings.TrimSpace(redact.Statement(ev.QuerySQL))
	if statement == "" {
		statement = "DDL"
	}
	statement = mysqlexpr.OneLine(statement, 120)
	return fmt.Errorf("%s", i18n.Tf("error.flashbackDDL", map[string]any{
		"Table":     table,
		"Statement": statement,
	}))
}

func (e *generatedRowRefusal) shownTable() string {
	if e == nil {
		return ""
	}
	if e.label != "" {
		return e.label
	}
	return flashTable(e.schema, e.table)
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
// column, a DELETE undo inserts only when the key is still absent, and an
// UPDATE undo that moves the key back runs only when no other row holds it.
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
		if postImage && !row.NoPK && len(row.PK) > 0 {
			conds = append(conds, "NOT EXISTS (SELECT 1 FROM "+table+" WHERE "+keyMatch(row, row.Before)+")")
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
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + " AND " + b
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

// sqlModeWrap names the sql_mode for one statement. The client computes it
// from the real session mode. Mode 0 applies @binlogviz_next_mode for that
// CALL only: a routine ignores a session sql_mode set before CALL, and MySQL
// restores the session mode when the routine returns.
func sqlModeWrap(statement string, drops []string) string {
	expr := "CONCAT(',', @@SESSION.sql_mode, ',')"
	for _, flag := range drops {
		expr = "REPLACE(" + expr + ", '," + flag + ",', ',')"
	}
	return "SET @binlogviz_next_mode = TRIM(BOTH ',' FROM " + expr + ");\n" + statement
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
	return &generatedRowRefusal{schema: row.Schema, table: row.Table}
}

// generatedRowRefusal is a row with no base column to write. It names the
// binlog table; FlashbackSQL re-renders it with the catalog label.
type generatedRowRefusal struct {
	schema, table string
	label         string
}

func (e *generatedRowRefusal) Error() string {
	label := e.label
	if label == "" {
		label = flashTable(e.schema, e.table)
	}
	return i18n.Tf("error.flashbackGeneratedRow", map[string]any{"Table": label})
}

func (e *generatedRowRefusal) named(label string) error {
	copied := *e
	copied.label = label
	return &copied
}

// refusalTable names schema.table in a refusal, from its current catalog binding.
func (a *Planner) refusalTable(schema, table string) string {
	return refusalLabel(a.flashCat.Peek(schema, table), schema, table)
}

// refusalLabel is the table name every refusal prints. A table bound by a
// letter-case fold also names the schema-file table, in every language and on
// every refusal line (#225).
func refusalLabel(res schemacatalog.Resolution, schema, table string) string {
	name := flashTable(schema, table)
	if res.FileTable == "" {
		return name
	}
	return i18n.Tf("error.flashbackSchemaFoldLabel", map[string]any{"Binlog": name, "File": res.FileTable})
}

func undoWhere(row model.FlashRow, head string, image []string, cond string) (string, error) {
	return undoWhereImage(row, head, image, cond, false)
}

// keyMatch is the primary-key match for image.
func keyMatch(row model.FlashRow, image []string) string {
	parts := make([]string, 0, len(row.PK))
	for _, index := range row.PK {
		if index >= 0 && index < len(row.Columns) && index < len(image) {
			parts = append(parts, quoteIdent(row.Columns[index])+" <=> "+image[index])
		}
	}
	return strings.Join(parts, " AND ")
}

// undoWhereImage matches the primary key, or with allColumns (the --conflict
// guard) every logged column of image, so a row changed later matches nothing.
func undoWhereImage(row model.FlashRow, head string, image []string, cond string, allColumns bool) (string, error) {
	pk := !row.NoPK && len(row.PK) > 0
	indexes := row.PK
	if !pk || allColumns {
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
