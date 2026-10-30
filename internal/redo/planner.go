// Package redo plans forward (redo) SQL: staging, schema binding, refusals,
// transaction-boundary mirroring, and the streaming writer (ADR 0022).
// input: normalized events from the analyzer, schema SQL, and the selection filters the analyzer already applies.
// output: a replayable script staged to disk until the stream completes cleanly, then written forward.
// pos: the planner. Analyzer holds one *Planner and does not keep redo fields. The undo-shaped spill, conflict, guard, and apply machinery of internal/flashback is deliberately not shared: redo is stream-shaped.
// note: if this file changes, update this header and module README.md.
package redo

import (
	"fmt"
	"io"
	"strings"

	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/schemacatalog"
)

// Options is the redo half of an analyzer run.
type Options struct {
	SchemaSQL    string
	SchemaFileDB string
	// Where is the compiled --where row predicate (ADR 0024), or nil. Kept
	// row events still run every check; only rows whose matched image is
	// provably selected are staged.
	Where *mysqlexpr.WhereExpr
}

// Planner owns the redo pipeline: the schema catalog, the staged records,
// the name-binding bookkeeping, and the refusal/warning state.
type Planner struct {
	opts Options

	cat   *schemacatalog.Catalog
	spill *redoSpill
	err   error

	seq          int64
	checked      bool
	clearSeq     map[string]int64
	pendingNames map[string][]string
	schemaWarn   []model.FlashbackWarning
	unknown      []string
	split        map[string]*redoSplit
	splitOrder   []string
	splitDone    []string
	cut          []string
	countsReady  bool
	rowCount     int64
	stmtCount    int64
	spillLimit   int64
	memLimit     int64
}

// redoSplit is the kept/skipped bookkeeping for one transaction group, the
// same shape undo uses to warn about partial selection.
type redoSplit struct {
	gtid    string
	kept    bool
	skipped bool
}

// New starts a planner. Schema SQL is loaded now, matching the analyzer reset.
func New(opts Options) *Planner {
	spillLimit, spillErr := spillLimitFromEnv()
	mem, memErr := memoryLimitFromEnv()
	p := &Planner{opts: opts, cat: schemacatalog.New(opts.SchemaFileDB), spillLimit: spillLimit, memLimit: mem}
	if spillErr != nil {
		p.err = spillErr
	} else if memErr != nil {
		p.err = memErr
	}
	if strings.TrimSpace(opts.SchemaSQL) != "" {
		p.cat.LoadFile(opts.SchemaSQL)
	}
	return p
}

// Close removes the staged records. The analyzer calls it on reset.
func (a *Planner) Close() {
	if a == nil {
		return
	}
	a.spill.close()
	a.spill = nil
}

// Observe records schema changes for one scanned event, selected or not.
// txnKey and gtid are the transaction open before this event is applied;
// redo needs only the catalog side of the seam — conflicts are undo's.
func (a *Planner) Observe(ev model.NormalizedEvent, txnKey, gtid string) {
	if a == nil {
		return
	}
	dropped := a.cat.ObserveTable(ev.Schema, ev.Table)
	if ev.EventType == "DDL" {
		session := ev.QuerySchema
		if session == "" {
			session = ev.Schema
		}
		dropped = append(dropped, a.cat.ObserveDDL(session, ev.QuerySQL)...)
	}
	for _, table := range dropped {
		a.clearPendingUse(table)
	}
}

// NoteSkipped records that the open transaction dropped a row event.
func (a *Planner) NoteSkipped(txnKey, gtid string) {
	if a == nil {
		return
	}
	a.noteSplit(txnKey, gtid, false)
}

// Note records one retained event in stream order.
func (a *Planner) Note(ev model.NormalizedEvent) { a.noteRedo(ev) }

func (a *Planner) noteRedo(ev model.NormalizedEvent) {
	if a == nil || a.err != nil {
		return
	}
	switch ev.EventType {
	case "GTID":
		a.stage(&stagedRecord{kind: recGTID, key: ev.TxnKey, gtid: ev.GTID, path: ev.BinlogPath, pos: ev.PositionStart})
	case "BEGIN":
		a.stage(&stagedRecord{kind: recBegin, key: ev.TxnKey, gtid: ev.TxnGTID, path: ev.TxnStartPath, pos: ev.TxnStartPos})
	case "XID", "COMMIT":
		a.stage(&stagedRecord{kind: recClose, key: ev.TxnKey, gtid: ev.TxnGTID, text: "COMMIT"})
	case "ROLLBACK":
		a.stage(&stagedRecord{kind: recClose, key: ev.TxnKey, gtid: ev.TxnGTID, text: "ROLLBACK"})
	case "ROWS_QUERY":
		a.stage(&stagedRecord{kind: recComment, key: ev.TxnKey, text: ev.QuerySQL})
	case "ROWS":
		a.noteRows(ev)
	case "XA_PREPARE":
		if ev.QuerySQL == "" {
			// The server-generated marker logged no QUERY text; the XA END
			// it implies is emitted before PREPARE (ADR 0022).
			a.stage(&stagedRecord{kind: recXAPair, key: ev.TxnKey, text: ev.XAXID})
			return
		}
		a.noteStatement(ev, true)
	case "XA_START", "XA_END", "XA_COMMIT", "XA_ROLLBACK":
		a.noteStatement(ev, true)
	case "DDL", "ADMIN":
		a.noteStatement(ev, false)
	case "UNCLASSIFIED_QUERY":
		if ev.QueryTruncated {
			a.err = truncatedQueryError(statementExcerpt(ev))
			return
		}
		a.noteStatement(ev, false)
	}
}

// noteStatement stages a verbatim statement. xaVerb statements carry XA
// protocol text that is never wrapped in BEGIN/COMMIT.
func (a *Planner) noteStatement(ev model.NormalizedEvent, xaVerb bool) {
	schema := ev.QuerySchema
	if schema == "" {
		schema = ev.Schema
	}
	a.stage(&stagedRecord{kind: recStmt, key: ev.TxnKey, schema: schema, text: ev.QuerySQL, xaVerb: xaVerb})
}

func statementExcerpt(ev model.NormalizedEvent) string {
	text := mysqlexpr.OneLine(strings.TrimSpace(ev.QuerySQL), 120)
	if text == "" {
		return "QUERY"
	}
	return text
}

// noteRows resolves the table binding for one row event and stages it.
// Refusals that can be decided now fire here; deferred bindings wait for
// finishSchema, exactly as undo defers them.
func (a *Planner) noteRows(ev model.NormalizedEvent) {
	if len(ev.RedoRows) == 0 {
		table := a.refusalTable(ev.Schema, ev.Table)
		a.err = refuse("capture", table, fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{
			"Table": table,
		})))
		return
	}
	for _, row := range ev.RedoRows {
		if row.ProblemKind != "" {
			a.err = rowProblemError(row, a.refusalTable(row.Schema, row.Table), ev.ServerFlavor)
			return
		}
	}
	res := a.cat.Resolve(ev.Schema, ev.Table)
	label := refusalLabel(res, ev.Schema, ev.Table)
	bind := redoBind{}
	switch res.Kind {
	case schemacatalog.Refused:
		a.err = refuse("generated_unknown", label, fmt.Errorf("%s", i18n.Tf("error.flashbackGenerated", map[string]any{
			"Table": label,
		})))
		return
	case schemacatalog.Missing, schemacatalog.Ambiguous:
		if positionalNames(ev.RedoRows[0]) {
			a.err = namesError(ev.RedoRows[0], label, ev.ServerFlavor)
			return
		}
		a.noteUnknown(redoTable(ev.Schema, ev.Table))
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
		} else if err := a.acceptSchemaRows(label, ev.RedoRows, res.Cols); err != nil {
			a.err = err
			return
		} else {
			bind.gen = mysqlexpr.GeneratedNameSet(res.Cols)
		}
	}
	rows, err := a.selectRedoRows(ev.RedoRows, bind, ev.ServerFlavor)
	if err != nil {
		a.err = err
		return
	}
	if len(rows) < len(ev.RedoRows) {
		a.noteSplit(ev.TxnKey, ev.TxnGTID, false)
	}
	if len(rows) > 0 {
		a.appendRows(ev, rows, bind)
		a.noteSplit(ev.TxnKey, ev.TxnGTID, true)
	}
}

// positionalNames reports whether the row's labels are @1..@N placeholders
// (binlog_row_metadata=MINIMAL): names and the type facts they drive must
// come from a resolved definition.
func positionalNames(row model.RedoRow) bool {
	return row.Names == model.RowNamesPositional
}

func (a *Planner) acceptSchemaRows(label string, rows []model.RedoRow, cols []mysqlexpr.SchemaCol) error {
	for _, row := range rows {
		if err := a.acceptSchema(label, row, cols); err != nil {
			return err
		}
	}
	return nil
}

// acceptSchema validates one row against a resolved definition. FULL rows
// compare names and facts; MINIMAL rows compare the count and the wire
// facts a definition can contradict (ADR 0022).
func (a *Planner) acceptSchema(label string, row model.RedoRow, cols []mysqlexpr.SchemaCol) error {
	if positionalNames(row) {
		return a.acceptPositionalSchema(label, row, cols)
	}
	warns, err := mysqlexpr.ValidateSchemaRow(label, flashRowForCheck(row), cols)
	if err != nil {
		return schemaError(label, err)
	}
	a.noteSchemaWarnings(warns)
	return nil
}

// flashRowForCheck projects the row facts schema validation compares
// (columns and per-column descriptors) into the undo row shape. Literal
// images are not part of that check.
func flashRowForCheck(row model.RedoRow) model.FlashRow {
	return model.FlashRow{
		Schema:  row.Schema,
		Table:   row.Table,
		Op:      row.Op,
		Columns: row.Columns,
		Cols:    row.Cols,
	}
}

// acceptPositionalSchema is the MINIMAL-metadata half of acceptSchema:
// positional labels cannot be name-compared, so the count must match and
// each comparable fact is checked before FillFlashCol merges it.
func (a *Planner) acceptPositionalSchema(label string, row model.RedoRow, cols []mysqlexpr.SchemaCol) error {
	if len(cols) != len(row.Columns) {
		return schemaError(label, fmt.Errorf("%s", i18n.Tf("error.redoSchemaCount", map[string]any{
			"Table": label,
			"File":  len(cols),
			"Row":   len(row.Columns),
		})))
	}
	for i := range cols {
		if i < len(row.Cols) {
			if diff := mysqlexpr.CheckSchemaCol(cols[i], row.Cols[i]); diff != "" {
				return schemaError(label, fmt.Errorf("%s", i18n.Tf("error.flashbackSchema", map[string]any{
					"Table": label,
					"Diff":  diff,
				})))
			}
		}
	}
	return nil
}

func (a *Planner) noteSchemaWarnings(warns []model.FlashbackWarning) {
	for _, warn := range warns {
		a.schemaWarn = appendWarningOnce(a.schemaWarn, warn)
	}
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

func (a *Planner) noteUnknown(table string) {
	for _, have := range a.unknown {
		if have == table {
			return
		}
	}
	a.unknown = append(a.unknown, table)
}

// clearPendingUse drops the deferred definition from every row recorded so
// far for table. Staged rows cannot be edited, so the cut-off is a row
// sequence number that settleBinds applies when a record is read back —
// the same contract undo uses.
func (a *Planner) clearPendingUse(table string) {
	key := strings.ToLower(table)
	names := a.pendingNames[key]
	if len(names) == 0 {
		return
	}
	if a.clearSeq == nil {
		a.clearSeq = map[string]int64{}
	}
	a.clearSeq[key] = a.seq
	for _, name := range names {
		a.noteUnknown(name)
	}
	delete(a.pendingNames, key)
}

func (a *Planner) notePendingName(row model.RedoRow) {
	key := strings.ToLower(row.Table)
	name := redoTable(row.Schema, row.Table)
	for _, have := range a.pendingNames[key] {
		if have == name {
			return
		}
	}
	if a.pendingNames == nil {
		a.pendingNames = map[string][]string{}
	}
	a.pendingNames[key] = append(a.pendingNames[key], name)
}

// appendRows stages one row event. A new transaction key or a full record
// spills what is open; only one record stays in memory.
func (a *Planner) appendRows(ev model.NormalizedEvent, rows []model.RedoRow, bind redoBind) {
	copied := append([]model.RedoRow(nil), rows...)
	binds := make([]redoBind, len(copied))
	seqs := make([]int64, len(copied))
	for i := range binds {
		binds[i] = bind
		if bind.gen != nil {
			binds[i].gen = mysqlexpr.CopyNames(bind.gen)
		}
		if bind.cols != nil {
			binds[i].cols = mysqlexpr.CloneSchemaCols(bind.cols)
		}
		seqs[i] = a.seq
		a.seq++
		if bind.fromPending {
			a.notePendingName(copied[i])
		}
	}
	path := ev.TxnStartPath
	pos := ev.TxnStartPos
	if path == "" {
		path = ev.BinlogPath
	}
	if pos == 0 {
		pos = ev.PositionStart
	}
	a.stage(&stagedRecord{
		kind:  recRows,
		key:   ev.TxnKey,
		gtid:  ev.TxnGTID,
		path:  path,
		pos:   pos,
		rows:  copied,
		binds: binds,
		seqs:  seqs,
	})
}

func (a *Planner) stage(rec *stagedRecord) {
	if a.err != nil {
		return
	}
	if a.spill == nil {
		spill, err := newRedoSpill()
		if err != nil {
			a.err = clierr.Wrap(clierr.KindOutput, fmt.Errorf("redo: cannot create temp file: %w", err))
			return
		}
		a.spill = spill
	}
	if err := a.spill.append(rec); err != nil {
		a.err = clierr.Wrap(clierr.KindOutput, fmt.Errorf("redo: cannot write temp file: %w", err))
		return
	}
	if err := a.overBudget(); err != nil {
		a.err = err
		return
	}
	if err := a.checkMemory(); err != nil {
		a.err = err
	}
}

// noteSplit records that a transaction group kept or skipped row work under
// the selection. The window matches undo: marks all arrive while the group
// is open or just closed, so older entries are final.
func (a *Planner) noteSplit(key, gtid string, kept bool) {
	if a == nil || key == "" {
		return
	}
	if a.split == nil {
		a.split = map[string]*redoSplit{}
	}
	split, ok := a.split[key]
	if !ok {
		a.evictSplits()
		split = &redoSplit{}
		a.split[key] = split
		a.splitOrder = append(a.splitOrder, key)
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

const splitWindow = 64

func (a *Planner) evictSplits() {
	for len(a.splitOrder) >= splitWindow {
		key := a.splitOrder[0]
		a.splitOrder = a.splitOrder[1:]
		if split := a.split[key]; split != nil && split.kept && split.skipped {
			a.splitDone = append(a.splitDone, split.gtid)
		}
		delete(a.split, key)
	}
}

// settleBinds applies table clears and finished deferred checks to a record
// read from the spill. It is idempotent.
func (a *Planner) settleBinds(rec *stagedRecord) {
	for i := range rec.binds {
		bind := &rec.binds[i]
		if bind.fromPending && i < len(rec.seqs) && i < len(rec.rows) {
			if cut, ok := a.clearSeq[strings.ToLower(rec.rows[i].Table)]; ok && rec.seqs[i] < cut {
				*bind = redoBind{}
				continue
			}
		}
		if bind.deferCheck && a.checked {
			bind.gen = mysqlexpr.GeneratedNameSet(bind.cols)
			bind.deferCheck = false
		}
	}
}

// finishSchema runs every deferred row check after the whole input was
// observed. A failure here refuses the script before anything is emitted.
func (a *Planner) finishSchema() error {
	if a.checked {
		return nil
	}
	err := a.spill.forward(func(rec *stagedRecord) error {
		for i := range rec.binds {
			if !rec.binds[i].deferCheck {
				continue
			}
			row := rec.rows[i]
			if err := a.acceptSchema(a.refusalTable(row.Schema, row.Table), row, rec.binds[i].cols); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	a.checked = true
	return nil
}

// Prepare runs every check that can refuse the script and returns the row
// and statement counts. Warnings are complete after it returns.
func (a *Planner) Prepare() (rows int64, err error) {
	if a == nil {
		return 0, refuse("capture", "selected range", fmt.Errorf("%s", i18n.T("error.flashbackCapture", map[string]any{"Table": "selected range"})))
	}
	if a.err != nil {
		return 0, a.err
	}
	if err := a.finishSchema(); err != nil {
		return 0, err
	}
	if !a.countsReady {
		if err := a.countEmissions(); err != nil {
			return 0, err
		}
		a.countsReady = true
	}
	return a.rowCount, nil
}

// countEmissions replays the staged record stream to learn which groups
// stayed open at the end (the cut-boundary warnings) and whether any
// statement will be emitted (the no_data check). It writes nothing.
func (a *Planner) countEmissions() error {
	st := &emitState{open: map[string]bool{}, closed: map[string]bool{}, xa: map[string]bool{}, gtidByKey: map[string]string{}}
	err := a.spill.forward(func(rec *stagedRecord) error {
		a.settleBinds(rec)
		st.track(rec)
		switch rec.kind {
		case recRows:
			a.rowCount += int64(len(rec.rows))
			a.stmtCount += int64(len(rec.rows))
		case recStmt, recXAPair, recBegin, recClose:
			a.stmtCount++
		}
		return nil
	})
	if err != nil {
		return err
	}
	a.cut = st.stillOpen()
	return nil
}

// Warnings is WarningList as the stderr lines redo prints.
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

// WarningList reports schema-file notes, tables whose definitions were not
// seen, transactions a table or --dml filter replayed only in part, and
// transactions still open at the end of the input. Empty when redo has
// nothing to print.
func (a *Planner) WarningList() []model.FlashbackWarning {
	if a == nil {
		return nil
	}
	var out []model.FlashbackWarning
	out = append(out, a.cat.Warnings()...)
	out = append(out, a.schemaWarn...)
	for _, table := range a.unknown {
		out = append(out, model.NewFlashWarning(model.WarnTableDefinitionMissing, "warning.flashbackGenerated", map[string]any{"Table": table}))
	}
	gtids := append([]string(nil), a.splitDone...)
	for _, key := range a.splitOrder {
		split := a.split[key]
		if split == nil || !split.kept || !split.skipped {
			continue
		}
		gtids = append(gtids, split.gtid)
	}
	for _, gtid := range gtids {
		if gtid == "" {
			gtid = "GTID unavailable"
		}
		out = append(out, model.NewFlashWarning(model.WarnPartialTransaction, "warning.redoSplit", map[string]any{"GTID": gtid}))
	}
	for _, label := range a.cut {
		out = append(out, model.NewFlashWarning(model.WarnPartialTransaction, "warning.redoCutTxn", map[string]any{"GTID": label}))
	}
	return out
}

// Statements is how many statements the script will emit, counted by
// Prepare. Zero means there is nothing to print.
func (a *Planner) Statements() int64 { return a.stmtCount }

// WriteTo streams the script to w and returns the emitted row count.
// Refusals were all decided by Prepare; a rendering refusal can still fire
// here, which is why callers stage through Publish — a failed WriteTo
// leaves nothing on stdout (ADR 0022).
func (a *Planner) WriteTo(w io.Writer) (int64, error) {
	rows, err := a.Prepare()
	if err != nil {
		return 0, err
	}
	if a.stmtCount == 0 {
		return 0, nil
	}
	st := &emitState{w: w, open: map[string]bool{}, closed: map[string]bool{}, xa: map[string]bool{}, gtidByKey: map[string]string{}}
	if err := writeScriptHead(w); err != nil {
		return 0, err
	}
	err = a.spill.forward(func(rec *stagedRecord) error {
		a.settleBinds(rec)
		return a.emitRecord(st, rec)
	})
	if err != nil {
		return 0, err
	}
	if err := st.emitTrailers(); err != nil {
		return 0, err
	}
	return rows, nil
}
