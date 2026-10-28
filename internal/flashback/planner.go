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
	"strings"

	"binlogviz/internal/clierr"
	"binlogviz/internal/guard"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/schemacatalog"
)

// Options is the flashback half of an analyzer run.
type Options struct {
	SchemaSQL     string
	SchemaFileDB  string
	Conflict      string
	Plan          bool
	ConflictLimit int
	// Selection is the table and schema filter of the run, or nil when no
	// such filter is set. It decides which DDL in range refuses (ADR 0015).
	Selection Selection
}

// Selection answers whether a table or a schema named by a DDL could hold a
// selected row. Both answers lean to true: letter case is folded for
// inclusion and an empty schema means the schema is not known.
type Selection interface {
	MayHoldTable(schema, table string) bool
	MayHoldSchema(schema string) bool
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
	// flashMariaDB is set when a planned row event came from MariaDB.
	flashMariaDB    bool
	flashGenNotes   []string
	flashReviewed   bool
	flashReviewErr  error
	flashSplit      map[string]*flashSplit
	flashSplitOrder []string
	flashSplitDone  []string

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
		session := ev.QuerySchema
		if session == "" {
			session = ev.Schema
		}
		dropped = append(dropped, a.flashCat.ObserveDDL(session, ev.QuerySQL)...)
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
		a.flashErr = a.ddlRefusal(ev)
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
			a.flashErr = flashRowError(row, a.refusalTable(row.Schema, row.Table), ev.ServerFlavor)
			return
		}
	}
	if ev.ServerFlavor == "mariadb" {
		a.flashMariaDB = true
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
// were not seen, transactions a table or --dml filter undid only in part,
// then a MariaDB binlog (the script does not apply on MariaDB).
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
	if a.flashMariaDB {
		out = append(out, model.NewFlashWarning(model.WarnMariaDBApplyUnsupported, "warning.flashbackMariaDB", nil))
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
