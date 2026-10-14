// Package analyzer collects selected row images for undo SQL.
// input: retained normalized events that already passed time, position, GTID, schema, table, and DML filters, plus flashback images captured by the parser.
// output: one SQL script that reverses those row changes, or one error and no script when a selected row cannot be rendered exactly, a seen table definition cannot be read, a schema file does not match the binlog columns, a schema-file generated value contradicts the expression, or the selected range contains DDL. Generated columns learned from schema SQL or parsed CREATE/ALTER are omitted from INSERT and UPDATE SET. A schema-file omission is checked by a guard after the session SET lines: apply fails before any transaction when that column is not generated on the target. A mismatch commits and sets the session read-only, and that lock is repeated before each transaction, so a client that continues cannot write. The lock is SET SESSION TRANSACTION READ ONLY, which names no server variable, and a prepared READ WRITE undoes it only when @binlogviz_ok holds this script's token, which the guard sets only after the check ran and matched on MySQL 5.7+ or MariaDB 10.2+ in a writable session, so a failed step, including PREPARE, a header-less block, another script's block, or a reconnect leaves the session read-only. Every undo statement also checks the token, so statements run in a session without it change no row. The token is a hash of the rendered script. A match in a session that is already read-only fails the guard with a disconnect hint. Older servers fail that guard with a clear message and stay read-only. The failing sql_mode value names every mismatched column without a comma, and a long list keeps the count and the first names. Each guard arm is SELECT ... FROM DUAL, which MySQL 5.7 accepts. An expression that cannot be checked exactly is omitted with one stderr warning and a header comment when nothing contradicts it. A no-primary-key WHERE keeps generated columns. A selected table with no definition is warned and still printed. An ENUM index of 0, a zero month or day, and an omitted generated column logged as NULL are each wrapped in a sql_mode save and restore that drops only the flags that would reject the stored value, plus TRADITIONAL.
// stream: rows are spilled per transaction part (flashback_spill.go); the script is rendered twice from the spill, into the guard hash and then to the writer, so memory does not grow with the binlog and nothing is written before every check passes (ADR-0008).
// pos: optional collector on Analyzer. It runs only when Options.Flashback is set.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

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
	seqs  []int64
	// id names the transaction group; chunk numbers its parts. A large
	// transaction is spilled in parts of flashChunkRows rows (#258).
	id    int64
	chunk int
}

// flashChunkRows bounds the rows of one transaction held in memory.
const flashChunkRows = 2048

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
		a.flashErr = flashDDLError(ev, a.refusalTable(ev.Schema, ev.Table))
		return
	}
	if ev.EventType != "ROWS" {
		return
	}
	if len(ev.FlashRows) == 0 {
		a.flashErr = fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{
			"Table": a.refusalTable(ev.Schema, ev.Table),
		}))
		return
	}
	for _, row := range ev.FlashRows {
		if row.ProblemKind != "" {
			a.flashErr = flashRowError(row, a.refusalTable(row.Schema, row.Table))
			return
		}
	}
	res := a.flashCat.resolve(ev.Schema, ev.Table)
	label := refusalLabel(res, ev.Schema, ev.Table)
	bind := flashBind{}
	switch res.kind {
	case resolveRefused:
		a.flashErr = fmt.Errorf("%s", i18n.Tf("error.flashbackGenerated", map[string]any{
			"Table": label,
		}))
		return
	case resolveMissing, resolveAmbiguous:
		a.noteFlashUnknown(flashTable(ev.Schema, ev.Table))
	default: // resolveExact, resolveFold
		if !res.checkRows {
			bind.gen = generatedNameSet(res.cols)
			break
		}
		bind.cols = res.cols
		bind.fromPending = res.deferred
		bind.deferCheck = res.deferred
		if res.deferred {
			// The schema is not final until every table map for this name is seen.
		} else if err := a.acceptSchemaRows(label, ev.FlashRows, res.cols); err != nil {
			a.flashErr = err
			return
		} else {
			bind.gen = generatedNameSet(res.cols)
		}
	}
	a.appendFlashRows(ev, ev.FlashRows, bind)
	a.noteFlashbackKept(ev)
}

func (a *Analyzer) acceptSchemaRows(label string, rows []model.FlashRow, cols []schemaCol) error {
	for _, row := range rows {
		if err := a.acceptSchema(label, row, cols); err != nil {
			return err
		}
	}
	return nil
}

func (a *Analyzer) acceptSchema(label string, row model.FlashRow, cols []schemaCol) error {
	warns, err := validateSchemaRow(label, row, cols)
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

// clearPendingUse drops the deferred definition from every row recorded so
// far for table. Spilled rows cannot be edited, so the cut-off is a row
// sequence number that settleBinds applies when a group is read back.
func (a *Analyzer) clearPendingUse(table string) {
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
func (a *Analyzer) settleBinds(group *flashGroup) {
	for i := range group.binds {
		bind := &group.binds[i]
		if bind.fromPending && i < len(group.seqs) && i < len(group.rows) {
			if cut, ok := a.flashClearSeq[strings.ToLower(group.rows[i].Table)]; ok && group.seqs[i] < cut {
				*bind = flashBind{}
				continue
			}
		}
		if bind.deferCheck && a.flashChecked {
			bind.gen = generatedNameSet(bind.cols)
			bind.deferCheck = false
		}
	}
}

// eachFlashGroup visits every kept group, in binlog order or last first.
// Resident memory is one group: the rest are read back from the spill.
func (a *Analyzer) eachFlashGroup(backward bool, fn func(*flashGroup) error) error {
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

func (a *Analyzer) finishSchema() error {
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
func (a *Analyzer) appendFlashRows(ev model.NormalizedEvent, rows []model.FlashRow, bind flashBind) {
	copied := append([]model.FlashRow(nil), rows...)
	binds := make([]flashBind, len(copied))
	seqs := make([]int64, len(copied))
	for i := range binds {
		binds[i] = bind
		if bind.gen != nil {
			binds[i].gen = copyNames(bind.gen)
		}
		if bind.cols != nil {
			binds[i].cols = cloneSchemaCols(bind.cols)
		}
		seqs[i] = a.flashSeq
		a.flashSeq++
		if bind.fromPending {
			a.notePendingName(copied[i])
		}
	}
	key := ev.TxnKey
	if cur := a.flashCur; cur != nil && key != "" && cur.key == key {
		cur.rows = append(cur.rows, copied...)
		cur.binds = append(cur.binds, binds...)
		cur.seqs = append(cur.seqs, seqs...)
		if len(cur.rows) >= flashChunkRows {
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

func (a *Analyzer) notePendingName(row model.FlashRow) {
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

func (a *Analyzer) spillCurrent() error {
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
			return fmt.Errorf("flashback: cannot create temp file: %w", err)
		}
		a.flashSpill = spill
	}
	if err := a.flashSpill.append(cur); err != nil {
		return fmt.Errorf("flashback: cannot write temp file: %w", err)
	}
	return nil
}

func (a *Analyzer) closeFlashSpill() {
	a.flashSpill.close()
	a.flashSpill = nil
	a.flashCur = nil
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

func (a *Analyzer) evictFlashSplits() {
	for len(a.flashSplitOrder) >= flashSplitWindow {
		key := a.flashSplitOrder[0]
		a.flashSplitOrder = a.flashSplitOrder[1:]
		if split := a.flashSplit[key]; split != nil && split.kept && split.skipped {
			a.flashSplitDone = append(a.flashSplitDone, split.gtid)
		}
		delete(a.flashSplit, key)
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
	out = append(out, a.flashCat.warnings()...)
	out = append(out, a.flashSchemaWarn...)
	for _, table := range a.flashUnknown {
		out = append(out, i18n.Tf("warning.flashbackGenerated", map[string]any{"Table": table}))
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
		out = append(out, i18n.Tf("warning.flashbackSplit", map[string]any{"GTID": gtid}))
	}
	return out
}

// FlashbackSQL returns undo SQL for the selected row changes.
// A non-nil error means the script must not be applied; the string is empty.
// It holds the whole script in memory; the CLI uses WriteFlashback.
func (a *Analyzer) FlashbackSQL() (string, error) {
	var b strings.Builder
	if _, err := a.WriteFlashback(&b); err != nil {
		return "", err
	}
	return b.String(), nil
}

// PrepareFlashback runs every check that can refuse the script and returns
// the number of undo rows. Zero rows means there is nothing to print.
// FlashbackWarnings is complete after it returns.
func (a *Analyzer) PrepareFlashback() (int, error) {
	if a == nil {
		return 0, fmt.Errorf("%s", i18n.T("error.flashbackCapture", map[string]any{"Table": "selected range"}))
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
func (a *Analyzer) WriteFlashback(w io.Writer) (int, error) {
	rows, err := a.PrepareFlashback()
	if err != nil || rows == 0 {
		return 0, err
	}
	var cols []guardCol
	seen := map[string]struct{}{}
	if err := a.eachFlashGroup(false, func(g *flashGroup) error {
		cols = appendGuardCols(cols, seen, *g)
		return nil
	}); err != nil {
		return 0, err
	}
	err = writeFlashbackScript(w, func(fn func(*flashGroup) error) error {
		return a.eachFlashGroup(true, fn)
	}, a.flashGenNotes, cols)
	var rowErr *generatedRowRefusal
	if errors.As(err, &rowErr) {
		return 0, rowErr.named(a.refusalTable(rowErr.schema, rowErr.table))
	}
	if err != nil {
		return 0, err
	}
	return rows, nil
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
	err := a.eachFlashGroup(false, func(group *flashGroup) error {
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
		return nil
	})
	if err != nil {
		return err
	}
	var mismatches, notes, warns []string
	hinted := false
	for _, key := range order {
		b := groups[key]
		table := flashTable(b.schema, b.table)
		label := a.refusalTable(b.schema, b.table)
		var imgs *genImages
		for _, col := range b.cols {
			if !col.generated {
				continue
			}
			if imgs == nil {
				imgs = prepareGenImages(b.images, b.cols)
			}
			example, unknown := imgs.outcome(col)
			if example != "" {
				// Any fitting or unchecked increment gets the increment
				// message: an unchecked one may be where the dump is right,
				// so the dump is not called wrong (#243).
				if fit, unchecked := imgs.divIncrementFit(col); len(fit) > 0 || len(unchecked) > 0 {
					line := divIncrementMismatch(label, col, example, fit, unchecked).Error()
					if !hinted {
						hinted = true
						line = joinHint(line, divIncrementHint(sqlTableName(b.schema, b.table), fit, unchecked))
					}
					mismatches = append(mismatches, line)
					continue
				}
				mismatches = append(mismatches, generatedMismatch(label, col, example).Error())
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

// joinHint appends the once-per-run hand-restore note to a refusal line,
// keeping it one line ("Error: ..."). zh text needs no separating space.
func joinHint(line, hint string) string {
	if strings.HasSuffix(line, "。") {
		return line + hint
	}
	return line + " " + hint
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

func flashRowError(row model.FlashRow, table string) error {
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
		// The column follows the table name, so the plain binlog name is kept.
		return fmt.Errorf("%s", i18n.Tf("error.flashbackType", map[string]any{
			"Table":  flashTable(row.Schema, row.Table),
			"Column": column,
			"Type":   row.ProblemType,
		}))
	default:
		return fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": table}))
	}
}

func flashDDLError(ev model.NormalizedEvent, table string) error {
	statement := strings.TrimSpace(ev.QuerySQL)
	if statement == "" {
		statement = "DDL"
	}
	statement = oneLine(statement, 120)
	return fmt.Errorf("%s", i18n.Tf("error.flashbackDDL", map[string]any{
		"Table":     table,
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
	}, notes, omittedGeneratedCols(groups))
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

// writeFlashbackScript renders the script twice from backward (last group
// first): into the guard hash, then to w with the token sealed in. Nothing
// reaches w when a statement cannot be rendered.
func writeFlashbackScript(w io.Writer, backward func(func(*flashGroup) error) error, notes []string, cols []guardCol) error {
	guard := newFlashGuard(cols)
	h := sha256.New()
	if err := renderFlashbackTo(toWriter(h), backward, notes, guard); err != nil {
		return err
	}
	return renderFlashbackTo(guard.sealWriter(w, h.Sum(nil)), backward, notes, guard)
}

func renderFlashbackTo(out writerFunc, backward func(func(*flashGroup) error) error, notes []string, guard flashGuard) error {
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
	if guard.Header() != "" {
		b.WriteByte('\n')
		b.WriteString(guard.Header())
	}
	if err := out(b.String()); err != nil {
		return err
	}
	// With a guard, every undo statement also checks this script's token, so
	// a statement run in a session where this script's guard did not match
	// (a reconnect inside a block, a block from another script) changes no row.
	tokenCond := guard.RowCond()
	// Parts of one transaction arrive last part first; the block opens at
	// the first part seen and commits after part 0.
	open := int64(-1)
	return backward(func(group *flashGroup) error {
		b.Reset()
		if group.id != open {
			open = group.id
			writeBlockHead(&b, group, guard)
		}
		for j := len(group.rows) - 1; j >= 0; j-- {
			var gen map[string]struct{}
			if j < len(group.binds) {
				gen = group.binds[j].gen
			}
			statement, err := renderUndoStatement(group.rows[j], gen, tokenCond)
			if err != nil {
				return err
			}
			b.WriteString(statement)
			b.WriteByte('\n')
			if b.Len() > 1<<20 {
				if err := out(b.String()); err != nil {
					return err
				}
				b.Reset()
			}
		}
		if group.chunk == 0 {
			b.WriteString("COMMIT;\n")
		}
		return out(b.String())
	})
}

func writeBlockHead(b *strings.Builder, group *flashGroup, guard flashGuard) {
	b.WriteByte('\n')
	gtid := group.gtid
	if gtid == "" {
		gtid = "GTID unavailable"
	}
	fmt.Fprintf(b, "-- gtid: %s\n", oneLine(gtid, 0))
	file := filepath.Base(group.path)
	if file == "" || file == "." {
		file = "binlog"
	}
	fmt.Fprintf(b, "-- binlog: %s:%d\n", oneLine(file, 0), group.pos)
	if lock := guard.BlockLock(); lock != "" {
		// SET sql_mode, COMMIT, START TRANSACTION, and SET GTID_NEXT do not
		// clear this session flag. Repeat the same lock outside a
		// transaction so this block cannot start writable: it is read-only
		// unless the header guard ran in this session and matched.
		b.WriteString(lock)
	}
	b.WriteString("START TRANSACTION;\n")
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
func (a *Analyzer) refusalTable(schema, table string) string {
	return refusalLabel(a.flashCat.peek(schema, table), schema, table)
}

// refusalLabel is the table name every refusal prints. A table bound by a
// letter-case fold also names the schema-file table, in every language and on
// every refusal line (#225).
func refusalLabel(res resolution, schema, table string) string {
	name := flashTable(schema, table)
	if res.fileTable == "" {
		return name
	}
	return i18n.Tf("error.flashbackSchemaFoldLabel", map[string]any{"Binlog": name, "File": res.fileTable})
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
