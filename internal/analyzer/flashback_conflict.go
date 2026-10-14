// Package analyzer detects flashback conflicts: later changes to selected rows.
// input: every row event in the scanned range (selected or not) and the selected rows.
// output: a FlashbackConflictError that refuses the script by default (#259), the post-image guard switch, and the JSON plan.
// pos: flashback collector on Analyzer (ADR 0008). Memory is one 8-byte hash per selected row key plus one candidate per later change to a selected key, not the binlog. A candidate whose own change is selected is dropped (#267).
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// Flashback conflict policies (--conflict).
const (
	FlashbackConflictRefuse = "refuse"
	FlashbackConflictGuard  = "guard"
)

// flashConflictListed bounds the conflicts kept for messages and the plan.
const flashConflictListed = 20

// FlashbackConflict is one later change to a selected row.
type FlashbackConflict struct {
	Table         string `json:"table"`
	Key           string `json:"key"`
	LaterGTID     string `json:"later_gtid"`
	LaterPosition string `json:"later_position"`
	LaterOp       string `json:"later_op"`
}

// FlashbackConflictError refuses a script that would overwrite later changes.
type FlashbackConflictError struct {
	Count  int
	Listed []FlashbackConflict
}

func (e *FlashbackConflictError) Error() string {
	first := ""
	if len(e.Listed) > 0 {
		c := e.Listed[0]
		first = fmt.Sprintf("%s %s (%s %s at %s)", c.Table, c.Key, c.LaterOp, c.LaterGTID, c.LaterPosition)
	}
	return i18n.Tf("error.flashbackConflict", map[string]any{"Count": e.Count, "First": first})
}

// ValidFlashbackConflict reports whether policy is a --conflict value.
func ValidFlashbackConflict(policy string) bool {
	switch policy {
	case "", FlashbackConflictRefuse, FlashbackConflictGuard:
		return true
	}
	return false
}

func (a *Analyzer) conflictPolicy() string {
	if a.opts.FlashbackConflict == FlashbackConflictGuard {
		return FlashbackConflictGuard
	}
	return FlashbackConflictRefuse
}

func (a *Analyzer) postImageGuard() bool {
	return a.conflictPolicy() == FlashbackConflictGuard
}

func rowKeyHash(row *model.FlashRow, image []string) (uint64, bool) {
	if image == nil || len(row.Columns) == 0 {
		return 0, false
	}
	h := fnv.New64a()
	h.Write([]byte(strings.ToLower(row.Schema)))
	h.Write([]byte{0})
	h.Write([]byte(strings.ToLower(row.Table)))
	if !row.NoPK && len(row.PK) > 0 {
		for _, i := range row.PK {
			if i < 0 || i >= len(image) {
				return 0, false
			}
			h.Write([]byte{0})
			h.Write([]byte(image[i]))
		}
		return h.Sum64(), true
	}
	h.Write([]byte{1})
	for _, v := range image {
		h.Write([]byte{0})
		h.Write([]byte(v))
	}
	return h.Sum64(), true
}

func txnHash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func rowKeyLabel(row *model.FlashRow, image []string) string {
	if row.NoPK || len(row.PK) == 0 {
		return "(no primary key; whole row image)"
	}
	parts := make([]string, 0, len(row.PK))
	for _, i := range row.PK {
		if i >= 0 && i < len(image) && i < len(row.Columns) {
			parts = append(parts, row.Columns[i]+"="+oneLine(image[i], 64))
		}
	}
	return strings.Join(parts, ",")
}

// conflictRef is one later change to a selected key, from transaction txn.
type conflictRef struct{ txn, key uint64 }

// pendingConflict is a candidate until the run ends: if the same transaction's
// change to the key is selected too, the script undoes it and it is dropped (#267).
type pendingConflict struct {
	seq    int64
	detail FlashbackConflict
}

// noteSelectedKeys remembers the row keys an undo will write, and resolves
// candidates from this same transaction: a selected change is undone by
// this script, so it is not a later change the script would overwrite.
func (a *Analyzer) noteSelectedKeys(txnKey string, rows []model.FlashRow) {
	if a.flashKeys == nil {
		a.flashKeys = map[uint64]uint64{}
	}
	txn := txnHash(txnKey)
	for i := range rows {
		for _, image := range [][]string{rows[i].Before, rows[i].After} {
			if k, ok := rowKeyHash(&rows[i], image); ok {
				a.flashKeys[k] = txn
				delete(a.flashPending, conflictRef{txn: txn, key: k})
			}
		}
	}
}

// checkFlashConflict runs on every row event before any filter. A selected
// key touched by another, later transaction is a conflict.
func (a *Analyzer) checkFlashConflict(ev model.NormalizedEvent) {
	if len(a.flashKeys) == 0 || ev.EventType != "ROWS" || len(ev.FlashRows) == 0 {
		return
	}
	txn := uint64(0)
	haveTxn := false
	for i := range ev.FlashRows {
		row := &ev.FlashRows[i]
		if row.ProblemKind != "" {
			continue
		}
		for _, image := range [][]string{row.Before, row.After} {
			k, ok := rowKeyHash(row, image)
			if !ok {
				continue
			}
			owner, hit := a.flashKeys[k]
			if !hit {
				continue
			}
			if !haveTxn {
				txn, haveTxn = txnHash(a.txnBuilder.CurrentTxnKey()), true
			}
			if owner == txn {
				continue
			}
			ref := conflictRef{txn: txn, key: k}
			if _, seen := a.flashPending[ref]; seen {
				break
			}
			if a.flashPending == nil {
				a.flashPending = map[conflictRef]pendingConflict{}
			}
			gtid, _, _ := a.txnBuilder.touchLocation()
			if gtid == "" {
				gtid = "GTID unavailable"
			}
			a.flashPendingSeq++
			a.flashPending[ref] = pendingConflict{seq: a.flashPendingSeq, detail: FlashbackConflict{
				Table:         flashTable(row.Schema, row.Table),
				Key:           rowKeyLabel(row, image),
				LaterGTID:     gtid,
				LaterPosition: fmt.Sprintf("%s:%d", filepath.Base(ev.BinlogPath), ev.PositionStart),
				LaterOp:       row.Op,
			}}
			break
		}
	}
}

// settleConflicts turns the candidates left after the run into the
// conflict count and the first listed conflicts, in binlog order.
func (a *Analyzer) settleConflicts() {
	if a.flashConflictsSettled {
		return
	}
	a.flashConflictsSettled = true
	left := make([]pendingConflict, 0, len(a.flashPending))
	for _, p := range a.flashPending {
		left = append(left, p)
	}
	sort.Slice(left, func(i, j int) bool { return left[i].seq < left[j].seq })
	a.flashConflicts = len(left)
	a.flashConflictList = nil
	for i := 0; i < len(left) && i < flashConflictListed; i++ {
		a.flashConflictList = append(a.flashConflictList, left[i].detail)
	}
}

func (a *Analyzer) conflictError() error {
	a.settleConflicts()
	if a.flashConflicts == 0 || a.postImageGuard() {
		return nil
	}
	return &FlashbackConflictError{Count: a.flashConflicts, Listed: append([]FlashbackConflict(nil), a.flashConflictList...)}
}

// FlashbackPlan is the --plan output: what the script would undo, and why it
// would be refused. plan_version changes only when a field changes meaning.
type FlashbackPlan struct {
	PlanVersion       int                  `json:"plan_version"`
	Transactions      int                  `json:"transactions"`
	Rows              int                  `json:"rows"`
	Tables            []FlashbackPlanTable `json:"tables"`
	ConflictPolicy    string               `json:"conflict_policy"`
	Conflicts         FlashbackPlanCounts  `json:"conflicts"`
	GuardedStatements int                  `json:"guarded_statements"`
	WouldRefuse       bool                 `json:"would_refuse"`
	Warnings          []string             `json:"warnings"`
}

// FlashbackPlanTable counts the logged operations an undo reverses, per table.
type FlashbackPlanTable struct {
	Table  string `json:"table"`
	Insert int    `json:"insert"`
	Update int    `json:"update"`
	Delete int    `json:"delete"`
}

// FlashbackPlanCounts lists conflicts, bounded.
type FlashbackPlanCounts struct {
	Count   int                 `json:"count"`
	Listed  []FlashbackConflict `json:"listed"`
	Omitted int                 `json:"omitted"`
}

// WriteFlashbackPlan writes the JSON plan instead of SQL. It returns the
// error a real run would return (a refusal or a conflict) after writing the
// plan, so the exit code matches; a refusal other than a conflict writes no plan.
func (a *Analyzer) WriteFlashbackPlan(w io.Writer) error {
	if _, err := a.prepareChecks(); err != nil {
		return err
	}
	plan := FlashbackPlan{PlanVersion: 1, ConflictPolicy: a.conflictPolicy(), Tables: []FlashbackPlanTable{}, Warnings: a.FlashbackWarnings()}
	if plan.Warnings == nil {
		plan.Warnings = []string{}
	}
	tables := map[string]*FlashbackPlanTable{}
	guard := a.postImageGuard()
	lastID := int64(-1)
	err := a.eachFlashGroup(false, func(g *flashGroup) error {
		if g.id != lastID {
			lastID = g.id
			plan.Transactions++
		}
		for i := range g.rows {
			row := &g.rows[i]
			plan.Rows++
			name := flashTable(row.Schema, row.Table)
			t := tables[name]
			if t == nil {
				t = &FlashbackPlanTable{Table: name}
				tables[name] = t
			}
			switch row.Op {
			case "INSERT":
				t.Insert++
			case "UPDATE":
				t.Update++
			default:
				t.Delete++
			}
			if guard && postImageGuarded(row) {
				plan.GuardedStatements++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, t := range tables {
		plan.Tables = append(plan.Tables, *t)
	}
	sort.Slice(plan.Tables, func(i, j int) bool { return plan.Tables[i].Table < plan.Tables[j].Table })
	a.settleConflicts()
	plan.Conflicts = FlashbackPlanCounts{Count: a.flashConflicts, Listed: append([]FlashbackConflict{}, a.flashConflictList...)}
	plan.Conflicts.Omitted = plan.Conflicts.Count - len(plan.Conflicts.Listed)
	cerr := a.conflictError()
	plan.WouldRefuse = cerr != nil
	body, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	if _, err := w.Write(append(body, '\n')); err != nil {
		return err
	}
	return cerr
}

// postImageGuarded reports whether --conflict guard changes this row's undo.
func postImageGuarded(row *model.FlashRow) bool {
	switch row.Op {
	case "UPDATE", "INSERT":
		return true
	default:
		return !row.NoPK && len(row.PK) > 0
	}
}

// FlashbackPlanMode reports whether --plan replaces the SQL with the JSON plan.
func (a *Analyzer) FlashbackPlanMode() bool { return a != nil && a.opts.FlashbackPlan }
