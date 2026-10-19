// Package analyzer detects flashback conflicts: later changes to selected rows.
// input: every row event in the scanned range (selected or not) and the selected rows.
// output: a FlashbackConflictError that refuses the script by default (#259), the post-image guard switch, and the JSON plan (plan_version 2; a refusal still prints it, #270).
// pos: flashback collector on Analyzer (ADR 0008). Memory is one 8-byte hash per selected row key plus one candidate per later change to a selected key, not the binlog. A candidate whose own change is selected is dropped (#267).
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// Flashback conflict policies (--conflict).
const (
	FlashbackConflictRefuse = "refuse"
	FlashbackConflictGuard  = "guard"
)

// flashConflictListed is the default bound on the conflicts kept for
// messages and the plan (--conflict-limit).
const flashConflictListed = 20

// conflictLimit is how many conflicts to list; negative lists all.
func (a *Analyzer) conflictLimit() int {
	if a.opts.FlashbackConflictLimit == 0 {
		return flashConflictListed
	}
	return a.opts.FlashbackConflictLimit
}

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
			parts = append(parts, row.Columns[i]+"="+mysqlexpr.OneLine(image[i], 64))
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
	limit := a.conflictLimit()
	for i := 0; i < len(left) && (limit < 0 || i < limit); i++ {
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

// FlashbackPlanVersion is the --plan schema version. It changes only when a
// field changes meaning or type; added fields keep it.
// 2: warnings are {code, message} objects; refusal names why the script is refused (#270, #282).
const FlashbackPlanVersion = 2

// FlashbackPlan is the --plan output: what the script would undo, and why it
// would be refused.
type FlashbackPlan struct {
	PlanVersion       int                      `json:"plan_version"`
	Transactions      int                      `json:"transactions"`
	Rows              int                      `json:"rows"`
	Tables            []FlashbackPlanTable     `json:"tables"`
	ConflictPolicy    string                   `json:"conflict_policy"`
	Conflicts         FlashbackPlanCounts      `json:"conflicts"`
	GuardedStatements int                      `json:"guarded_statements"`
	WouldRefuse       bool                     `json:"would_refuse"`
	Refusal           *FlashbackPlanRefusal    `json:"refusal"`
	Warnings          []model.FlashbackWarning `json:"warnings"`
}

// FlashbackPlanRefusal is why the real run would print no script. Kind is
// the exit kind: "conflict" (exit 7) or "refused" (exit 6). After a
// "refused", the counts are zero: the rows were not read to the end.
type FlashbackPlanRefusal struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

// FlashbackPlanTable counts the logged operations an undo reverses, per table.
type FlashbackPlanTable struct {
	Table  string `json:"table"`
	Insert int    `json:"insert"`
	Update int    `json:"update"`
	Delete int    `json:"delete"`
}

// FlashbackPlanCounts lists conflicts, bounded by --conflict-limit.
type FlashbackPlanCounts struct {
	Count   int                 `json:"count"`
	Listed  []FlashbackConflict `json:"listed"`
	Omitted int                 `json:"omitted"`
}

// WriteFlashbackPlan writes the JSON plan instead of SQL. It returns the
// error a real run would return after writing the plan, so the exit code
// matches. A refusal (exit 6) or a conflict (exit 7) still writes a plan,
// with would_refuse and refusal set; any other failure writes none.
func (a *Analyzer) WriteFlashbackPlan(w io.Writer) error {
	plan, err := a.flashbackPlan()
	if err != nil {
		kind := clierr.KindRefused
		var conflict *FlashbackConflictError
		if errors.As(err, &conflict) {
			kind = clierr.KindConflict
		} else if typed, _ := clierr.Of(err); typed != clierr.KindError {
			return err
		}
		plan.WouldRefuse = true
		plan.Refusal = &FlashbackPlanRefusal{Kind: string(kind), Message: err.Error()}
	}
	body, jerr := json.MarshalIndent(plan, "", "  ")
	if jerr != nil {
		return jerr
	}
	if _, werr := w.Write(append(body, '\n')); werr != nil {
		return werr
	}
	return err
}

// flashbackPlan counts what the script would undo. The error is the one the
// real run would return; the plan holds what was known when it stopped.
func (a *Analyzer) flashbackPlan() (plan FlashbackPlan, err error) {
	plan = FlashbackPlan{
		PlanVersion:    FlashbackPlanVersion,
		ConflictPolicy: a.conflictPolicy(),
		Tables:         []FlashbackPlanTable{},
		Conflicts:      FlashbackPlanCounts{Listed: []FlashbackConflict{}},
	}
	defer func() {
		plan.Warnings = append([]model.FlashbackWarning{}, a.FlashbackWarningList()...)
	}()
	if _, err = a.prepareChecks(); err != nil {
		return plan, err
	}
	tables := map[string]*FlashbackPlanTable{}
	guard := a.postImageGuard()
	lastID := int64(-1)
	err = a.eachFlashGroup(false, func(g *flashGroup) error {
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
		return plan, err
	}
	for _, t := range tables {
		plan.Tables = append(plan.Tables, *t)
	}
	sort.Slice(plan.Tables, func(i, j int) bool { return plan.Tables[i].Table < plan.Tables[j].Table })
	a.settleConflicts()
	plan.Conflicts = FlashbackPlanCounts{Count: a.flashConflicts, Listed: append([]FlashbackConflict{}, a.flashConflictList...)}
	plan.Conflicts.Omitted = plan.Conflicts.Count - len(plan.Conflicts.Listed)
	return plan, a.conflictError()
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
