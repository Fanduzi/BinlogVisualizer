package flashback

import (
	"fmt"
	"strings"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// reviewGenerated checks schema-file generated columns against every logged image.
// A contradiction refuses the script. An expression that cannot be checked exactly
// is omitted, with a warning, when nothing contradicts it. Images are not retained:
// the check streams the row spill and spills clash keys (ADR 0012).
func (a *Planner) reviewGenerated() error {
	if a.flashReviewed {
		return a.flashReviewErr
	}
	a.flashReviewed = true
	a.flashReviewErr = a.collectGeneratedReview()
	return a.flashReviewErr
}

type reviewBucket struct {
	schema string
	table  string
	rev    *mysqlexpr.StreamReview
}

func (a *Planner) collectGeneratedReview() error {
	if err := a.checkMemory(); err != nil {
		return err
	}
	var order []string
	groups := map[string]*reviewBucket{}
	err := a.eachFlashGroup(false, func(group *flashGroup) error {
		if err := a.checkMemory(); err != nil {
			return err
		}
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
				b = &reviewBucket{schema: row.Schema, table: row.Table, rev: mysqlexpr.NewStreamReview(cols)}
				groups[key] = b
				order = append(order, key)
			}
			if row.Before != nil {
				b.rev.Observe(mysqlexpr.LoggedImage{Columns: row.Columns, Values: row.Before})
			}
			if row.After != nil {
				b.rev.Observe(mysqlexpr.LoggedImage{Columns: row.Columns, Values: row.After})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	needClash := false
	for _, key := range order {
		groups[key].rev.Finish()
		if groups[key].rev.NeedsClash() {
			needClash = true
		}
	}
	if needClash {
		if err := a.spillClashes(order, groups); err != nil {
			return err
		}
	}
	for _, key := range order {
		b := groups[key]
		if !b.rev.NeedsAnyDiv() {
			continue
		}
		if err := a.eachImage(key, groups, func(im mysqlexpr.LoggedImage) error {
			b.rev.NoteDiv(im)
			return a.checkMemory()
		}); err != nil {
			return err
		}
		b.rev.MarkDivDone()
	}
	var mismatches, notes []string
	var warns []model.FlashbackWarning
	hinted := false
	var mismatchTable string
	multiTable := false
	for _, key := range order {
		b := groups[key]
		table := flashTable(b.schema, b.table)
		label := a.refusalTable(b.schema, b.table)
		for i := 0; i < b.rev.Len(); i++ {
			col := b.rev.Col(i)
			example, unknown := b.rev.Outcome(i)
			if example != "" {
				if mismatchTable == "" {
					mismatchTable = label
				} else if mismatchTable != label {
					multiTable = true
				}
				if fit, unchecked := b.rev.DivIncrementFit(i); len(fit) > 0 || len(unchecked) > 0 {
					line := mysqlexpr.DivIncrementMismatch(label, col, example, fit, unchecked).Error()
					if !hinted {
						hinted = true
						line = joinHint(line, mysqlexpr.DivIncrementHint(mysqlexpr.SQLTableName(b.schema, b.table), fit, unchecked))
					}
					mismatches = append(mismatches, line)
					continue
				}
				mismatches = append(mismatches, mysqlexpr.GeneratedMismatch(label, col, example).Error())
				continue
			}
			if !unknown {
				continue
			}
			notes = append(notes, mysqlexpr.UnverifiedGeneratedComment(table, col))
			warns = append(warns, mysqlexpr.UnverifiedGeneratedWarning(table, col))
		}
	}
	if len(mismatches) > 0 {
		table := mismatchTable
		if multiTable {
			table = ""
		}
		return refuse("generated_mismatch", table, fmt.Errorf("%s", strings.Join(mismatches, "\n")))
	}
	a.noteSchemaWarnings(warns)
	a.flashGenNotes = append(a.flashGenNotes, notes...)
	return nil
}

func (a *Planner) spillClashes(order []string, groups map[string]*reviewBucket) error {
	spill := &clashSpill{budget: a.reviewBudget}
	defer spill.close()
	index := map[string]uint32{}
	for i, key := range order {
		index[key] = uint32(i)
	}
	seqs := map[string]uint64{}
	err := a.eachFlashGroup(false, func(group *flashGroup) error {
		if err := a.checkMemory(); err != nil {
			return err
		}
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
			if b == nil || !b.rev.NeedsClash() {
				continue
			}
			bucket := index[key]
			note := func(im mysqlexpr.LoggedImage) error {
				seq := seqs[key]
				seqs[key] = seq + 1
				return b.rev.NoteClash(seq, im, func(col int, seq uint64, ref, lit string) error {
					return spill.add(clashRec{bucket: bucket, col: uint32(col), seq: seq, key: ref, lit: lit})
				})
			}
			if row.Before != nil {
				if err := note(mysqlexpr.LoggedImage{Columns: row.Columns, Values: row.Before}); err != nil {
					return err
				}
			}
			if row.After != nil {
				if err := note(mysqlexpr.LoggedImage{Columns: row.Columns, Values: row.After}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	hits, err := spill.hits()
	if err != nil {
		return err
	}
	type replayCol struct {
		col int
		lit string
	}
	replay := map[string]map[uint64][]replayCol{}
	for _, key := range order {
		b := groups[key]
		bucket := index[key]
		for col := 0; col < b.rev.Len(); col++ {
			hit, ok := hits[bucket][uint32(col)]
			if !ok {
				continue
			}
			if b.rev.NeedsDiv(col) || !b.rev.NeedsExactClash(col) {
				b.rev.SetSawClash(col)
				continue
			}
			bySeq := replay[key]
			if bySeq == nil {
				bySeq = map[uint64][]replayCol{}
				replay[key] = bySeq
			}
			bySeq[hit.firstSeq] = append(bySeq[hit.firstSeq], replayCol{col: col, lit: hit.lit})
		}
	}
	if len(replay) == 0 {
		return nil
	}
	seqs = map[string]uint64{}
	return a.eachFlashGroup(false, func(group *flashGroup) error {
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
			want := replay[key]
			if want == nil {
				continue
			}
			b := groups[key]
			note := func(im mysqlexpr.LoggedImage) {
				seq := seqs[key]
				seqs[key] = seq + 1
				for _, item := range want[seq] {
					b.rev.SetExactClash(item.col, b.rev.Example(im), item.lit)
				}
			}
			if row.Before != nil {
				note(mysqlexpr.LoggedImage{Columns: row.Columns, Values: row.Before})
			}
			if row.After != nil {
				note(mysqlexpr.LoggedImage{Columns: row.Columns, Values: row.After})
			}
		}
		return nil
	})
}

func (a *Planner) eachImage(bucketKey string, groups map[string]*reviewBucket, fn func(mysqlexpr.LoggedImage) error) error {
	return a.eachFlashGroup(false, func(group *flashGroup) error {
		for i, row := range group.rows {
			if i >= len(group.binds) {
				continue
			}
			sig := generatedSig(group.binds[i].cols)
			if sig == "" {
				continue
			}
			key := strings.ToLower(row.Schema) + "\x00" + strings.ToLower(row.Table) + "\x00" + sig
			if key != bucketKey || groups[key] == nil {
				continue
			}
			if row.Before != nil {
				if err := fn(mysqlexpr.LoggedImage{Columns: row.Columns, Values: row.Before}); err != nil {
					return err
				}
			}
			if row.After != nil {
				if err := fn(mysqlexpr.LoggedImage{Columns: row.Columns, Values: row.After}); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
