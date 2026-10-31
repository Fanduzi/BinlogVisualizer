package redo

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
	"binlogviz/internal/model"
	"binlogviz/internal/splitout"
)

// runSplit drives the planner like the command does: stage events, then
// WriteSplit into a published sink. It returns the part files' contents in
// manifest order.
func runSplit(t *testing.T, spec splitout.Spec, events ...model.NormalizedEvent) (*Planner, []string) {
	t.Helper()
	p := New(Options{})
	t.Cleanup(p.Close)
	for i := range events {
		p.Note(events[i])
	}
	root := t.TempDir()
	spec.Dir = filepath.Join(root, "out")
	sink, err := splitout.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Abort()
	if _, err := p.WriteSplit(sink, spec); err != nil {
		return p, nil
	}
	if err := sink.Publish(); err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, name := range sink.Manifest() {
		body, err := os.ReadFile(filepath.Join(spec.Dir, name))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, string(body))
	}
	return p, files
}

// statements returns a file's executable lines — no comments, no preamble.
func statements(sql string) []string {
	var out []string
	for _, line := range strings.Split(sql, "\n") {
		if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "SET SESSION sql_mode") ||
			strings.HasPrefix(line, "SET NAMES") || strings.HasPrefix(line, "SET time_zone") {
			continue
		}
		out = append(out, line)
	}
	return out
}

func TestSplitCutMidGroupSealsBothSides(t *testing.T) {
	// A file cut inside a transaction group ends that file with COMMIT and
	// opens the next with the continued-GTID comment and BEGIN, so each
	// part applies on its own (ADR 0025).
	row := func(id int64, note string) model.RedoRow {
		r := insertRow()
		r.After = []any{id, note}
		return r
	}
	var events []model.NormalizedEvent
	events = append(events, ev("GTID", "k1", "uuid:1"), ev("BEGIN", "k1", "uuid:1"))
	for i := int64(1); i <= 4; i++ {
		events = append(events, ev("ROWS", "k1", "uuid:1", row(i, "a")))
	}
	events = append(events, ev("XID", "k1", "uuid:1"))
	_, files := runSplit(t, splitout.Spec{MaxStatements: 3}, events...)
	if len(files) != 2 {
		t.Fatalf("files = %d, want 2: %v", len(files), files)
	}
	stmts0 := statements(files[0])
	if stmts0[len(stmts0)-1] != "COMMIT;" {
		t.Fatalf("first file does not commit the cut group:\n%s", files[0])
	}
	if !strings.Contains(files[1], "-- gtid: uuid:1 (continued in part 000002)") {
		t.Fatalf("no continuation comment:\n%s", files[1])
	}
	stmts1 := statements(files[1])
	if stmts1[0] != "BEGIN;" || stmts1[len(stmts1)-1] != "COMMIT;" {
		t.Fatalf("continued file missing boundaries:\n%s", files[1])
	}
	var inserts int
	for _, f := range files {
		inserts += strings.Count(f, "INSERT INTO `shop`.`orders`")
	}
	if inserts != 4 {
		t.Fatalf("inserts across files = %d, want 4", inserts)
	}
}

func TestSplitWarnsOnSpanningGroup(t *testing.T) {
	var events []model.NormalizedEvent
	events = append(events, ev("GTID", "k1", "uuid:7"), ev("BEGIN", "k1", "uuid:7"))
	for i := 0; i < 4; i++ {
		events = append(events, ev("ROWS", "k1", "uuid:7", insertRow()))
	}
	events = append(events, ev("XID", "k1", "uuid:7"))
	p, _ := runSplit(t, splitout.Spec{MaxStatements: 2}, events...)
	var found bool
	for _, w := range p.Warnings() {
		if strings.Contains(w, "uuid:7") && strings.Contains(w, "spans multiple output files") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no file-span warning: %#v", p.Warnings())
	}
}

func TestSplitBoundaryAtGroupEdgeKeepsWholeGroups(t *testing.T) {
	// When groups fit, each file holds complete transactions and nothing
	// warns — the boundary landed exactly at a group edge.
	events := []model.NormalizedEvent{
		ev("GTID", "k1", "uuid:1"), ev("BEGIN", "k1", "uuid:1"),
		ev("ROWS", "k1", "uuid:1", insertRow()), ev("XID", "k1", "uuid:1"),
		ev("GTID", "k2", "uuid:2"), ev("BEGIN", "k2", "uuid:2"),
		ev("ROWS", "k2", "uuid:2", insertRow()), ev("XID", "k2", "uuid:2"),
	}
	p, files := runSplit(t, splitout.Spec{MaxStatements: 3}, events...)
	if len(files) != 2 {
		t.Fatalf("files = %d, want 2", len(files))
	}
	for i, f := range files {
		stmts := statements(f)
		if stmts[0] != "BEGIN;" || stmts[len(stmts)-1] != "COMMIT;" {
			t.Fatalf("file %d does not hold a whole group:\n%s", i, f)
		}
		if strings.Contains(f, "continued in part") {
			t.Fatalf("file %d claims a continuation it does not have:\n%s", i, f)
		}
	}
	for _, w := range p.Warnings() {
		if strings.Contains(w, "spans multiple output files") {
			t.Fatalf("unexpected span warning: %#v", p.Warnings())
		}
	}
}

func TestSplitDeferredBeginLeavesNoStub(t *testing.T) {
	// The count lands exactly between a group's BEGIN and its first row.
	// Deferring BEGIN to the first statement means the earlier file ends
	// with the previous group's COMMIT — never an empty BEGIN;COMMIT stub.
	var events []model.NormalizedEvent
	events = append(events,
		ev("GTID", "k1", "uuid:1"), ev("BEGIN", "k1", "uuid:1"),
		ev("ROWS", "k1", "uuid:1", insertRow()), ev("XID", "k1", "uuid:1"),
		ev("GTID", "k2", "uuid:2"), ev("BEGIN", "k2", "uuid:2"),
		ev("ROWS", "k2", "uuid:2", insertRow()), ev("XID", "k2", "uuid:2"))
	_, files := runSplit(t, splitout.Spec{MaxStatements: 4}, events...)
	if len(files) != 2 {
		t.Fatalf("files = %d, want 2: %v", len(files), files)
	}
	if strings.HasSuffix(strings.TrimRight(files[0], "\n"), "BEGIN;\nCOMMIT;") ||
		strings.Contains(files[0], "uuid:2") {
		t.Fatalf("first file kept a stub or a stray comment:\n%s", files[0])
	}
	stmts := statements(files[1])
	if stmts[0] != "BEGIN;" {
		t.Fatalf("second group did not begin in its file:\n%s", files[1])
	}
}

func TestSplitPerTableRefuses(t *testing.T) {
	// Per-table partition scatters one transaction's rows across files —
	// redo keeps mirrored order, so the flag is a usage error (ADR 0025).
	p := New(Options{})
	t.Cleanup(p.Close)
	p.Note(ev("BEGIN", "k1", "uuid:1"))
	p.Note(ev("ROWS", "k1", "uuid:1", insertRow()))
	err := p.PrepareSplit(splitout.Spec{PerTable: true, MaxStatements: 5})
	var kerr *clierr.Error
	if !errors.As(err, &kerr) || kerr.Kind != clierr.KindUsage {
		t.Fatalf("err = %v, want usage kind", err)
	}
}

func TestSplitPreservesUnsplitStatements(t *testing.T) {
	// Splitting is output-layer only: concatenating the parts' statements
	// must yield exactly the monolith's statement stream (ADR 0025's
	// no-content-change contract).
	row := func(id int64) model.RedoRow {
		r := insertRow()
		r.After = []any{id, "a"}
		return r
	}
	events := []model.NormalizedEvent{
		ev("GTID", "k1", "uuid:1"), ev("BEGIN", "k1", "uuid:1"),
		ev("ROWS", "k1", "uuid:1", row(1)), ev("ROWS", "k1", "uuid:1", row(2)),
		ev("XID", "k1", "uuid:1"),
		{EventType: "DDL", TxnKey: "k2", TxnGTID: "uuid:2",
			Schema: "shop", QuerySchema: "shop", QuerySQL: "CREATE TABLE t (id int)"},
	}
	_, files := runSplit(t, splitout.Spec{MaxStatements: 2}, events...)
	var split []string
	for _, f := range files {
		for _, s := range statements(f) {
			if s == "BEGIN;" || s == "COMMIT;" {
				continue
			}
			split = append(split, s)
		}
	}
	_, mono, err := plan(t, Options{}, events...)
	if err != nil {
		t.Fatal(err)
	}
	var whole []string
	for _, s := range statements(mono) {
		if s == "BEGIN;" || s == "COMMIT;" {
			continue
		}
		whole = append(whole, s)
	}
	if strings.Join(split, "\n") != strings.Join(whole, "\n") {
		t.Fatalf("split changed statements:\nsplit:\n%s\nwhole:\n%s", strings.Join(split, "\n"), strings.Join(whole, "\n"))
	}
}
