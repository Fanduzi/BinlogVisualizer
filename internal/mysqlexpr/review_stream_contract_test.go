package mysqlexpr

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

type streamClashRecord struct {
	col int
	seq uint64
	key string
	lit string
}

func TestStreamReviewLifecycleMatchesBatch(t *testing.T) {
	good := LoggedImage{Columns: []string{"a", "g"}, Values: []string{"1", "2"}}
	bad := LoggedImage{Columns: []string{"a", "g"}, Values: []string{"1", "3"}}
	laterBad := LoggedImage{Columns: []string{"a", "g"}, Values: []string{"2", "5"}}
	missing := LoggedImage{Columns: []string{"g"}, Values: []string{"2"}}
	tests := []struct {
		name        string
		expr        string
		images      []LoggedImage
		wantUnknown bool
		mismatchAt  int
		checkClash  bool
		wantClash   bool
		wantExact   bool
	}{
		{name: "no images", expr: "a+1", wantUnknown: true, mismatchAt: -1},
		{name: "blank expression", expr: "  ", images: []LoggedImage{good}, wantUnknown: true, mismatchAt: -1},
		{name: "verified", expr: "a+1", images: []LoggedImage{good}, mismatchAt: -1, checkClash: true, wantClash: true, wantExact: true},
		{name: "missing base", expr: "a+1", images: []LoggedImage{missing}, wantUnknown: true, mismatchAt: -1},
		{name: "unknown then verified", expr: "a+1", images: []LoggedImage{missing, good}, wantUnknown: true, mismatchAt: -1},
		{name: "mismatch", expr: "a+1", images: []LoggedImage{bad}, mismatchAt: 0, checkClash: true},
		{name: "unknown then mismatch", expr: "a+1", images: []LoggedImage{missing, bad}, mismatchAt: 1},
		{name: "first mismatch wins", expr: "a+1", images: []LoggedImage{bad, laterBad, good}, mismatchAt: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := []SchemaCol{
				{Name: "a", base: "int"},
				{Name: "g", base: "int", Generated: true, Expr: tt.expr},
			}
			review := NewStreamReview(meta)
			if review.Len() != 1 {
				t.Fatalf("generated column count = %d, want 1", review.Len())
			}
			if !reflect.DeepEqual(review.Col(0), meta[1]) {
				t.Fatalf("generated metadata = %+v, want %+v", review.Col(0), meta[1])
			}
			for _, image := range tt.images {
				review.Observe(image)
			}
			review.Finish()
			example, unknown := review.Outcome(0)
			wantExample := ""
			if tt.mismatchAt >= 0 {
				wantExample = review.Example(tt.images[tt.mismatchAt])
				if wantExample == "" {
					t.Fatal("mismatch example must not be empty")
				}
			}
			if example != wantExample || unknown != tt.wantUnknown {
				t.Fatalf("stream outcome = (%q, %t), want (%q, %t)", example, unknown, wantExample, tt.wantUnknown)
			}
			batchExample, batchUnknown := PrepareGenImages(tt.images, meta).Outcome(meta[1])
			if example != batchExample || unknown != batchUnknown {
				t.Fatalf("stream outcome = (%q, %t), batch = (%q, %t)", example, unknown, batchExample, batchUnknown)
			}
			if tt.checkClash && (review.NeedsClash() != tt.wantClash || review.NeedsExactClash(0) != tt.wantExact) {
				t.Fatalf("clash flags = (%t, %t), want (%t, %t)", review.NeedsClash(), review.NeedsExactClash(0), tt.wantClash, tt.wantExact)
			}
		})
	}
}

func TestStreamReviewPreservesGeneratedSchemaOrder(t *testing.T) {
	meta := []SchemaCol{
		{Name: "a", base: "int"},
		{Name: "first", base: "int", Generated: true, Expr: "a+1"},
		{Name: "b", base: "int"},
		{Name: "second", base: "int", Generated: true, Expr: "b+1"},
	}
	review := NewStreamReview(meta)
	if review.Len() != 2 {
		t.Fatalf("generated column count = %d, want 2", review.Len())
	}
	if !reflect.DeepEqual(review.Col(0), meta[1]) || !reflect.DeepEqual(review.Col(1), meta[3]) {
		t.Fatalf("generated schema order = %+v, %+v", review.Col(0), review.Col(1))
	}
}

func TestStreamReviewDependencyRecordsUseCaseFoldedNames(t *testing.T) {
	meta := []SchemaCol{
		{Name: "a", base: "int"},
		{Name: "g", base: "int", Generated: true, Expr: "a+1"},
	}
	first := LoggedImage{Columns: []string{"a", "g"}, Values: []string{"1", "2"}}
	folded := LoggedImage{Columns: []string{"A", "G"}, Values: []string{"1", "2"}}
	review := NewStreamReview(meta)
	review.Observe(first)
	review.Observe(folded)
	review.Finish()
	if !review.NeedsClash() || !review.NeedsExactClash(0) {
		t.Fatal("verified generated column must request exact dependency checks")
	}
	var records []streamClashRecord
	write := func(col int, seq uint64, key, lit string) error {
		records = append(records, streamClashRecord{col, seq, key, lit})
		return nil
	}
	if err := review.NoteClash(7, first, write); err != nil {
		t.Fatal(err)
	}
	if err := review.NoteClash(19, folded, write); err != nil {
		t.Fatal(err)
	}
	want := []streamClashRecord{{0, 7, "1:1\x00", "2"}, {0, 19, "1:1\x00", "2"}}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("dependency records = %#v, want %#v", records, want)
	}
	if err := review.NoteClash(20, LoggedImage{Columns: []string{"g"}, Values: []string{"2"}}, nil); err != nil {
		t.Fatalf("missing dependency: %v", err)
	}
	if err := review.NoteClash(21, LoggedImage{Columns: []string{"a"}, Values: []string{"1"}}, nil); err != nil {
		t.Fatalf("missing generated value: %v", err)
	}
	example, unknown := review.Outcome(0)
	batchExample, batchUnknown := PrepareGenImages([]LoggedImage{first, folded}, meta).Outcome(meta[1])
	if example != "" || unknown || example != batchExample || unknown != batchUnknown {
		t.Fatalf("case-folded outcome = (%q, %t), batch = (%q, %t)", example, unknown, batchExample, batchUnknown)
	}
}

func TestStreamReviewClashWriterErrorStopsBeforeNextColumn(t *testing.T) {
	meta := []SchemaCol{
		{Name: "a", base: "int"},
		{Name: "g", base: "int", Generated: true, Expr: "a+1"},
		{Name: "h", base: "int", Generated: true, Expr: "a+1"},
	}
	image := LoggedImage{Columns: []string{"a", "g", "h"}, Values: []string{"1", "2", "2"}}
	review := NewStreamReview(meta)
	review.Observe(image)
	review.Finish()
	if !review.NeedsExactClash(0) || !review.NeedsExactClash(1) {
		t.Fatal("both generated columns must need dependency records")
	}
	sentinel := errors.New("record write failed")
	var calls []streamClashRecord
	err := review.NoteClash(42, image, func(col int, seq uint64, key, lit string) error {
		calls = append(calls, streamClashRecord{col, seq, key, lit})
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("NoteClash error = %v, want sentinel", err)
	}
	if want := []streamClashRecord{{0, 42, "1:1\x00", "2"}}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("writer calls = %#v, want %#v", calls, want)
	}
}

func TestStreamReviewUnknownImagesStillDetectDependencyClash(t *testing.T) {
	meta := []SchemaCol{
		{Name: "a", base: "int"},
		{Name: "g", base: "int", Generated: true, Expr: "unsupported(a)"},
	}
	first := LoggedImage{Columns: []string{"a", "g"}, Values: []string{"1", "2"}}
	second := LoggedImage{Columns: []string{"a", "g"}, Values: []string{"1", "3"}}
	for _, image := range []LoggedImage{first, second} {
		review := NewStreamReview(meta)
		review.Observe(image)
		review.Finish()
		example, unknown := review.Outcome(0)
		if example != "" || !unknown || !review.NeedsClash() || !review.NeedsExactClash(0) || review.NeedsDiv(0) {
			t.Fatalf("single unsupported image: outcome (%q, %t), clash %t, exact %t, div %t",
				example, unknown, review.NeedsClash(), review.NeedsExactClash(0), review.NeedsDiv(0))
		}
	}
	review := NewStreamReview(meta)
	review.Observe(first)
	review.Observe(second)
	review.Finish()
	for _, invalid := range []int{-1, 1, 99} {
		review.SetSawClash(invalid)
		review.SetExactClash(invalid, "ignored", "ignored")
		if review.NeedsExactClash(invalid) || review.NeedsDiv(invalid) {
			t.Fatalf("invalid column %d needs work", invalid)
		}
	}
	if example, unknown := review.Outcome(0); example != "" || !unknown || !review.NeedsExactClash(0) {
		t.Fatalf("invalid setters changed outcome: (%q, %t)", example, unknown)
	}
	var records []streamClashRecord
	write := func(col int, seq uint64, key, lit string) error {
		records = append(records, streamClashRecord{col, seq, key, lit})
		return nil
	}
	if err := review.NoteClash(10, first, write); err != nil {
		t.Fatal(err)
	}
	if err := review.NoteClash(11, second, write); err != nil {
		t.Fatal(err)
	}
	if want := []streamClashRecord{{0, 10, "1:1\x00", "2"}, {0, 11, "1:1\x00", "3"}}; !reflect.DeepEqual(records, want) {
		t.Fatalf("clash records = %#v, want %#v", records, want)
	}
	review.SetExactClash(0, review.Example(first), "3")
	example, unknown := review.Outcome(0)
	if example == "" || unknown || !strings.Contains(example, "g=3") {
		t.Fatalf("clash outcome = (%q, %t)", example, unknown)
	}
	batchExample, batchUnknown := PrepareGenImages([]LoggedImage{first, second}, meta).Outcome(meta[1])
	if example != batchExample || unknown != batchUnknown {
		t.Fatalf("stream clash = (%q, %t), batch = (%q, %t)", example, unknown, batchExample, batchUnknown)
	}
}

func TestStreamReviewMismatchPrecedesLaterExactClash(t *testing.T) {
	meta := []SchemaCol{
		{Name: "a", base: "int"},
		{Name: "g", base: "int", Generated: true, Expr: "a+1"},
	}
	image := LoggedImage{Columns: []string{"a", "g"}, Values: []string{"1", "3"}}
	review := NewStreamReview(meta)
	review.Observe(image)
	review.Finish()
	want := review.Example(image)
	if want == "" || review.NeedsClash() || review.NeedsExactClash(0) {
		t.Fatalf("mismatch example %q or dependency flags are incorrect", want)
	}
	review.SetExactClash(0, "later clash", "4")
	example, unknown := review.Outcome(0)
	batchExample, batchUnknown := PrepareGenImages([]LoggedImage{image}, meta).Outcome(meta[1])
	if example != want || unknown || example != batchExample || unknown != batchUnknown {
		t.Fatalf("mismatch after clash = (%q, %t), want %q and batch (%q, %t)", example, unknown, want, batchExample, batchUnknown)
	}
}

func TestStreamReviewDivisionSweepMatchesBatch(t *testing.T) {
	meta := []SchemaCol{
		{Name: "a", base: "bigint"},
		{Name: "b", base: "bigint"},
		{Name: "g", base: "decimal", prec: 12, scale: 4, hasPrec: true, Generated: true, Expr: "(`a` / `b`)"},
	}
	columns := []string{"a", "b", "g"}
	images := []LoggedImage{
		{Columns: columns, Values: []string{"-616", "-374", "1.0000"}},
		{Columns: columns, Values: []string{"7", "3", "2.0000"}},
		{Columns: columns, Values: []string{"10", "4", "2.0000"}},
	}
	review := NewStreamReview(meta)
	for _, image := range images {
		review.Observe(image)
	}
	review.Finish()
	if !review.NeedsClash() || review.NeedsExactClash(0) || !review.NeedsDiv(0) || !review.NeedsAnyDiv() {
		t.Fatalf("division flags: clash %t exact %t div %t any %t",
			review.NeedsClash(), review.NeedsExactClash(0), review.NeedsDiv(0), review.NeedsAnyDiv())
	}
	if fit, unchecked := review.DivIncrementFit(0); fit != nil || unchecked != nil {
		t.Fatalf("unfinished sweep = %v, %v", fit, unchecked)
	}
	var records []streamClashRecord
	for i, image := range images {
		err := review.NoteClash(uint64(i), image, func(col int, seq uint64, key, lit string) error {
			records = append(records, streamClashRecord{col, seq, key, lit})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	wantRecords := []streamClashRecord{
		{0, 0, "4:-616\x004:-374\x00", "1.0000"},
		{0, 1, "1:7\x001:3\x00", "2.0000"},
		{0, 2, "2:10\x001:4\x00", "2.0000"},
	}
	if !reflect.DeepEqual(records, wantRecords) {
		t.Fatalf("division dependency records = %#v, want %#v", records, wantRecords)
	}
	for _, image := range images {
		review.NoteDiv(image)
		review.NoteDiv(image)
	}
	review.MarkDivDone()
	fit, unchecked := review.DivIncrementFit(0)
	// The 9-digit word expansion exceeds the checker's 30-digit scale bound at increments 28-30.
	if !reflect.DeepEqual(fit, []int{0}) || !reflect.DeepEqual(unchecked, []int{28, 29, 30}) {
		t.Fatalf("division fit = %v, unchecked = %v; want [0], [28 29 30]", fit, unchecked)
	}
	batch := PrepareGenImages(images, meta)
	batchFit, batchUnchecked := batch.DivIncrementFit(meta[2])
	if !reflect.DeepEqual(fit, batchFit) || !reflect.DeepEqual(unchecked, batchUnchecked) {
		t.Fatalf("stream fit = %v, %v; batch = %v, %v", fit, unchecked, batchFit, batchUnchecked)
	}
	if review.NeedsAnyDiv() || review.NeedsDiv(0) {
		t.Fatal("finished division sweep still needs work")
	}
	example, unknown := review.Outcome(0)
	batchExample, batchUnknown := batch.Outcome(meta[2])
	if example == "" || example != review.Example(images[0]) || unknown || example != batchExample || unknown != batchUnknown {
		t.Fatalf("division remains a mismatch: stream (%q, %t), batch (%q, %t)", example, unknown, batchExample, batchUnknown)
	}
	review.SetSawClash(0)
	if fit, unchecked := review.DivIncrementFit(0); fit != nil || unchecked != nil {
		t.Fatalf("clash must suppress fits: %v, %v", fit, unchecked)
	}
}

func TestStreamReviewClashStopsPendingDivision(t *testing.T) {
	meta := []SchemaCol{
		{Name: "a", base: "bigint"},
		{Name: "b", base: "bigint"},
		{Name: "g", base: "decimal", prec: 12, scale: 4, hasPrec: true, Generated: true, Expr: "(`a` / `b`)"},
	}
	image := LoggedImage{Columns: []string{"a", "b", "g"}, Values: []string{"-616", "-374", "1.0000"}}
	review := NewStreamReview(meta)
	review.Observe(image)
	review.Finish()
	if !review.NeedsDiv(0) || !review.NeedsAnyDiv() {
		t.Fatal("default-increment mismatch must request a division sweep")
	}
	review.SetSawClash(0)
	if review.NeedsDiv(0) || review.NeedsAnyDiv() {
		t.Fatal("dependency clash must stop the pending division sweep")
	}
	review.NoteDiv(image)
	review.MarkDivDone()
	if fit, unchecked := review.DivIncrementFit(0); fit != nil || unchecked != nil {
		t.Fatalf("clashing column produced fit %v and unchecked %v", fit, unchecked)
	}
}

func TestStreamReviewNilAndZeroValue(t *testing.T) {
	image := LoggedImage{Columns: []string{"A", "a"}, Values: []string{"1", "1"}}
	var review *StreamReview
	review.Observe(image)
	review.Finish()
	if review.NeedsClash() || review.NeedsExactClash(0) || review.NeedsDiv(0) || review.NeedsAnyDiv() {
		t.Fatal("nil review must not need clash or division work")
	}
	if err := review.NoteClash(1, image, nil); err != nil {
		t.Fatalf("nil review clash: %v", err)
	}
	review.NoteDiv(image)
	review.MarkDivDone()
	if review.Len() != 0 || review.Example(image) != "" {
		t.Fatal("nil review must have no columns or example")
	}
	review.SetSawClash(0)
	review.SetExactClash(0, "example", "1")

	var zero StreamReview
	zero.Observe(image)
	zero.Observe(LoggedImage{Columns: []string{"a", "B"}, Values: []string{"1", "2"}})
	zero.Finish()
	if zero.Len() != 0 || zero.NeedsClash() || zero.NeedsAnyDiv() {
		t.Fatal("zero-value review must not invent generated columns")
	}
	if err := zero.NoteClash(2, image, nil); err != nil {
		t.Fatalf("zero-value review clash: %v", err)
	}
}
