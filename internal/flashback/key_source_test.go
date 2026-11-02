package flashback

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

// A key taken from the definition is only as good as the dump, so the run says
// where it came from: stderr and --plan (one warning per table), and the script
// header (ADR 0030).
func TestSchemaKeySourceIsAnnounced(t *testing.T) {
	p := New(Options{})
	p.noteSchemaKeyRows([]model.FlashRow{
		{Schema: "shop", Table: "orders", PKFromSchema: true, PK: []int{0}},
		{Schema: "shop", Table: "orders", PKFromSchema: true, PK: []int{0}},
		{Schema: "shop", Table: "users"},
	})

	var got int
	for _, w := range p.WarningList() {
		if w.Code == model.WarnSchemaFileKey && strings.Contains(w.Message, "shop.orders") {
			got++
		}
		if strings.Contains(w.Message, "shop.users") {
			t.Fatalf("a binlog-keyed table must not be announced: %v", w)
		}
	}
	if got != 1 {
		t.Fatalf("want exactly one schema-key warning, got %d: %v", got, p.WarningList())
	}

	notes := p.keySourceNotes()
	if len(notes) != 1 || !strings.Contains(notes[0], "shop.orders") || !strings.Contains(notes[0], "--schema-file") {
		t.Fatalf("script note missing the source: %q", notes)
	}

	var script strings.Builder
	if err := writeFlashbackScript(&script, func(fn func(*flashGroup) error) error { return nil }, notes, nil, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(script.String(), "primary key from --schema-file for shop.orders") {
		t.Fatalf("the script header must name the key source:\n%s", script.String())
	}
}
