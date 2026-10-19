package flashback

import (
	"errors"
	"testing"

	"binlogviz/internal/model"
)

func TestSpillLimitIsResource(t *testing.T) {
	p := New(Options{})
	p.spillLimit = 1
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "DELETE",
		Columns: []string{"id"}, Before: []string{"1"}, PK: []int{0},
	}
	ev := func(key string) model.NormalizedEvent {
		return model.NormalizedEvent{
			EventType: "ROWS", Schema: "shop", Table: "t", TxnKey: key,
			Operation: "DELETE", FlashRows: []model.FlashRow{row},
		}
	}
	p.Note(ev("a"))
	p.Note(ev("b"))
	var got *ResourceError
	if !errors.As(p.flashErr, &got) || got.Reason != "spill_limit" {
		t.Fatalf("err = %v", p.flashErr)
	}
}

func TestByteCeilingRejectsGarbage(t *testing.T) {
	for _, v := range []string{"abc", "0", "-1", "1MB", ""} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("BINLOGVIZ_SPILL_BYTES", v)
			if _, err := parseByteCeiling("BINLOGVIZ_SPILL_BYTES"); err == nil {
				t.Fatalf("accepted %q", v)
			}
		})
	}
	n, err := parseByteCeiling("BINLOGVIZ_NOT_A_REAL_CEILING")
	if err != nil || n != 0 {
		t.Fatalf("unset = %d %v", n, err)
	}
	t.Setenv("BINLOGVIZ_MEMORY_BYTES", "4096")
	n, err = parseByteCeiling("BINLOGVIZ_MEMORY_BYTES")
	if err != nil || n != 4096 {
		t.Fatalf("memory = %d %v", n, err)
	}
}

func TestMemoryLimitIsResource(t *testing.T) {
	p := New(Options{})
	p.memLimit = 1
	err := p.checkMemory()
	var got *ResourceError
	if !errors.As(err, &got) || got.Reason != "memory_limit" || got.Used <= 0 {
		t.Fatalf("err = %v", err)
	}
}
