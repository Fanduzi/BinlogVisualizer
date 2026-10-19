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

func TestMemoryLimitIsResource(t *testing.T) {
	p := New(Options{})
	p.memLimit = 1
	err := p.checkMemory()
	var got *ResourceError
	if !errors.As(err, &got) || got.Reason != "memory_limit" || got.Used <= 0 {
		t.Fatalf("err = %v", err)
	}
}
