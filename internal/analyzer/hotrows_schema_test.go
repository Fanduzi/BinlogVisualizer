package analyzer

import (
	"testing"

	"binlogviz/internal/model"
)

// A key from --schema-file ranks like a metadata key and says where it came
// from; without the flag the table stays unavailable (ADR 0032).
func TestHotRowsUseSchemaFileKeys(t *testing.T) {
	ev := model.NormalizedEvent{
		Operation: "UPDATE", Schema: "shop", Table: "t",
		KeyStatus: model.KeyStatusUnknown,
		RowKeys:   []string{"id=7", "id=9"},
	}

	a := newHotRowAggregator(10)
	a.Consume(ev)
	snap := a.Snapshot()
	if len(snap.Rows) != 0 || len(snap.Unavailable) != 1 || snap.KeySource != "" {
		t.Fatalf("without the flag the table stays unavailable: %+v", snap)
	}

	ev.RowKeysFromSchema = true
	a = newHotRowAggregator(10)
	a.Consume(ev)
	snap = a.Snapshot()
	if len(snap.Rows) != 2 || len(snap.Unavailable) != 0 {
		t.Fatalf("definition keys must rank: %+v", snap)
	}
	if snap.KeySource != model.HotRowKeySourceSchemaFile {
		t.Fatalf("the report must say the keys came from the definition: %q", snap.KeySource)
	}

	// key_status and the no-key alert stay metadata-only: a binlog that says a
	// table has no key is not re-decided by a definition.
	noPK := ev
	noPK.KeyStatus = model.KeyStatusNoPK
	a = newHotRowAggregator(10)
	a.Consume(noPK)
	if snap := a.Snapshot(); len(snap.Rows) != 0 {
		t.Fatalf("a binlog-declared no-key table stays out: %+v", snap)
	}
}
