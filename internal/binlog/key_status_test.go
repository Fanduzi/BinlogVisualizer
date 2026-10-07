package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
)

func TestTableKeyStatus(t *testing.T) {
	tests := []struct {
		name  string
		table *replication.TableMapEvent
		want  string
	}{
		{name: "nil", want: model.KeyStatusUnknown},
		{
			name: "minimal signedness only",
			table: &replication.TableMapEvent{
				ColumnCount:      1,
				SignednessBitmap: []byte{0x80},
			},
			want: model.KeyStatusUnknown,
		},
		{
			name: "full simple primary key",
			table: &replication.TableMapEvent{
				ColumnCount: 2,
				ColumnName:  [][]byte{[]byte("id"), []byte("note")},
				PrimaryKey:  []uint64{0},
			},
			want: model.KeyStatusHasPK,
		},
		{
			name: "full prefix primary key",
			table: &replication.TableMapEvent{
				ColumnCount:      2,
				ColumnName:       [][]byte{[]byte("sku"), []byte("note")},
				PrimaryKey:       []uint64{0},
				PrimaryKeyPrefix: []uint64{8},
			},
			want: model.KeyStatusHasPK,
		},
		{
			name: "full without primary key",
			table: &replication.TableMapEvent{
				ColumnCount: 2,
				ColumnName:  [][]byte{[]byte("id"), []byte("note")},
			},
			want: model.KeyStatusNoPK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tableKeyStatus(tt.table); got != tt.want {
				t.Fatalf("status=%s, want %s", got, tt.want)
			}
		})
	}
}

func TestParseNoPKFixtureKeyStatus(t *testing.T) {
	full := keyStatusByTable(t, "testdata/mysql-8.0.46-no-pk-full.binlog")
	if full["shop.orders"] != model.KeyStatusHasPK || full["shop.prefixed"] != model.KeyStatusHasPK {
		t.Fatalf("FULL pk tables = %v", full)
	}
	for _, name := range []string{"shop.heap", "shop.log", "shop.scratch"} {
		if full[name] != model.KeyStatusNoPK {
			t.Fatalf("FULL %s = %s, want no_pk (%v)", name, full[name], full)
		}
	}

	minimal := keyStatusByTable(t, "testdata/mysql-8.0.46-no-pk-minimal.binlog")
	for name, status := range minimal {
		if status != model.KeyStatusUnknown {
			t.Fatalf("MINIMAL %s = %s, want unknown", name, status)
		}
	}
	if len(minimal) != 5 {
		t.Fatalf("MINIMAL tables = %v", minimal)
	}
}

func keyStatusByTable(t *testing.T, path string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := NewParser().ParseFiles([]string{path}, func(raw RawEvent) error {
		if raw.EventType != kindWriteRows && raw.EventType != kindUpdateRows && raw.EventType != kindDeleteRows {
			return nil
		}
		name := raw.Schema + "." + raw.Table
		if prev, ok := out[name]; ok && prev != raw.KeyStatus {
			t.Fatalf("%s key status changed from %s to %s", name, prev, raw.KeyStatus)
		}
		out[name] = raw.KeyStatus
		return nil
	})
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return out
}
