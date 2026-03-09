package analyzer

import (
	"testing"

	"binlogviz/internal/model"
)

func TestTableAggregatorTracksRowsAndOperations(t *testing.T) {
	agg := NewTableAggregator()

	agg.Consume(model.NormalizedEvent{
		Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2, TxnKey: "t1",
	})
	agg.Consume(model.NormalizedEvent{
		Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 1, TxnKey: "t1",
	})

	stats := agg.Snapshot()
	if len(stats) != 1 {
		t.Fatalf("expected 1 table, got %d", len(stats))
	}

	s := stats[0]
	if s.Schema != "shop" || s.Table != "orders" {
		t.Fatalf("unexpected schema/table: %s.%s", s.Schema, s.Table)
	}
	if s.TotalRows != 3 {
		t.Fatalf("expected 3 total rows, got %d", s.TotalRows)
	}
	if s.InsertRows != 2 {
		t.Fatalf("expected 2 insert rows, got %d", s.InsertRows)
	}
	if s.UpdateRows != 1 {
		t.Fatalf("expected 1 update row, got %d", s.UpdateRows)
	}
	// Same transaction touched this table once
	if s.TxnCount != 1 {
		t.Fatalf("expected 1 distinct transaction, got %d", s.TxnCount)
	}
}

func TestTableAggregatorCountsDistinctTransactions(t *testing.T) {
	agg := NewTableAggregator()

	// Multiple events from same transaction
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 1, TxnKey: "t1"})

	// Event from different transaction
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3, TxnKey: "t2"})

	stats := agg.Snapshot()
	if len(stats) != 1 {
		t.Fatalf("expected 1 table, got %d", len(stats))
	}

	// Two distinct transactions touched this table
	if stats[0].TxnCount != 2 {
		t.Fatalf("expected 2 distinct transactions, got %d", stats[0].TxnCount)
	}
}

func TestTableAggregatorHandlesMultipleTables(t *testing.T) {
	agg := NewTableAggregator()

	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "users", Operation: "INSERT", RowCount: 3, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "DELETE", RowCount: 2, TxnKey: "t2"})

	stats := agg.Snapshot()
	if len(stats) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(stats))
	}

	// Find orders table
	var orders, users *model.TableStats
	for i := range stats {
		if stats[i].Table == "orders" {
			orders = &stats[i]
		} else if stats[i].Table == "users" {
			users = &stats[i]
		}
	}

	if orders == nil || users == nil {
		t.Fatal("expected to find both orders and users tables")
	}

	// orders: 5 INSERT + 2 DELETE = 7 total, 2 transactions
	if orders.TotalRows != 7 {
		t.Fatalf("expected orders 7 total rows, got %d", orders.TotalRows)
	}
	if orders.TxnCount != 2 {
		t.Fatalf("expected orders 2 transactions, got %d", orders.TxnCount)
	}

	// users: 3 INSERT = 3 total, 1 transaction
	if users.TotalRows != 3 {
		t.Fatalf("expected users 3 total rows, got %d", users.TotalRows)
	}
	if users.TxnCount != 1 {
		t.Fatalf("expected users 1 transaction, got %d", users.TxnCount)
	}
}

func TestTableAggregatorProvidesDeterministicSorting(t *testing.T) {
	agg := NewTableAggregator()

	// Insert in non-alphabetical order
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "zebra", Operation: "INSERT", RowCount: 1, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "app", Table: "alpha", Operation: "INSERT", RowCount: 1, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "apple", Operation: "INSERT", RowCount: 1, TxnKey: "t1"})

	stats := agg.Snapshot()
	if len(stats) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(stats))
	}

	// Should be sorted by TotalRows DESC, then Schema ASC, then Table ASC
	// All have same TotalRows, so should be alphabetical by schema.table
	if stats[0].Schema != "app" || stats[0].Table != "alpha" {
		t.Fatalf("expected first table to be app.alpha, got %s.%s", stats[0].Schema, stats[0].Table)
	}
	if stats[1].Schema != "shop" || stats[1].Table != "apple" {
		t.Fatalf("expected second table to be shop.apple, got %s.%s", stats[1].Schema, stats[1].Table)
	}
	if stats[2].Schema != "shop" || stats[2].Table != "zebra" {
		t.Fatalf("expected third table to be shop.zebra, got %s.%s", stats[2].Schema, stats[2].Table)
	}
}

func TestTableAggregatorSortsByTotalRowsDescending(t *testing.T) {
	agg := NewTableAggregator()

	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "small", Operation: "INSERT", RowCount: 5, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "large", Operation: "INSERT", RowCount: 100, TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "medium", Operation: "INSERT", RowCount: 50, TxnKey: "t1"})

	stats := agg.Snapshot()
	if len(stats) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(stats))
	}

	// Should be sorted by TotalRows DESC: large(100) > medium(50) > small(5)
	if stats[0].Table != "large" {
		t.Fatalf("expected first table to be large (100 rows), got %s", stats[0].Table)
	}
	if stats[1].Table != "medium" {
		t.Fatalf("expected second table to be medium (50 rows), got %s", stats[1].Table)
	}
	if stats[2].Table != "small" {
		t.Fatalf("expected third table to be small (5 rows), got %s", stats[2].Table)
	}
}

func TestTableAggregatorHandlesDeleteOperation(t *testing.T) {
	agg := NewTableAggregator()

	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "DELETE", RowCount: 10, TxnKey: "t1"})

	stats := agg.Snapshot()
	if len(stats) != 1 {
		t.Fatalf("expected 1 table, got %d", len(stats))
	}

	if stats[0].DeleteRows != 10 {
		t.Fatalf("expected 10 delete rows, got %d", stats[0].DeleteRows)
	}
	if stats[0].TotalRows != 10 {
		t.Fatalf("expected 10 total rows, got %d", stats[0].TotalRows)
	}
}

func TestTableAggregatorIgnoresNonRowEvents(t *testing.T) {
	agg := NewTableAggregator()

	// Non-row events with schema/table but no row mutation operation
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", EventType: "TABLE_MAP", TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", EventType: "BEGIN", TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", EventType: "COMMIT", TxnKey: "t1"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", EventType: "XID", TxnKey: "t1"})

	// No tables should be created
	stats := agg.Snapshot()
	if len(stats) != 0 {
		t.Fatalf("expected 0 tables (non-row events should be ignored), got %d", len(stats))
	}
}

func TestTableAggregatorIgnoresEmptyOperation(t *testing.T) {
	agg := NewTableAggregator()

	// Event with empty Operation (e.g., control event with schema/table populated)
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "", RowCount: 5, TxnKey: "t1"})

	stats := agg.Snapshot()
	if len(stats) != 0 {
		t.Fatalf("expected 0 tables (empty operation should be ignored), got %d", len(stats))
	}
}

func TestTableAggregatorDoesNotCountTxnFromNonRowEvents(t *testing.T) {
	agg := NewTableAggregator()

	// Real row event
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5, TxnKey: "t1"})

	// Non-row event with same transaction key - should NOT increment TxnCount
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", EventType: "TABLE_MAP", TxnKey: "t2"})
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", EventType: "BEGIN", TxnKey: "t3"})

	// Another row event from different transaction
	agg.Consume(model.NormalizedEvent{Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 2, TxnKey: "t4"})

	stats := agg.Snapshot()
	if len(stats) != 1 {
		t.Fatalf("expected 1 table, got %d", len(stats))
	}

	// Only t1 and t4 should be counted (2 distinct transactions)
	if stats[0].TxnCount != 2 {
		t.Fatalf("expected 2 distinct transactions, got %d", stats[0].TxnCount)
	}
}
