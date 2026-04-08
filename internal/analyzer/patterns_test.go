package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestBuildPatternsGroupsTransactionsByStructuralShape(t *testing.T) {
	txns := []model.Transaction{
		{
			TxnKey:       "txn-1",
			StartTime:    time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC),
			EndTime:      time.Date(2026, 4, 9, 10, 0, 2, 0, time.UTC),
			Duration:     2 * time.Second,
			TotalRows:    12,
			EventCount:   2,
			Tables:       map[string]int{"shop.orders": 8, "shop.order_items": 4},
			Operations:   map[string]int{"INSERT": 12},
			QuerySummary: "INSERT INTO orders ...",
		},
		{
			TxnKey:       "txn-2",
			StartTime:    time.Date(2026, 4, 9, 10, 1, 0, 0, time.UTC),
			EndTime:      time.Date(2026, 4, 9, 10, 1, 3, 0, time.UTC),
			Duration:     3 * time.Second,
			TotalRows:    18,
			EventCount:   3,
			Tables:       map[string]int{"shop.order_items": 8, "shop.orders": 10},
			Operations:   map[string]int{"INSERT": 18},
			QuerySummary: "INSERT INTO orders ...",
		},
	}

	patterns := BuildPatterns(txns)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d", len(patterns))
	}
	if patterns[0].TxnCount != 2 {
		t.Fatalf("expected txn_count=2, got %d", patterns[0].TxnCount)
	}
	if patterns[0].TotalRows != 30 {
		t.Fatalf("expected total_rows=30, got %d", patterns[0].TotalRows)
	}
}

func TestBuildPatternsSeparatesDifferentOperationShapes(t *testing.T) {
	txns := []model.Transaction{
		{
			TxnKey:     "txn-1",
			TotalRows:  10,
			EventCount: 2,
			Tables:     map[string]int{"shop.payments": 10},
			Operations: map[string]int{"UPDATE": 10},
		},
		{
			TxnKey:     "txn-2",
			TotalRows:  10,
			EventCount: 2,
			Tables:     map[string]int{"shop.payments": 10},
			Operations: map[string]int{"DELETE": 10},
		},
	}

	patterns := BuildPatterns(txns)
	if len(patterns) != 2 {
		t.Fatalf("expected 2 patterns, got %d", len(patterns))
	}
}

func TestBuildPatternsFallsBackWithoutQuerySummary(t *testing.T) {
	txns := []model.Transaction{
		{
			TxnKey:     "txn-1",
			TotalRows:  4,
			EventCount: 4,
			Tables:     map[string]int{"shop.inventory": 4},
			Operations: map[string]int{"UPDATE": 4},
		},
	}

	patterns := BuildPatterns(txns)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d", len(patterns))
	}
	if patterns[0].Label == "" {
		t.Fatal("expected non-empty fallback label")
	}
}

func TestBuildPatternsSortsByRowsTxnCountAndKey(t *testing.T) {
	txns := []model.Transaction{
		{TxnKey: "txn-a", TotalRows: 30, EventCount: 3, Tables: map[string]int{"a.t1": 30}, Operations: map[string]int{"UPDATE": 30}},
		{TxnKey: "txn-b", TotalRows: 30, EventCount: 3, Tables: map[string]int{"a.t1": 30}, Operations: map[string]int{"UPDATE": 30}},
		{TxnKey: "txn-c", TotalRows: 12, EventCount: 2, Tables: map[string]int{"a.t2": 12}, Operations: map[string]int{"INSERT": 12}},
	}

	patterns := BuildPatterns(txns)
	if len(patterns) != 2 {
		t.Fatalf("expected 2 patterns, got %d", len(patterns))
	}
	if patterns[0].TotalRows < patterns[1].TotalRows {
		t.Fatalf("expected descending total_rows order: %+v", patterns)
	}
}
