package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestDetectLargeTransactionAlertsOnRowsThreshold(t *testing.T) {
	transactions := []model.Transaction{
		{
			TxnKey:    "t-small",
			TotalRows: 100,
			Duration:  1 * time.Second,
		},
		{
			TxnKey:    "t-big",
			TotalRows: 1500,
			Duration:  1 * time.Second,
		},
	}

	alerts := DetectLargeTransactionAlerts(transactions, Options{LargeTxnRows: 1000})
	if len(alerts) == 0 {
		t.Fatal("expected large transaction alert for t-big")
	}
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	if alerts[0].TxnKey != "t-big" {
		t.Fatalf("expected alert for t-big, got %s", alerts[0].TxnKey)
	}
	if alerts[0].Type != "large_transaction" {
		t.Fatalf("expected type 'large_transaction', got %s", alerts[0].Type)
	}
}

func TestDetectLargeTransactionAlertsOnDurationThreshold(t *testing.T) {
	transactions := []model.Transaction{
		{
			TxnKey:    "t-fast",
			TotalRows: 10,
			Duration:  1 * time.Second,
		},
		{
			TxnKey:    "t-slow",
			TotalRows: 10,
			Duration:  60 * time.Second,
		},
	}

	alerts := DetectLargeTransactionAlerts(transactions, Options{LargeTxnDuration: 30 * time.Second})
	if len(alerts) == 0 {
		t.Fatal("expected large transaction alert for t-slow")
	}
	if alerts[0].TxnKey != "t-slow" {
		t.Fatalf("expected alert for t-slow, got %s", alerts[0].TxnKey)
	}
}

func TestDetectLargeTransactionAlertsOnBothThresholds(t *testing.T) {
	// Transaction triggers BOTH thresholds
	transactions := []model.Transaction{
		{
			TxnKey:    "t-huge",
			TotalRows: 5000,
			Duration:  120 * time.Second,
		},
	}

	alerts := DetectLargeTransactionAlerts(transactions, Options{
		LargeTxnRows:     1000,
		LargeTxnDuration: 30 * time.Second,
	})

	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert (not 2), got %d", len(alerts))
	}

	// Details should indicate which thresholds were triggered
	if alerts[0].Details == nil {
		t.Fatal("expected Details to be populated")
	}
	rows, ok := alerts[0].Details["rows"].(int)
	if !ok || rows != 5000 {
		t.Fatalf("expected Details['rows'] = 5000, got %v", alerts[0].Details["rows"])
	}
	duration, ok := alerts[0].Details["duration_ms"].(int64)
	if !ok || duration != 120000 {
		t.Fatalf("expected Details['duration_ms'] = 120000, got %v", alerts[0].Details["duration_ms"])
	}
}

func TestDetectLargeTransactionAlertsIncludesTableInfo(t *testing.T) {
	transactions := []model.Transaction{
		{
			TxnKey:    "t-multi-table",
			TotalRows: 2000,
			Duration:  5 * time.Second,
			Tables: map[string]int{
				"shop.orders": 1500,
				"shop.users":  500,
			},
		},
	}

	alerts := DetectLargeTransactionAlerts(transactions, Options{LargeTxnRows: 1000})
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}

	// Should include affected tables
	tables, ok := alerts[0].Details["tables"].([]string)
	if !ok || len(tables) != 2 {
		t.Fatalf("expected Details['tables'] with 2 entries, got %v", alerts[0].Details["tables"])
	}
}

func TestDetectLargeTransactionAlertsNoneTriggered(t *testing.T) {
	transactions := []model.Transaction{
		{
			TxnKey:    "t-ok",
			TotalRows: 100,
			Duration:  5 * time.Second,
		},
	}

	alerts := DetectLargeTransactionAlerts(transactions, Options{
		LargeTxnRows:     1000,
		LargeTxnDuration: 30 * time.Second,
	})

	if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}
}

func TestDetectLargeTransactionAlertsDisabledThresholds(t *testing.T) {
	// When thresholds are zero (disabled), no alerts should be generated
	transactions := []model.Transaction{
		{
			TxnKey:    "t-huge",
			TotalRows: 100000,
			Duration:  1 * time.Hour,
		},
	}

	alerts := DetectLargeTransactionAlerts(transactions, Options{
		LargeTxnRows:     0, // disabled
		LargeTxnDuration: 0, // disabled
	})

	if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts when thresholds disabled, got %d", len(alerts))
	}
}

func TestDetectLargeTransactionAlertsMultipleTransactions(t *testing.T) {
	transactions := []model.Transaction{
		{TxnKey: "t1", TotalRows: 2000, Duration: 1 * time.Second},
		{TxnKey: "t2", TotalRows: 50, Duration: 1 * time.Second},
		{TxnKey: "t3", TotalRows: 3000, Duration: 1 * time.Second},
	}

	alerts := DetectLargeTransactionAlerts(transactions, Options{LargeTxnRows: 1000})

	if len(alerts) != 2 {
		t.Fatalf("expected 2 alerts (t1, t3), got %d", len(alerts))
	}

	// Alerts should be ordered by transaction key or some deterministic order
	txnKeys := make(map[string]bool)
	for _, a := range alerts {
		txnKeys[a.TxnKey] = true
	}
	if !txnKeys["t1"] || !txnKeys["t3"] {
		t.Fatal("expected alerts for t1 and t3")
	}
}

func TestAnalyzerIntegratesLargeTransactionAlerts(t *testing.T) {
	a := New(Options{LargeTxnRows: 1000})
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)

	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1500},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Alerts) == 0 {
		t.Fatal("expected alert in analysis result")
	}
	if result.Alerts[0].Type != "large_transaction" {
		t.Fatalf("expected large_transaction alert, got %s", result.Alerts[0].Type)
	}
}
