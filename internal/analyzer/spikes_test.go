package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestDetectSpikeAlertsUsesRollingBaseline(t *testing.T) {
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{Minute: minute.Add(-2 * time.Minute), TotalRows: 10},
		{Minute: minute.Add(-1 * time.Minute), TotalRows: 12},
		{Minute: minute, TotalRows: 120}, // 10x spike
	}

	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})

	if len(alerts) == 0 {
		t.Fatal("expected spike alert")
	}
	if alerts[0].Type != "spike" {
		t.Fatalf("expected type 'spike', got %s", alerts[0].Type)
	}
}

func TestDetectSpikeAlertsRequiresMinRows(t *testing.T) {
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{Minute: minute.Add(-2 * time.Minute), TotalRows: 10},
		{Minute: minute.Add(-1 * time.Minute), TotalRows: 12},
		{Minute: minute, TotalRows: 120}, // 10x spike but below min rows threshold
	}

	// SpikeMinRows=200 means this spike should NOT trigger
	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 200,
	})

	if len(alerts) != 0 {
		t.Fatalf("expected no spike alert (below min rows), got %d", len(alerts))
	}
}

func TestDetectSpikeAlertsRequiresSpikeFactor(t *testing.T) {
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{Minute: minute.Add(-2 * time.Minute), TotalRows: 10},
		{Minute: minute.Add(-1 * time.Minute), TotalRows: 12},
		{Minute: minute, TotalRows: 50}, // Only ~4x spike, below factor threshold
	}

	// SpikeFactor=10 means this spike should NOT trigger
	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  10.0,
		SpikeMinRows: 20,
	})

	if len(alerts) != 0 {
		t.Fatalf("expected no spike alert (below factor), got %d", len(alerts))
	}
}

func TestDetectSpikeAlertsDisabled(t *testing.T) {
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{Minute: minute.Add(-2 * time.Minute), TotalRows: 10},
		{Minute: minute.Add(-1 * time.Minute), TotalRows: 12},
		{Minute: minute, TotalRows: 10000}, // Huge spike
	}

	// DetectSpikes=false means no detection
	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: false,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})

	if len(alerts) != 0 {
		t.Fatalf("expected no spike alert (disabled), got %d", len(alerts))
	}
}

func TestDetectSpikeAlertsUsesMedianBaseline(t *testing.T) {
	// Median of [10, 100] is 55 (average of sorted middle values)
	// 500 / 55 = ~9x, should trigger with factor=5
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{Minute: minute.Add(-2 * time.Minute), TotalRows: 10},
		{Minute: minute.Add(-1 * time.Minute), TotalRows: 100},
		{Minute: minute, TotalRows: 500},
	}

	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 100,
	})

	if len(alerts) == 0 {
		t.Fatal("expected spike alert with median baseline")
	}
}

func TestDetectSpikeAlertsIncludesDetails(t *testing.T) {
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{Minute: minute.Add(-2 * time.Minute), TotalRows: 10},
		{Minute: minute.Add(-1 * time.Minute), TotalRows: 12},
		{Minute: minute, TotalRows: 120},
	}

	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})

	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}

	details := alerts[0].Details
	if details == nil {
		t.Fatal("expected Details to be populated")
	}

	// Check required fields
	if _, ok := details["rows"].(int); !ok {
		t.Fatalf("expected Details['rows'] int, got %v", details["rows"])
	}
	if _, ok := details["baseline"].(int); !ok {
		t.Fatalf("expected Details['baseline'] int, got %v", details["baseline"])
	}
	if _, ok := details["factor"].(float64); !ok {
		t.Fatalf("expected Details['factor'] float64, got %v", details["factor"])
	}
	if _, ok := details["minute"].(time.Time); !ok {
		t.Fatalf("expected Details['minute'] time.Time, got %v", details["minute"])
	}
}

func TestDetectSpikeAlertsHandlesInsufficientHistory(t *testing.T) {
	// Only 1 bucket of history, which is less than SpikeWindow
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{Minute: minute.Add(-1 * time.Minute), TotalRows: 10},
		{Minute: minute, TotalRows: 1000}, // Huge spike but insufficient history
	}

	// With SpikeWindow=2, we need 2 buckets of history, but only have 1
	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})

	// Skip if insufficient history
	if len(alerts) != 0 {
		t.Fatalf("expected no spike alert (insufficient history), got %d", len(alerts))
	}
}

func TestDetectSpikeAlertsMultipleSpikes(t *testing.T) {
	// Create data where spikes are separated by low baseline
	// Minute 2: baseline=median([10,10])=10, factor=100/10=10x > 5 ✓
	// Minute 5: baseline=median([10,10,10,10])=10, factor=300/10=30x > 5 ✓
	base := time.Date(2026, time.March, 9, 10, 0, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{Minute: base, TotalRows: 10},
		{Minute: base.Add(1 * time.Minute), TotalRows: 10},
		{Minute: base.Add(2 * time.Minute), TotalRows: 100}, // Spike 1
		{Minute: base.Add(3 * time.Minute), TotalRows: 10},
		{Minute: base.Add(4 * time.Minute), TotalRows: 10},
		{Minute: base.Add(5 * time.Minute), TotalRows: 300}, // Spike 2
	}

	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})
    if len(alerts) != 2 {
		t.Fatalf("expected 2 spike alerts, got %d", len(alerts))
    }
}

func TestAnalyzerIntegratesSpikeAlerts(t *testing.T) {
	a := New(Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})
	base := time.Date(2026, time.March, 9, 10, 0, 0, 0, time.UTC)

	// Create events that span multiple minutes with a spike
	events := []model.NormalizedEvent{
		// Minute 1: 10 rows
		{Timestamp: base, EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 10},
		// Minute 2: 10 rows
		{Timestamp: base.Add(1 * time.Minute), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 10},
		// Minute 3: 100 rows (spike!)
		{Timestamp: base.Add(2 * time.Minute), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 100},
	}

	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have at least one spike alert
	spikeCount := 0
	for _, alert := range result.Alerts {
		if alert.Type == "spike" {
			spikeCount++
		}
	}
	if spikeCount == 0 {
		t.Fatal("expected spike alert in analysis result")
	}
}

func TestDetectSpikeAlertsTableLevel(t *testing.T) {
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{
			Minute:    minute.Add(-2 * time.Minute),
			TotalRows: 20,
			TableRows: map[string]int{"shop.orders": 10, "shop.users": 10},
		},
		{
			Minute:    minute.Add(-1 * time.Minute),
			TotalRows: 20,
			TableRows: map[string]int{"shop.orders": 10, "shop.users": 10},
		},
		{
			Minute:    minute,
			TotalRows: 110, // overall spike: 110/20 = 5.5x
			TableRows: map[string]int{"shop.orders": 100, "shop.users": 10}, // orders spike: 100/10 = 10x
		},
	}

	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})

	// Should have both overall spike and table-level spike for shop.orders
	if len(alerts) < 2 {
		t.Fatalf("expected at least 2 alerts (overall + table), got %d", len(alerts))
	}

	// Find table-level spike alert
	var tableAlert *model.Alert
	for i := range alerts {
		if alerts[i].Details["table"] != nil {
			tableAlert = &alerts[i]
			break
		}
	}
	if tableAlert == nil {
		t.Fatal("expected table-level spike alert with 'table' in Details")
	}

	// Verify table-level alert details
	if table, ok := tableAlert.Details["table"].(string); !ok || table != "shop.orders" {
		t.Fatalf("expected table 'shop.orders', got %v", tableAlert.Details["table"])
	}
	if rows, ok := tableAlert.Details["rows"].(int); !ok || rows != 100 {
		t.Fatalf("expected rows 100, got %v", tableAlert.Details["rows"])
	}
	if baseline, ok := tableAlert.Details["baseline"].(int); !ok || baseline != 10 {
		t.Fatalf("expected baseline 10, got %v", tableAlert.Details["baseline"])
	}
	if factor, ok := tableAlert.Details["factor"].(float64); !ok || factor < 5.0 {
		t.Fatalf("expected factor >= 5.0, got %v", tableAlert.Details["factor"])
	}
}

func TestDetectSpikeAlertsTableLevelNoOverallSpike(t *testing.T) {
	// Scenario: table-level spike but no overall spike
	// shop.orders spikes but shop.users drops, so total stays flat
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{
			Minute:    minute.Add(-2 * time.Minute),
			TotalRows: 100,
			TableRows: map[string]int{"shop.orders": 10, "shop.users": 90},
		},
		{
			Minute:    minute.Add(-1 * time.Minute),
			TotalRows: 100,
			TableRows: map[string]int{"shop.orders": 10, "shop.users": 90},
		},
		{
			Minute:    minute,
			TotalRows: 100, // no overall spike
			TableRows: map[string]int{"shop.orders": 90, "shop.users": 10}, // orders spike: 90/10 = 9x
		},
	}

	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})

	// Should NOT have overall spike (TotalRows stayed flat)
	// Should have table-level spike for shop.orders
	var overallCount, tableCount int
	for _, alert := range alerts {
		if alert.Details["table"] == nil {
			overallCount++
		} else {
			tableCount++
		}
	}

	if overallCount != 0 {
		t.Fatalf("expected 0 overall spike alerts, got %d", overallCount)
	}
	if tableCount != 1 {
		t.Fatalf("expected 1 table-level spike alert, got %d", tableCount)
	}
}

func TestDetectSpikeAlertsTableLevelRespectsMinRows(t *testing.T) {
	// Table spike below SpikeMinRows should not trigger
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{
			Minute:    minute.Add(-2 * time.Minute),
			TotalRows: 20,
			TableRows: map[string]int{"shop.orders": 1, "shop.users": 9},
		},
		{
			Minute:    minute.Add(-1 * time.Minute),
			TotalRows: 20,
			TableRows: map[string]int{"shop.orders": 1, "shop.users": 9},
		},
		{
			Minute:    minute,
			TotalRows: 30,
			TableRows: map[string]int{"shop.orders": 20, "shop.users": 10}, // orders: 20/1=20x but only 20 rows
		},
	}

	// SpikeMinRows=50 means shop.orders spike (20 rows) should NOT trigger
	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})

	// Count table-level alerts
	var tableCount int
	for _, alert := range alerts {
		if alert.Details["table"] != nil {
			tableCount++
		}
	}

	if tableCount != 0 {
		t.Fatalf("expected 0 table-level spike alerts (below min rows), got %d", tableCount)
	}
}

func TestDetectSpikeAlertsDistinguishesOverallVsTable(t *testing.T) {
	// Verify that overall and table-level alerts are distinguishable
	minute := time.Date(2026, time.March, 9, 10, 2, 0, 0, time.UTC)
	buckets := []model.MinuteBucket{
		{
			Minute:    minute.Add(-2 * time.Minute),
			TotalRows: 10,
			TableRows: map[string]int{"shop.orders": 10},
		},
		{
			Minute:    minute.Add(-1 * time.Minute),
			TotalRows: 10,
			TableRows: map[string]int{"shop.orders": 10},
		},
		{
			Minute:    minute,
			TotalRows: 100,
			TableRows: map[string]int{"shop.orders": 100},
		},
	}

	alerts := DetectSpikeAlerts(buckets, Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  5.0,
		SpikeMinRows: 50,
	})

	// Should have both overall and table-level alerts
	var overallAlert, tableAlert *model.Alert
	for i := range alerts {
		if alerts[i].Details["table"] == nil {
			overallAlert = &alerts[i]
		} else {
			tableAlert = &alerts[i]
		}
	}

	if overallAlert == nil {
		t.Fatal("expected overall spike alert")
	}
	if tableAlert == nil {
		t.Fatal("expected table-level spike alert")
	}

	// Overall alert should NOT have 'table' key
	if overallAlert.Details["table"] != nil {
		t.Fatal("overall alert should not have 'table' key")
	}

	// Table alert should have 'table' key
	if tableAlert.Details["table"] == nil {
		t.Fatal("table alert should have 'table' key")
	}
}
