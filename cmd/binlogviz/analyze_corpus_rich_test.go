// Package binlogviz validates richer DBA-oriented analyze corpus scenarios beyond the baseline product checks.
// input: deterministic SQL corpus scenarios covering mixed event bursts, DDL, multi-file evidence, and transaction rankings.
// output: regression coverage that stronger corpus fixtures surface operation mix, hot-table skew, and distinct largest/longest/widest evidence.
// pos: command-layer product tests that harden analyze semantics against toy-only corpora.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestAnalyzeCorpusCoversExtendedScenarios(t *testing.T) {
	for _, scenario := range []string{
		"event-mix-burst",
		"incident-mixed",
	} {
		t.Run(scenario, func(t *testing.T) {
			if len(loadCorpusEvents(t, scenario)) == 0 {
				t.Fatalf("scenario %s has no normalized events", scenario)
			}
		})
	}
}

func TestAnalyzeCorpusEventMixBurstBuildsOperationSeriesAndHotTableSkew(t *testing.T) {
	result := analyzeCorpus(t, "event-mix-burst")

	if maxSeriesValue(result.Timeseries.InsertEventSeries) == 0 {
		t.Fatal("expected non-zero insert event series")
	}
	if maxSeriesValue(result.Timeseries.UpdateEventSeries) == 0 {
		t.Fatal("expected non-zero update event series")
	}
	if maxSeriesValue(result.Timeseries.DeleteEventSeries) == 0 {
		t.Fatal("expected non-zero delete event series")
	}
	if len(result.Diagnostics.HotIntervals) == 0 {
		t.Fatal("expected hot intervals for mixed burst corpus")
	}

	auditLogs, ok := findTableStats(result.Tables, "shop", "audit_logs")
	if !ok {
		t.Fatal("expected audit_logs table stats")
	}
	orders, ok := findTableStats(result.Tables, "shop", "orders")
	if !ok {
		t.Fatal("expected orders table stats")
	}
	if auditLogs.TotalRows <= orders.TotalRows {
		t.Fatalf("expected audit_logs rows %d to exceed orders rows %d", auditLogs.TotalRows, orders.TotalRows)
	}
}

func TestAnalyzeCorpusIncidentMixedSeparatesLargestLongestAndWidest(t *testing.T) {
	result := analyzeCorpus(t, "incident-mixed")

	if len(result.Diagnostics.DDLEvents) < 2 {
		t.Fatalf("expected at least two DDL events, got %d", len(result.Diagnostics.DDLEvents))
	}
	if len(result.Diagnostics.LargestTransactions) == 0 {
		t.Fatal("expected largest transaction evidence")
	}
	if len(result.Diagnostics.LongestTransactions) == 0 {
		t.Fatal("expected longest transaction evidence")
	}
	if len(result.Diagnostics.WidestTransactions) == 0 {
		t.Fatal("expected widest transaction evidence")
	}

	largest := result.Diagnostics.LargestTransactions[0]
	longest := result.Diagnostics.LongestTransactions[0]
	widest := result.Diagnostics.WidestTransactions[0]

	if largest.TotalRows < 8000 {
		t.Fatalf("expected largest transaction >= 8000 rows, got %d", largest.TotalRows)
	}
	if longest.Duration < 90*time.Second {
		t.Fatalf("expected longest transaction >= 90s, got %s", longest.Duration)
	}
	if len(widest.Tables) < 4 {
		t.Fatalf("expected widest transaction touching >= 4 tables, got %d", len(widest.Tables))
	}
	if widest.BinlogPathStart == "" || widest.BinlogPathEnd == "" || widest.BinlogPathStart == widest.BinlogPathEnd {
		t.Fatalf("expected widest transaction to span multiple binlog files, got %q -> %q", widest.BinlogPathStart, widest.BinlogPathEnd)
	}
	if largest.TxnKey == longest.TxnKey || largest.TxnKey == widest.TxnKey || longest.TxnKey == widest.TxnKey {
		t.Fatalf("expected distinct top transactions, got largest=%s longest=%s widest=%s", largest.TxnKey, longest.TxnKey, widest.TxnKey)
	}
}

func maxSeriesValue(points []model.TimeseriesPoint) float64 {
	max := 0.0
	for _, point := range points {
		if point.Value > max {
			max = point.Value
		}
	}
	return max
}

func findTableStats(tables []model.TableStats, schema, table string) (model.TableStats, bool) {
	for _, item := range tables {
		if item.Schema == schema && item.Table == table {
			return item, true
		}
	}
	return model.TableStats{}, false
}
