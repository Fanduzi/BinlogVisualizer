package analyzer

import (
	"testing"

	"binlogviz/internal/model"
)

func assertFilteredWorkloadDiagnostics(t *testing.T, result *model.AnalysisResult) {
	t.Helper()
	if len(result.Patterns) != 1 || len(result.Patterns[0].Tables) != 1 || result.Patterns[0].Tables["dogfood.audit_log"] != 2000 {
		t.Fatalf("filtered patterns = %+v, want only dogfood.audit_log", result.Patterns)
	}
	if len(result.Minutes) != 1 || result.Minutes[0].TotalRows != 2000 || len(result.Minutes[0].TableRows) != 1 || result.Minutes[0].TableRows["dogfood.audit_log"] != 2000 {
		t.Fatalf("filtered minutes = %+v, want only dogfood.audit_log activity", result.Minutes)
	}
	if len(result.Timeseries.RowsSeries) != 1 || result.Timeseries.RowsSeries[0].Value != 2000 || result.Timeseries.InsertEventSeries[0].Value != 1 || result.Timeseries.DDLEventSeries[0].Value != 1 {
		t.Fatalf("filtered timeseries = %+v, want 2000 rows, one insert, one DDL", result.Timeseries)
	}
	if len(result.Diagnostics.DDLEvents) != 1 || result.Diagnostics.DDLEvents[0].Table != "audit_log" || len(result.Diagnostics.LargestTransactions) != 1 || len(result.Diagnostics.LongestTransactions) != 1 || len(result.Diagnostics.WidestTransactions) != 1 || len(result.Diagnostics.HotIntervals) != 1 {
		t.Fatalf("filtered diagnostics = %+v, want only included workload evidence", result.Diagnostics)
	}
	if len(result.Alerts) != 1 || result.Alerts[0].TxnKey != result.Transactions[0].TxnKey {
		t.Fatalf("filtered alerts = %+v, want one alert for the included transaction", result.Alerts)
	}
	if tables, ok := result.Alerts[0].Details["tables"].([]string); !ok || len(tables) != 1 || tables[0] != "dogfood.audit_log" {
		t.Fatalf("filtered alert tables = %#v, want only dogfood.audit_log", result.Alerts[0].Details["tables"])
	}
	if len(result.Diagnostics.Findings) != 1 || result.Diagnostics.Findings[0].TxnKey != result.Transactions[0].TxnKey {
		t.Fatalf("filtered findings = %+v, want one included transaction finding", result.Diagnostics.Findings)
	}
}
