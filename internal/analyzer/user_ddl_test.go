package analyzer

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// A DROP USER that names several accounts must show each one. The old single
// name read kept only the first account, so a DBA auditing a compromised
// account could not see the rest of the drop.
func TestAnalyzeMultiAccountDropUserReportsEachAccount(t *testing.T) {
	base := time.Date(2026, 10, 21, 8, 30, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{
		{
			Timestamp:   base,
			EventType:   "DDL",
			Schema:      "qatipd",
			BinlogBytes: 150,
			QuerySQL:    "DROP USER 'qatipu'@'localhost', 'gone'@'%'",
		},
		{
			Timestamp:   base.Add(time.Minute),
			EventType:   "ROWS",
			Operation:   "INSERT",
			Schema:      "qatipd",
			Table:       "orders",
			RowCount:    3,
			BinlogBytes: 200,
		},
	})
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	want := map[string]bool{"'qatipu'@'localhost'": false, "'gone'@'%'": false}
	for _, event := range result.Diagnostics.DDLEvents {
		if event.Operation != "DROP USER" {
			continue
		}
		if event.Object != "user" {
			t.Fatalf("DROP USER object = %q, want user", event.Object)
		}
		if event.Schema != "" {
			t.Fatalf("account %s schema = %q, want no session schema", event.Table, event.Schema)
		}
		if _, ok := want[event.Table]; !ok {
			t.Fatalf("unexpected DROP USER target %q in %+v", event.Table, result.Diagnostics.DDLEvents)
		}
		want[event.Table] = true
	}
	for account, seen := range want {
		if !seen {
			t.Fatalf("missing account %s in %+v", account, result.Diagnostics.DDLEvents)
		}
	}
	// An account is not a table: only the real row event earns a table row.
	for _, table := range result.Tables {
		if table.Table != "orders" {
			t.Fatalf("account %s.%s leaked into table stats", table.Schema, table.Table)
		}
	}
	if len(result.Tables) != 1 {
		t.Fatalf("tables = %+v, want only qatipd.orders", result.Tables)
	}

	text, err := report.RenderTextWithOptions(*result, report.DefaultOptions())
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	if strings.Contains(text, "qatipd.'qatipu'") || strings.Contains(text, "qatipd.'gone'") {
		t.Fatalf("text report pins accounts under the session schema:\n%s", text)
	}
	for _, account := range []string{"'qatipu'@'localhost'", "'gone'@'%'"} {
		if !strings.Contains(text, account) {
			t.Fatalf("text report missing account %s:\n%s", account, text)
		}
	}
}

// RENAME USER was generic DDL with no target. It now names each "old TO new"
// pair so the timeline shows which login disappeared and which appeared.
func TestAnalyzeRenameUserReportsPair(t *testing.T) {
	base := time.Date(2026, 10, 21, 8, 31, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{{
		Timestamp:   base,
		EventType:   "DDL",
		Schema:      "qatipd",
		BinlogBytes: 120,
		QuerySQL:    "RENAME USER 'old'@'%' TO 'new'@'%', 'x'@'h' TO 'y'@'h'",
	}})
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	var got []string
	for _, event := range result.Diagnostics.DDLEvents {
		if event.Operation != "RENAME USER" {
			continue
		}
		if event.Schema != "" {
			t.Fatalf("RENAME USER target %q schema = %q, want none", event.Table, event.Schema)
		}
		got = append(got, event.Table)
	}
	if len(got) != 2 || got[0] != "'old'@'%' TO 'new'@'%'" || got[1] != "'x'@'h' TO 'y'@'h'" {
		t.Fatalf("RENAME USER targets = %v", got)
	}
	if len(result.Tables) != 0 {
		t.Fatalf("USER DDL produced table stats: %+v", result.Tables)
	}
}

// CREATE and ALTER USER keep their first account: the account boundary is not
// safe to split while auth options sit between names.
func TestAnalyzeCreateUserKeepsFirstAccount(t *testing.T) {
	base := time.Date(2026, 10, 21, 8, 32, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{{
		Timestamp:   base,
		EventType:   "DDL",
		BinlogBytes: 100,
		QuerySQL:    "CREATE USER 'a'@'h' IDENTIFIED BY 's3,cret', 'b'@'h'",
	}})
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	var creates int
	for _, event := range result.Diagnostics.DDLEvents {
		if event.Operation == "CREATE USER" {
			creates++
			if event.Table != "'a'@'h'" {
				t.Fatalf("CREATE USER table = %q, want the first account", event.Table)
			}
		}
	}
	if creates != 1 {
		t.Fatalf("CREATE USER events = %d, want 1; events %+v", creates, result.Diagnostics.DDLEvents)
	}
	if len(result.Tables) != 0 {
		t.Fatalf("USER DDL produced table stats: %+v", result.Tables)
	}
}
