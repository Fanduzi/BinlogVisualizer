// Package compare verifies compare diagnostic HTML rendering.
// input: synthetic compare results with diagnostic deltas and replay evidence.
// output: assertions for HTML sections, escaping, localization, and copyable replay commands.
// pos: regression coverage for the compare HTML diagnostics path.
// note: if this file changes, update this header and internal/compare/README.md.
package compare

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

func TestRenderHTMLIncludesDiagnosticsDeltaSections(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 1000, BaselineTotalRows: 800, TotalRowsDelta: 200,
			CurrentTotalTransactions: 50, BaselineTotalTransactions: 40, TotalTransactionsDelta: 10,
		},
		DiagnosticsDelta: DiagnosticsDelta{
			DDLChanges: DDLChangeDelta{
				BaselineCount: 1,
				CurrentCount:  3,
				Delta:         2,
				Added: []DDLEventItem{
					{Timestamp: "2026-03-20T10:05:00Z", Schema: "shop", Table: "orders", Operation: "ALTER TABLE", Statement: "ALTER TABLE orders ADD COLUMN status INT"},
					{Timestamp: "2026-03-20T10:10:00Z", Schema: "shop", Table: "payments", Operation: "CREATE INDEX", Statement: "CREATE INDEX idx ON payments(id)"},
				},
				Removed: []DDLEventItem{
					{Timestamp: "2026-03-13T10:05:00Z", Schema: "shop", Table: "legacy", Operation: "DROP TABLE", Statement: "DROP TABLE legacy"},
				},
			},
			TxnDiagnostics: TxnDiagnosticDelta{
				LargestTxnDelta: TxnSizeCompare{
					BaselineRows: 300, BaselineKey: "txn-b1",
					CurrentRows: 500, CurrentKey: "txn-c1",
					DeltaRows: 200,
				},
				LongestTxnDelta: TxnDurationCompare{
					BaselineDuration: "15s", BaselineKey: "txn-b2",
					CurrentDuration: "30s", CurrentKey: "txn-c2",
				},
			},
			HotIntervalDelta: HotIntervalDelta{
				BaselineCount: 1, CurrentCount: 2,
				BaselineTopRows: 400, CurrentTopRows: 600, DeltaTopRows: 200,
				TopItems: []HotIntervalItem{
					{Minute: "2026-03-20T10:05:00Z", Source: "current", TotalRows: 600, TxnCount: 10, BinlogBytes: 10240},
					{Minute: "2026-03-13T10:05:00Z", Source: "baseline", TotalRows: 400, TxnCount: 8, BinlogBytes: 8192},
				},
			},
			EventMixDelta: EventMixDelta{
				InsertDelta: 20, UpdateDelta: 50, DeleteDelta: 10, DDLDelta: 3,
			},
		},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	// Verify section containers exist
	for _, token := range []string{
		`id="compare-ddl-changes"`,
		`id="compare-txn-diagnostics"`,
		`id="compare-hot-intervals"`,
		`id="compare-event-mix-delta"`,
		"DDL Changes",
		"Transaction Diagnostics",
		"Hot Interval Changes",
		"Event Mix Delta",
	} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected html to contain %q", token)
		}
	}

	// Verify DDL data contract
	if !strings.Contains(html, "window.compareDDLChanges =") {
		t.Fatalf("expected DDL changes data contract")
	}
	if !strings.Contains(html, "ALTER TABLE orders ADD COLUMN status INT") {
		t.Fatalf("expected added DDL statement in HTML")
	}
	if !strings.Contains(html, "DROP TABLE legacy") {
		t.Fatalf("expected removed DDL statement in HTML")
	}

	// Verify txn diagnostics data contract
	if !strings.Contains(html, "window.compareTxnDiagnostics =") {
		t.Fatalf("expected txn diagnostics data contract")
	}

	// Verify hot interval data contract
	if !strings.Contains(html, "window.compareHotIntervals =") {
		t.Fatalf("expected hot intervals data contract")
	}

	// Verify event mix data contract
	if !strings.Contains(html, "window.compareEventMixDelta =") {
		t.Fatalf("expected event mix delta data contract")
	}
}

func TestRenderHTMLIncludesCurrentTxnReplayEvidence(t *testing.T) {
	replayAvailable := true
	current := InputReport{
		Diagnostics: InputDiagnostics{
			LargestTransactions: []InputTransaction{{
				TxnKey:          "txn-current",
				TotalRows:       2000,
				EventCount:      8,
				BinlogFileStart: "minimal.binlog",
				BinlogFileEnd:   "minimal.binlog",
				PosStart:        962,
				PosEnd:          1186,
				Completeness:    "complete",
				ReplayAvailable: &replayAvailable,
				ReplayScope:     "full_transaction",
				MysqlbinlogCmd:  "mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=962 --stop-position=1186 /tmp/minimal.binlog",
			}},
		},
	}

	result := BuildCompareResult(current, InputReport{})
	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}
	for _, token := range []string{
		"minimal.binlog:962-1186",
		"mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=962 --stop-position=1186 /tmp/minimal.binlog",
		`data-copy="mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=962 --stop-position=1186 /tmp/minimal.binlog"`,
	} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected current transaction replay evidence %q", token)
		}
	}
}

func TestRenderHTMLDiagnosticsSectionsGracefulWhenEmpty(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
		},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	// Sections should still render (with empty-state fallbacks), not panic
	for _, token := range []string{
		`id="compare-ddl-changes"`,
		`id="compare-txn-diagnostics"`,
		`id="compare-hot-intervals"`,
		`id="compare-event-mix-delta"`,
	} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected section container %q even when empty", token)
		}
	}
}

func TestRenderHTMLDiagnosticsEscapesMaliciousContent(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
		},
		DiagnosticsDelta: DiagnosticsDelta{
			DDLChanges: DDLChangeDelta{
				Added: []DDLEventItem{
					{
						Schema:    `<img src=x onerror=alert(1)>`,
						Table:     `<script>alert("t")</script>`,
						Operation: "ALTER TABLE",
						Statement: `DROP TABLE <img src=x onerror=alert(2)>`,
					},
				},
			},
			TxnDiagnostics: TxnDiagnosticDelta{
				LargestTxnDelta: TxnSizeCompare{
					BaselineKey: `<script>alert("bk")</script>`,
					CurrentKey:  `<img src=x onerror=alert(3)>`,
				},
			},
			HotIntervalDelta: HotIntervalDelta{
				TopItems: []HotIntervalItem{
					{Minute: `<script>alert("m")</script>`, Source: "current", TotalRows: 100},
				},
			},
		},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	for _, forbidden := range []string{
		`<script>alert("t")</script>`,
		`<script>alert("bk")</script>`,
		`<script>alert("m")</script>`,
		`<img src=x onerror=alert(1)>`,
		`<img src=x onerror=alert(2)>`,
		`<img src=x onerror=alert(3)>`,
	} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("expected hostile content to be escaped, found %q", forbidden)
		}
	}
}

func TestRenderHTMLLocalizesHotIntervalSourcesToChinese(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(i18n.ResetForTesting)

	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
		},
		DiagnosticsDelta: DiagnosticsDelta{
			HotIntervalDelta: HotIntervalDelta{
				TopItems: []HotIntervalItem{
					{Minute: "2026-03-20T10:05:00Z", Source: "current", TotalRows: 600, TxnCount: 10},
					{Minute: "2026-03-13T10:05:00Z", Source: "baseline", TotalRows: 400, TxnCount: 8},
				},
			},
		},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	for _, token := range []string{`<td>当前</td>`, `<td>基线</td>`} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected localized source cell %q", token)
		}
	}
	for _, token := range []string{`<td>current</td>`, `<td>baseline</td>`} {
		if strings.Contains(html, token) {
			t.Fatalf("expected source cell not to leak English token %q", token)
		}
	}
}
