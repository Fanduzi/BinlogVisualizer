package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestSQLContextOffOmitsDDLStatementText(t *testing.T) {
	forceEnglishReportLocale(t)
	const hash = "pINSRVdE/pm7UzcDqjMRZ4wRQ3p8g2Ic9"
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			DDLEvents: []model.DDLEvent{{
				Timestamp:     time.Date(2026, 10, 6, 12, 32, 14, 0, time.UTC),
				Operation:     "CREATE USER",
				Object:        "user",
				Table:         "'app'@'%'",
				Statement:     "CREATE USER 'app'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '<secret>'",
				PositionStart: 101052,
				PositionEnd:   101285,
			}},
		},
	}
	for _, mode := range []SQLContextMode{SQLContextOff, SQLContextSummary, SQLContextFull} {
		text, err := RenderTextWithOptions(result, Options{SQLContextMode: mode, TopN: 10})
		if err != nil {
			t.Fatalf("text %s: %v", mode, err)
		}
		md, err := RenderMarkdownWithOptions(result, Options{SQLContextMode: mode})
		if err != nil {
			t.Fatalf("markdown %s: %v", mode, err)
		}
		html, err := RenderHTMLWithOptions(result, Options{SQLContextMode: mode})
		if err != nil {
			t.Fatalf("html %s: %v", mode, err)
		}
		js, err := RenderJSONWithOptions(result, Options{SQLContextMode: mode})
		if err != nil {
			t.Fatalf("json %s: %v", mode, err)
		}
		outputs := []string{text, md, html, js}
		for _, out := range outputs {
			if strings.Contains(out, hash) {
				t.Fatalf("%s leaked credential material:\n%s", mode, out)
			}
		}
		if mode == SQLContextOff {
			for _, out := range outputs {
				if strings.Contains(out, "secret") || strings.Contains(out, "IDENTIFIED") || strings.Contains(out, "caching_sha2") {
					t.Fatalf("off still printed the DDL statement:\n%s", out)
				}
			}
			if !strings.Contains(text, "CREATE USER") || !strings.Contains(text, "'app'@'%'") {
				t.Fatalf("off dropped the DDL operation or object:\n%s", text)
			}
			continue
		}
		for _, out := range outputs {
			if !strings.Contains(out, "secret") || strings.Contains(out, hash) {
				t.Fatalf("%s missing redacted DDL statement:\n%s", mode, out)
			}
		}
	}
}

func TestFullSQLTruncationMarkerInEveryFormat(t *testing.T) {
	forceEnglishReportLocale(t)
	original := "INSERT INTO t VALUES " + strings.Repeat("v", model.MaxStoredSQLBytes+100)
	qc := model.NewQueryContext(original)
	if qc == nil || !qc.Truncated || qc.OriginalBytes != len(original) {
		t.Fatalf("context = %+v", qc)
	}
	marker := model.TruncationMarker(len(qc.SQL), len(original))
	txn := model.Transaction{
		TxnKey:       "txn-1",
		TotalRows:    1,
		QuerySummary: model.FormatQuerySummary(qc.SQL, qc.OriginalBytes),
		QueryContext: qc,
	}
	result := model.AnalysisResult{
		Transactions: []model.Transaction{txn},
		Diagnostics:  model.Diagnostics{LargestTransactions: []model.Transaction{txn}},
	}
	text, err := RenderTextWithOptions(result, Options{SQLContextMode: SQLContextFull, TopN: 5})
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	md, err := RenderMarkdownWithOptions(result, Options{SQLContextMode: SQLContextFull})
	if err != nil {
		t.Fatalf("markdown: %v", err)
	}
	html, err := RenderHTMLWithOptions(result, Options{SQLContextMode: SQLContextFull})
	if err != nil {
		t.Fatalf("html: %v", err)
	}
	js, err := RenderJSONWithOptions(result, Options{SQLContextMode: SQLContextFull})
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	for _, out := range []string{text, md, html, js} {
		if !strings.Contains(out, marker) {
			t.Fatalf("missing marker %q\n%s", marker, out)
		}
	}
	if !strings.Contains(js, `"query_truncated": true`) || !strings.Contains(js, `"query_original_bytes": `) {
		t.Fatalf("json lost storage-truncation metadata:\n%s", js)
	}
}

func TestSummaryDisplayCutMarksOriginalLengthWithoutStorageFlag(t *testing.T) {
	forceEnglishReportLocale(t)
	sql := strings.Repeat("a", 180)
	qc := model.NewQueryContext(sql)
	txn := model.Transaction{
		TxnKey:       "txn-1",
		TotalRows:    1,
		QuerySummary: model.MakeQuerySummary(sql),
		QueryContext: qc,
	}
	result := model.AnalysisResult{
		Transactions: []model.Transaction{txn},
		Diagnostics:  model.Diagnostics{LargestTransactions: []model.Transaction{txn}},
	}
	text, err := RenderTextWithOptions(result, Options{SQLContextMode: SQLContextSummary, TopN: 5})
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	if !strings.Contains(text, model.TruncationMarker(model.MaxQuerySummaryChars, 180)) {
		t.Fatalf("summary text missing original length:\n%s", text)
	}
	js, err := RenderJSONWithOptions(result, Options{SQLContextMode: SQLContextSummary})
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	if strings.Contains(js, `"query_truncated": true`) {
		t.Fatalf("query_truncated is the 4096-byte store cap, not the 160-character summary:\n%s", js)
	}
	if !strings.Contains(js, "of 180 bytes]") {
		t.Fatalf("json summary missing marker:\n%s", js)
	}
}

func TestStdinReplayNoteHasNoRerunPath(t *testing.T) {
	forceEnglishReportLocale(t)
	txn := withFullReplaySpan(model.Transaction{
		TxnKey:          "txn-1",
		TotalRows:       800,
		EventCount:      4,
		BinlogBytes:     31571,
		BinlogPathStart: "stdin",
		BinlogPathEnd:   "stdin",
		PositionStart:   197,
		PositionEnd:     31768,
		StdinInput:      true,
	})
	result := model.AnalysisResult{
		Transactions: []model.Transaction{txn},
		Diagnostics:  model.Diagnostics{LargestTransactions: []model.Transaction{txn}},
	}
	text, err := RenderTextWithOptions(result, Options{TopN: 5})
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	md, err := RenderMarkdownWithOptions(result, Options{})
	if err != nil {
		t.Fatalf("markdown: %v", err)
	}
	html, err := RenderHTMLWithOptions(result, Options{})
	if err != nil {
		t.Fatalf("html: %v", err)
	}
	js, err := RenderJSONWithOptions(result, Options{})
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	for _, out := range []string{text, md, html, js} {
		if strings.Contains(out, "mysqlbinlog ") || strings.Contains(out, "mariadb-binlog ") || strings.Contains(out, "/stdin") {
			t.Fatalf("invented a replay path:\n%s", out)
		}
		if !strings.Contains(out, "input came from stdin") || !strings.Contains(out, "start-position=197") || !strings.Contains(out, "stop-position=31768") {
			t.Fatalf("missing stdin replay hint:\n%s", out)
		}
	}
	if strings.Contains(js, `"replay_available": true`) {
		t.Fatalf("stdin replay_available must be false:\n%s", js)
	}
}
