package report

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestRenderTextTopThreadsRanksAndOmitsAbsentIdentity(t *testing.T) {
	forceEnglishReportLocale(t)
	result := model.AnalysisResult{
		ThreadsRankedBy: model.ThreadRankRows,
		Threads: []model.ThreadStats{
			{ThreadID: 8, ServerID: 1, Schema: "reviewdb", TotalRows: 132591, EventCount: 40, TxnCount: 3, BinlogBytes: 2048, Share: 0.726},
			{ThreadID: 14, ServerID: 1, ActorUser: "app", ActorHost: "db.local", Schema: "reviewdb", TotalRows: 50001, EventCount: 12, TxnCount: 1, BinlogBytes: 512, Share: 0.274},
		},
	}
	out, err := RenderTextWithOptions(result, Options{TopN: 10, TopThreads: 10, TopThreadsSet: true})
	if err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	if !strings.Contains(out, "=== Top Threads (by rows) ===") {
		t.Fatalf("missing top threads section:\n%s", out)
	}
	thread8 := strings.Index(out, " 8 ")
	thread14 := strings.Index(out, " 14 ")
	if thread8 < 0 || thread14 < 0 || thread8 > thread14 {
		t.Fatalf("expected thread 8 before thread 14:\n%s", out)
	}
	if !strings.Contains(out, "app@db.local") || !strings.Contains(out, "reviewdb") || !strings.Contains(out, "72.6%") {
		t.Fatalf("missing actor, schema, or share:\n%s", out)
	}
	limited, err := RenderTextWithOptions(result, Options{TopN: 10, TopThreads: 1, TopThreadsSet: true})
	if err != nil {
		t.Fatalf("limited RenderText: %v", err)
	}
	if strings.Contains(limited, "app@db.local") || !strings.Contains(limited, "1 more threads") {
		t.Fatalf("limit did not keep only the first thread:\n%s", limited)
	}
}

func TestRenderTextTopThreadsOmitsUserHostColumnWhenAbsent(t *testing.T) {
	forceEnglishReportLocale(t)
	result := model.AnalysisResult{
		ThreadsRankedBy: model.ThreadRankRows,
		Threads: []model.ThreadStats{
			{ThreadID: 3, ServerID: 1, TotalRows: 4, EventCount: 2, TxnCount: 1, Share: 1},
		},
	}
	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("RenderText: %v", err)
	}
	section := out[strings.Index(out, "=== Top Threads"):]
	if strings.Contains(section, "user@host") {
		t.Fatalf("invented user@host column:\n%s", section)
	}
	if !strings.Contains(section, "thread_id") || !strings.Contains(section, "server_id") {
		t.Fatalf("missing thread identity columns:\n%s", section)
	}
}

func TestRenderTextSQLContextModesOnTransactionsAndPatterns(t *testing.T) {
	forceEnglishReportLocale(t)
	result := model.AnalysisResult{
		SQLContextAvailable: true,
		Transactions: []model.Transaction{
			sampleTransactionWithQueryContext(),
		},
		Patterns: []model.PatternStats{
			{PatternKey: "p1", Label: "delete victims", TotalRows: 2, TxnCount: 1, AvgRowsPerTxn: 2, SampleQuerySummary: "DELETE FROM victims WHERE id IN (2,3)"},
		},
	}
	summary, err := RenderTextWithOptions(result, Options{ShowPatterns: true, SQLContextMode: SQLContextSummary, TopN: 5})
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if !strings.Contains(summary, "Query: UPDATE orders SET status = ? WHERE id = ?") {
		t.Fatalf("summary text missing query line:\n%s", summary)
	}
	if !strings.Contains(summary, "Query: DELETE FROM victims WHERE id IN (2,3)") {
		t.Fatalf("summary patterns missing query line:\n%s", summary)
	}
	full, err := RenderTextWithOptions(result, Options{ShowPatterns: true, SQLContextMode: SQLContextFull, TopN: 5})
	if err != nil {
		t.Fatalf("full: %v", err)
	}
	if !strings.Contains(full, "Query: UPDATE orders SET status = 'paid' WHERE id = 42") {
		t.Fatalf("full text missing stored SQL:\n%s", full)
	}
	off, err := RenderTextWithOptions(result, Options{ShowPatterns: true, SQLContextMode: SQLContextOff, TopN: 5})
	if err != nil {
		t.Fatalf("off: %v", err)
	}
	if strings.Contains(off, "Query:") || strings.Contains(off, "DELETE FROM victims") || strings.Contains(off, "UPDATE orders") {
		t.Fatalf("off text still printed query text:\n%s", off)
	}
	if !strings.Contains(off, "delete victims") {
		t.Fatalf("off text dropped the pattern label:\n%s", off)
	}
}

func TestRenderJSONThreadsAndSQLContextOff(t *testing.T) {
	result := model.AnalysisResult{
		ThreadsRankedBy: model.ThreadRankRows,
		Threads: []model.ThreadStats{
			{ThreadID: 14, ServerID: 1, ActorUser: "app", ActorHost: "db.local", Schema: "reviewdb", Schemas: []string{"reviewdb", "other"}, TotalRows: 50001, EventCount: 12, TxnCount: 1, BinlogBytes: 100, Share: 1, ShareOfRows: 1},
		},
		Patterns: []model.PatternStats{
			{PatternKey: "p", Label: "shape", SampleQuerySummary: "DELETE FROM victims WHERE id IN (2,3)"},
		},
	}
	out, err := RenderJSONWithOptions(result, Options{SQLContextMode: SQLContextOff, TopThreads: 10, TopThreadsSet: true})
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	parsed := parseJSONMap(t, out)
	if parsed["threads_ranked_by"] != "rows" {
		t.Fatalf("threads_ranked_by = %v", parsed["threads_ranked_by"])
	}
	threads := parsed["threads"].([]any)
	if len(threads) != 1 {
		t.Fatalf("threads = %v", threads)
	}
	thread := threads[0].(map[string]any)
	if thread["thread_id"] != float64(14) || thread["rows"] != float64(50001) || thread["transactions"] != float64(1) {
		t.Fatalf("thread = %+v", thread)
	}
	actor := thread["actor"].(map[string]any)
	if actor["user"] != "app" || actor["host"] != "db.local" || thread["schema"] != "reviewdb" {
		t.Fatalf("thread identity = %+v", thread)
	}
	patterns := parsed["patterns"].([]any)
	pattern := patterns[0].(map[string]any)
	if _, ok := pattern["sample_query_summary"]; ok {
		t.Fatalf("off mode kept sample_query_summary: %+v", pattern)
	}
}

func TestRenderMarkdownAndHTMLIncludeThreadsAndHonorSQLContext(t *testing.T) {
	forceEnglishReportLocale(t)
	txn := sampleTransactionWithQueryContext()
	result := model.AnalysisResult{
		ThreadsRankedBy: model.ThreadRankEvents,
		Threads: []model.ThreadStats{
			{ThreadID: 9, EventCount: 8, TxnCount: 1, Share: 1},
		},
		Transactions: []model.Transaction{txn},
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{txn},
		},
	}
	md, err := RenderMarkdownWithOptions(result, Options{SQLContextMode: SQLContextOff, TopN: 5, TopThreads: 5, TopThreadsSet: true})
	if err != nil {
		t.Fatalf("markdown: %v", err)
	}
	if !strings.Contains(md, "Top Threads (by events)") || !strings.Contains(md, "9") {
		t.Fatalf("markdown missing threads:\n%s", md)
	}
	if strings.Contains(md, "user@host") {
		t.Fatalf("markdown invented user@host column:\n%s", md)
	}
	if strings.Contains(md, "UPDATE orders") {
		t.Fatalf("markdown off mode printed query text:\n%s", md)
	}
	htmlOff, err := RenderHTMLWithOptions(result, Options{SQLContextMode: SQLContextOff, TopN: 5, TopThreads: 5, TopThreadsSet: true})
	if err != nil {
		t.Fatalf("html off: %v", err)
	}
	if !strings.Contains(htmlOff, "top-threads-table") || !strings.Contains(htmlOff, ">9<") && !strings.Contains(htmlOff, ">9</td>") {
		if !strings.Contains(htmlOff, "Top Threads (by events)") {
			t.Fatalf("html missing threads:\n%s", htmlOff)
		}
	}
	if strings.Contains(htmlOff, "UPDATE orders") {
		t.Fatalf("html off mode printed query text")
	}
	htmlFull, err := RenderHTMLWithOptions(result, Options{SQLContextMode: SQLContextFull, TopN: 5})
	if err != nil {
		t.Fatalf("html full: %v", err)
	}
	if !strings.Contains(htmlFull, "UPDATE orders SET status = &#39;paid&#39; WHERE id = 42") && !strings.Contains(htmlFull, "UPDATE orders SET status = 'paid' WHERE id = 42") {
		t.Fatalf("html full mode missing stored SQL")
	}
}
