package view

import (
	"testing"

	"binlogviz/internal/model"
)

func TestKeyTransactionsAreTheListedSet(t *testing.T) {
	listed := []model.Transaction{
		{TxnKey: "a", QuerySummary: "select 1"},
		{TxnKey: "b"},
		{TxnKey: "c"},
		{TxnKey: "d"},
	}
	largest := []model.Transaction{
		{TxnKey: "l1"},
		{TxnKey: "l2"},
		{TxnKey: "l3"},
		{TxnKey: "l4"},
	}
	longest := []model.Transaction{
		{TxnKey: "d1"},
		{TxnKey: "d2"},
		{TxnKey: "d3"},
		{TxnKey: "d4"},
	}
	widest := []model.Transaction{
		{TxnKey: "w1"},
		{TxnKey: "w2"},
	}
	result := model.AnalysisResult{
		Summary:      model.WorkloadSummary{TotalTransactions: 10},
		Transactions: listed,
		Diagnostics: model.Diagnostics{
			LargestTransactions: largest,
			LongestTransactions: longest,
			WidestTransactions:  widest,
			LargestByteTransactions: []model.Transaction{
				{TxnKey: "b1", BinlogBytes: 30},
				{TxnKey: "b2", BinlogBytes: 20},
				{TxnKey: "b3", BinlogBytes: 10},
				{TxnKey: "b4", BinlogBytes: 5},
				{TxnKey: "b5", BinlogBytes: 0},
			},
		},
	}
	v := Build(result, Limits{
		TopN:            10,
		DiagnosticLists: true,
		HasQuery:        func(txn model.Transaction) bool { return txn.QuerySummary != "" },
	})
	if len(v.Key) != 4 || len(v.Listed) != 4 || v.Omitted != 6 || keys(v.Key) != "a,b,c,d" {
		t.Fatalf("key %s listed %d omitted %d", keys(v.Key), len(v.Listed), v.Omitted)
	}
	if got := keys(v.Text.Largest); got != "l1,l2,l3" {
		t.Fatalf("text largest %s", got)
	}
	if got := keys(v.Text.Longest); got != "d1,d2,d3" {
		t.Fatalf("text longest %s", got)
	}
	if got := keys(v.Text.Widest); got != "w1" {
		t.Fatalf("text widest %s", got)
	}
	if v.Text.QueryFallback == nil || v.Text.QueryFallback.TxnKey != "a" {
		t.Fatalf("fallback %+v", v.Text.QueryFallback)
	}
	if got := keys(v.Text.ByteLeaders); got != "b1,b2,b3" {
		t.Fatalf("byte leaders %s", got)
	}
	if len(v.Largest) != 4 || len(v.Longest) != 4 || len(v.Widest) != 2 || len(v.LargestBytes) != 5 {
		t.Fatalf("json lists %d %d %d %d", len(v.Largest), len(v.Longest), len(v.Widest), len(v.LargestBytes))
	}
	if len(v.Evidence) != 0 {
		t.Fatalf("champions outside Key are not evidence, got %+v", v.Evidence)
	}
}

func TestTopNOneAndMergedEvidence(t *testing.T) {
	shared := model.Transaction{TxnKey: "same", QuerySummary: "update t"}
	result := model.AnalysisResult{
		Transactions: []model.Transaction{shared, {TxnKey: "other", QuerySummary: "insert t"}},
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{shared, {TxnKey: "l2"}},
			LongestTransactions: []model.Transaction{shared},
			WidestTransactions:  []model.Transaction{{TxnKey: "w1"}, {TxnKey: "w2"}},
		},
	}
	v := Build(result, Limits{
		TopN:     1,
		HasQuery: func(txn model.Transaction) bool { return txn.QuerySummary != "" },
	})
	if keys(v.Text.Largest) != "same" || keys(v.Text.Longest) != "same" || keys(v.Text.Widest) != "w1" {
		t.Fatalf("caps largest=%s longest=%s widest=%s", keys(v.Text.Largest), keys(v.Text.Longest), keys(v.Text.Widest))
	}
	if v.Text.QueryFallback != nil {
		t.Fatalf("fallback should be empty when a champion has a query, got %+v", v.Text.QueryFallback)
	}
	if len(v.Listed) != 2 {
		t.Fatalf("listed was cut by TopN: %d", len(v.Listed))
	}
	if v.Largest != nil || v.Longest != nil {
		t.Fatalf("diagnostic lists should be nil when DiagnosticLists is false")
	}
	if len(v.Evidence) != 1 {
		t.Fatalf("evidence %+v", v.Evidence)
	}
	if v.Evidence[0].Txn.TxnKey != "same" || len(v.Evidence[0].Reasons) != 2 || v.Evidence[0].Reasons[0] != ReasonLargest || v.Evidence[0].Reasons[1] != ReasonLongest {
		t.Fatalf("merged champion %+v", v.Evidence[0])
	}
}

func TestEmptyTxnKeyDoesNotMerge(t *testing.T) {
	blank := model.Transaction{TotalRows: 1}
	v := Build(model.AnalysisResult{
		Transactions: []model.Transaction{blank},
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{blank},
			LongestTransactions: []model.Transaction{blank},
		},
	}, Limits{TopN: 10})
	if len(v.Evidence) != 2 {
		t.Fatalf("empty keys must stay separate, got %+v", v.Evidence)
	}
}

func TestByteLeadersStopAtZero(t *testing.T) {
	v := Build(model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			LargestByteTransactions: []model.Transaction{
				{TxnKey: "a", BinlogBytes: 8},
				{TxnKey: "b", BinlogBytes: 0},
				{TxnKey: "c", BinlogBytes: 9},
			},
		},
	}, Limits{TopN: 10})
	if keys(v.Text.ByteLeaders) != "a" {
		t.Fatalf("leaders %s", keys(v.Text.ByteLeaders))
	}
}

func keys(txns []model.Transaction) string {
	out := ""
	for i, txn := range txns {
		if i > 0 {
			out += ","
		}
		out += txn.TxnKey
	}
	return out
}
