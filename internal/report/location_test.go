// Package report verifies XID-only spans are not offered as rollback file:pos.
// input: transactions that match dogfood issue #18 (31-byte XID) versus usable spans.
// output: rollback hint omitted for XID-only; recorded span still shown with an honest note.
// pos: regression coverage so the HTML incident page does not invent start positions.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func xidOnlyLargeTxn() model.Transaction {
	return model.Transaction{
		TxnKey:          "txn-1",
		TotalRows:       400000,
		EventCount:      9524,
		BinlogBytes:     31,
		BinlogPathStart: "mysql-bin.000008",
		BinlogPathEnd:   "mysql-bin.000008",
		PositionStart:   77914917,
		PositionEnd:     77914948,
		Operations:      map[string]int{"INSERT": 400000},
		Tables:          map[string]int{"dogfood_big.t": 400000},
	}
}

func TestIsXIDOnlyTxnSpanMatchesDogfoodLargeTxn(t *testing.T) {
	txn := xidOnlyLargeTxn()
	if !isXIDOnlyTxnSpan(txn) {
		t.Fatal("expected 31-byte / 400000-row span to be treated as XID-only")
	}
	if got := usableRollbackLocation(txn); got != "" {
		t.Fatalf("usable rollback location should be empty, got %q", got)
	}
	if formatRecordedTxnLocation(txn) != "mysql-bin.000008:77914917-77914948" {
		t.Fatalf("recorded span should still be shown, got %q", formatRecordedTxnLocation(txn))
	}
}

func TestUsableRollbackLocationKeepsRealSpan(t *testing.T) {
	txn := model.Transaction{
		TxnKey:          "txn-real",
		TotalRows:       2,
		EventCount:      4,
		BinlogBytes:     240,
		BinlogPathStart: "mysql-bin.000123",
		BinlogPathEnd:   "mysql-bin.000123",
		PositionStart:   240,
		PositionEnd:     480,
	}
	if isXIDOnlyTxnSpan(txn) {
		t.Fatal("240-byte span must remain usable")
	}
	if got := usableRollbackLocation(txn); got != "mysql-bin.000123:240-480" {
		t.Fatalf("usable location = %q", got)
	}
}

func TestRenderHTMLOmitsXIDOnlyRollbackHint(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{xidOnlyLargeTxn()},
			LongestTransactions: []model.Transaction{xidOnlyLargeTxn()},
		},
	}

	textOut, err := RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	htmlOut, err := RenderHTML(result)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(textOut, "First suspicious position") {
		t.Fatalf("text must not offer an XID-only span as a rollback hint\n%s", textOut)
	}
	if strings.Contains(htmlOut, "First suspicious position") {
		t.Fatalf("HTML must not offer an XID-only span as a rollback hint\n%s", htmlOut)
	}
	if !strings.Contains(htmlOut, "mysql-bin.000008:77914917-77914948") {
		t.Fatal("HTML should still show the recorded XID span")
	}
	if !strings.Contains(htmlOut, "XID-only") || !strings.Contains(htmlOut, "not a usable --start-position") {
		t.Fatalf("HTML should say the recorded span is XID-only\n%s", htmlOut)
	}
	if strings.Contains(htmlOut, "mysql-bin.000008:385") {
		t.Fatal("HTML must not invent a start position the analyzer does not have")
	}
}

func TestRenderHTMLDrilldownDoesNotLinkRepresentativeTxns(t *testing.T) {
	result := model.AnalysisResult{
		PatternDrilldowns: []model.PatternDrilldown{{
			PatternKey:    "dogfood_big.t|DELETE|large",
			Label:         "dogfood_big.t / DELETE / large batch",
			WhySelected:   "high signal: dominates workload",
			ShareOfRows:   0.02,
			ShareOfTxns:   0.66,
			AvgRowsPerTxn: 48.7,
			RepresentativeTransactions: []model.PatternRepresentativeTxn{
				{TxnKey: "txn-1", TotalRows: 400000},
				{TxnKey: "txn-2", TotalRows: 400000},
			},
		}},
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{xidOnlyLargeTxn()},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "txn-1") {
		t.Fatal("expected analyzer representative txn to be listed")
	}
	for _, token := range []string{`data-filter-txn=`, `href="#txn-1"`, `href="#txn-2"`} {
		if strings.Contains(out, token) {
			t.Fatalf("must not add a click path onto representative txns (%s)", token)
		}
	}
}
