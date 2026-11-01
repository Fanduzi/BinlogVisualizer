package binlogviz

import (
	"encoding/json"
	"testing"

	"binlogviz/internal/analyzer"
)

func assertSelectedMariaDBXACommit(t *testing.T, mock *mockParser, xid string) {
	t.Helper()
	selector, err := analyzer.ParseGTIDSelector([]string{"0-7-1860"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := analyzer.DefaultOptions()
	opts.GTIDSelector = selector
	selected, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, opts, "json", mock)
	})
	if err != nil {
		t.Fatalf("select XA COMMIT GTID: %v", err)
	}
	var selectedReport struct {
		Transactions []struct {
			GTID      string `json:"gtid"`
			XAXID     string `json:"xa_xid"`
			TotalRows int    `json:"total_rows"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(selected), &selectedReport); err != nil {
		t.Fatalf("decode selected XA COMMIT: %v\n%s", err, selected)
	}
	if len(selectedReport.Transactions) != 1 || selectedReport.Transactions[0].GTID != "0-7-1860" || selectedReport.Transactions[0].XAXID != xid || selectedReport.Transactions[0].TotalRows != 0 {
		t.Fatalf("selected XA COMMIT = %+v", selectedReport.Transactions)
	}
}
