// Package binlogviz proves analyze reports keep transaction identity from a real binlog.
// input: runAnalysis on mysql-8.0.46-flush-tables.binlog and minimal.binlog through the real parser.
// output: text, JSON, Markdown, and HTML show server_id, thread_id, GTID, and xid when the file has them, and omit GTID and user@host when it does not.
// pos: operator I/O check that identity survives the parser, not only hand-built events.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"strings"
	"testing"

	"binlogviz/internal/analyzer"
)

func TestAnalyzeRealFixtureKeepsTransactionIdentity(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	flush := mustFixturePath(t, "mysql-8.0.46-flush-tables.binlog")
	const gtid = "59bea4c2-b1c8-11f1-9298-8e53d7213b97:9"
	wantText := []string{
		"server_id=1",
		"thread_id=12",
		"gtid=" + gtid,
		"xid=16",
	}

	for _, format := range []string{"text", "markdown", "html"} {
		t.Run("flush-"+format, func(t *testing.T) {
			stdout, stderr, err := captureStdoutStderrRun(t, func() error {
				return runAnalysis([]string{flush}, analyzer.DefaultOptions(), format)
			})
			if err != nil {
				t.Fatalf("analyze %s: %v stderr=%q", format, err, stderr)
			}
			if strings.Contains(stderr, "Error:") {
				t.Fatalf("successful analyze printed Error: %q", stderr)
			}
			for _, token := range wantText {
				if !strings.Contains(stdout, token) {
					t.Fatalf("%s missing %q", format, token)
				}
			}
			if strings.Contains(stdout, "user@host") {
				t.Fatalf("%s invented user@host", format)
			}
		})
	}

	t.Run("flush-json", func(t *testing.T) {
		stdout, stderr, err := captureStdoutStderrRun(t, func() error {
			return runAnalysis([]string{flush}, analyzer.DefaultOptions(), "json")
		})
		if err != nil {
			t.Fatalf("analyze json: %v stderr=%q", err, stderr)
		}
		txn := decodeFirstTransaction(t, stdout)
		if txn["server_id"] != float64(1) || txn["thread_id"] != float64(12) || txn["gtid"] != gtid || txn["xid"] != "16" {
			t.Fatalf("transaction identity = %+v", txn)
		}
		for _, absent := range []string{"actor", "xa_xid"} {
			if _, ok := txn[absent]; ok {
				t.Fatalf("json invented %s: %+v", absent, txn)
			}
		}
	})

	minimal := mustFixturePath(t, "minimal.binlog")
	t.Run("minimal-text", func(t *testing.T) {
		stdout, stderr, err := captureStdoutStderrRun(t, func() error {
			return runAnalysis([]string{minimal}, analyzer.DefaultOptions(), "text")
		})
		if err != nil {
			t.Fatalf("analyze text: %v stderr=%q", err, stderr)
		}
		for _, token := range []string{"server_id=1", "thread_id=3", "xid="} {
			if !strings.Contains(stdout, token) {
				t.Fatalf("minimal text missing %q\n%s", token, stdout)
			}
		}
		for _, token := range []string{"gtid=", "user@host"} {
			if strings.Contains(stdout, token) {
				t.Fatalf("minimal text invented %q", token)
			}
		}
	})

	t.Run("minimal-json", func(t *testing.T) {
		stdout, stderr, err := captureStdoutStderrRun(t, func() error {
			return runAnalysis([]string{minimal}, analyzer.DefaultOptions(), "json")
		})
		if err != nil {
			t.Fatalf("analyze json: %v stderr=%q", err, stderr)
		}
		var decoded struct {
			Transactions []map[string]any `json:"transactions"`
		}
		if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}
		if len(decoded.Transactions) == 0 {
			t.Fatal("minimal fixture produced no transactions")
		}
		for _, txn := range decoded.Transactions {
			if txn["server_id"] != float64(1) || txn["thread_id"] != float64(3) {
				t.Fatalf("minimal identity = %+v", txn)
			}
			xid, _ := txn["xid"].(string)
			if xid == "" {
				t.Fatalf("minimal xid missing: %+v", txn)
			}
			for _, absent := range []string{"gtid", "actor", "xa_xid"} {
				if _, ok := txn[absent]; ok {
					t.Fatalf("minimal json invented %s: %+v", absent, txn)
				}
			}
		}
	})
}

func decodeFirstTransaction(t *testing.T, stdout string) map[string]any {
	t.Helper()
	var decoded struct {
		Transactions []map[string]any `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, stdout)
	}
	if len(decoded.Transactions) != 1 {
		t.Fatalf("transactions = %d, want 1", len(decoded.Transactions))
	}
	return decoded.Transactions[0]
}
