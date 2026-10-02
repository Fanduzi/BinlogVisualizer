// Package binlogviz verifies the committed multi-second duration fixture at the analyze command.
// input: analyze on internal/binlog/testdata/mysql-8.0.46-committed-duration.binlog.
// output: text ranks that transaction and prints a non-<1s bucket; JSON duration_buckets matches; a lower --large-trx-duration alerts with the real duration.
// pos: operator I/O for the #89 committed duration path through the real parser.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAnalyzeCommittedDurationFixtureRanksTextAndJSON(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-committed-duration.binlog")

	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "text")
	if err != nil {
		t.Fatalf("analyze text: %v stderr=%s", err, stderr)
	}
	if strings.Contains(stderr, "Error:") {
		t.Fatalf("successful analyze must not print Error:, got %q", stderr)
	}
	if !strings.Contains(stdout, "Committed duration:") || !strings.Contains(stdout, "<1s=") || !strings.Contains(stdout, "1s-10s=") {
		t.Fatalf("duration buckets missing:\n%s", stdout)
	}
	line := longestTransactionLine(t, stdout)
	if strings.Contains(line, "dur=0") {
		t.Fatalf("longest transaction is still sub-second: %s", line)
	}

	stdout, stderr, err = executeAnalyzeLikeMain(t, fixture, "--format", "json")
	if err != nil {
		t.Fatalf("analyze json: %v stderr=%s", err, stderr)
	}
	var decoded struct {
		Diagnostics struct {
			DurationBuckets []struct {
				Label    string `json:"label"`
				TxnCount int    `json:"txn_count"`
			} `json:"duration_buckets"`
			LongestTransactions []struct {
				Duration string         `json:"duration"`
				Tables   map[string]int `json:"tables"`
			} `json:"longest_transactions"`
		} `json:"diagnostics"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json: %v\n%s", jsonErr, stdout)
	}
	if len(decoded.Diagnostics.LongestTransactions) == 0 || decoded.Diagnostics.LongestTransactions[0].Tables["testdb.users"] != 1 {
		t.Fatalf("longest = %+v", decoded.Diagnostics.LongestTransactions)
	}
	leader, parseErr := time.ParseDuration(decoded.Diagnostics.LongestTransactions[0].Duration)
	if parseErr != nil || leader < time.Second || leader >= 10*time.Second {
		t.Fatalf("longest duration %q: %v", decoded.Diagnostics.LongestTransactions[0].Duration, parseErr)
	}
	buckets := map[string]int{}
	for _, bucket := range decoded.Diagnostics.DurationBuckets {
		buckets[bucket.Label] = bucket.TxnCount
	}
	if buckets["<1s"] != 1 || buckets["1s-10s"] != 1 {
		t.Fatalf("duration_buckets = %+v", decoded.Diagnostics.DurationBuckets)
	}
}

func TestAnalyzeCommittedDurationFixtureAlertsBelowThreshold(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-committed-duration.binlog")

	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "text", "--large-trx-duration", "1s")
	if err != nil {
		t.Fatalf("analyze: %v stderr=%s", err, stderr)
	}
	if !strings.Contains(stdout, "exceeds duration threshold (") {
		t.Fatalf("duration alert missing:\n%s", stdout)
	}
	if strings.Contains(stdout, "exceeds duration threshold (0s)") || strings.Contains(stdout, "exceeds duration threshold (0ms)") {
		t.Fatalf("alert used an empty duration:\n%s", stdout)
	}
}

func longestTransactionLine(t *testing.T, stdout string) string {
	t.Helper()
	const label = "Longest transaction:"
	idx := strings.Index(stdout, label)
	if idx < 0 {
		t.Fatalf("longest-duration ranking missing:\n%s", stdout)
	}
	rest := stdout[idx:]
	if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
		rest = rest[:nl]
	}
	return rest
}
