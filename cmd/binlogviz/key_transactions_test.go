// Package binlogviz checks that text, Markdown, JSON, and HTML list one ordered sequence of key transactions.
// input: analyze on committed ROW fixtures under internal/binlog/testdata, including a window that clips one transaction (partial_start evidence).
// output: a failure when any format's ordered key-transaction sequence differs from JSON, or when HTML evidence names a transaction outside it.
// pos: the A3 check in docs/production-readiness.md. It runs under go test ./...
// note: if this file changes, update this header and that checklist row.
package binlogviz

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

func TestAnalyzeFormatsShareKeyTransactions(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, tc := range []struct {
		name    string
		fixture string
		args    []string
		// incomplete requires at least one listed transaction whose evidence is not complete.
		incomplete bool
	}{
		{name: "busiest-minute", fixture: "mysql-8.0.46-busiest-minute.binlog"},
		{name: "committed-duration", fixture: "mysql-8.0.46-committed-duration.binlog"},
		{name: "dml-full", fixture: "mysql-8.0.46-dml-full.binlog"},
		// Position 384 is inside txn-1 (157-590), so txn-1 is clipped to partial_start and ranks
		// after the complete transactions despite having the most rows.
		{name: "busiest-minute-partial-start", fixture: "mysql-8.0.46-busiest-minute.binlog", args: []string{"--start-position", "384", "--top-transactions", "0"}, incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := mustFixturePath(t, tc.fixture)
			sequences := map[string][]string{}
			var evidence []string
			for _, format := range []string{"text", "markdown", "json", "html"} {
				args := append([]string{fixture}, tc.args...)
				args = append(args, "--format", format, "-q")
				stdout, stderr, err := executeAnalyzeLikeMain(t, args...)
				if err != nil {
					t.Fatalf("analyze %s: %v\n%s", format, err, stderr)
				}
				if format == "json" && tc.incomplete {
					requireIncompleteKeyTransaction(t, stdout)
				}
				keys, extra := keyTransactions(t, format, stdout)
				sequences[format] = keys
				if format == "html" {
					evidence = extra
				}
			}
			if len(sequences["json"]) == 0 {
				t.Fatal("fixture produced no key transactions")
			}
			want := strings.Join(sequences["json"], ",")
			for _, format := range []string{"text", "markdown", "html"} {
				if got := strings.Join(sequences[format], ","); got != want {
					t.Fatalf("key transactions differ (order matters)\njson:     %s\n%s: %s", want, format, got)
				}
			}
			if outside := setDifference(evidence, sequences["json"]); len(outside) > 0 {
				t.Fatalf("HTML evidence names transactions outside the key set: %s", strings.Join(outside, ","))
			}
		})
	}
}

// requireIncompleteKeyTransaction keeps the incomplete-evidence case honest: if the
// fixture or window stops clipping a transaction, the case would silently test nothing.
func requireIncompleteKeyTransaction(t *testing.T, out string) {
	t.Helper()
	var doc struct {
		Transactions []struct {
			TxnKey       string `json:"txn_key"`
			Completeness string `json:"completeness"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("json: %v", err)
	}
	for _, txn := range doc.Transactions {
		if txn.Completeness != "complete" {
			return
		}
	}
	t.Fatal("expected a listed transaction with incomplete evidence; the window no longer clips one")
}

var txnKeyPattern = regexp.MustCompile(`txn-\d+`)

func keyTransactions(t *testing.T, format, out string) (keys, evidence []string) {
	t.Helper()
	switch format {
	case "json":
		var doc struct {
			Transactions []struct {
				TxnKey string `json:"txn_key"`
			} `json:"transactions"`
		}
		if err := json.Unmarshal([]byte(out), &doc); err != nil {
			t.Fatalf("json: %v", err)
		}
		for _, txn := range doc.Transactions {
			keys = append(keys, txn.TxnKey)
		}
	case "text":
		section, ok := headedSection(out, "=== Top Transactions ===", "\n===")
		if !ok {
			t.Fatal("text report has no Top Transactions section")
		}
		keys = txnKeyPattern.FindAllString(section, -1)
	case "markdown":
		section, ok := headedSection(out, "## Top Transactions", "\n## ")
		if !ok {
			t.Fatal("markdown report has no Top Transactions section")
		}
		for _, line := range strings.Split(section, "\n") {
			if strings.HasPrefix(line, "| ") && !strings.HasPrefix(line, "| #") && !strings.HasPrefix(line, "|---") {
				keys = append(keys, txnKeyPattern.FindAllString(line, -1)...)
			}
		}
	case "html":
		lookup, ok := headedSection(out, `id="transaction-lookup"`, `id="transaction-evidence"`)
		if !ok {
			lookup, ok = headedSection(out, `id="transaction-lookup"`, `id="ddl-timeline"`)
		}
		if !ok {
			t.Fatal("html report has no transaction lookup")
		}
		keys = htmlTxnKeys(lookup)
		if evidenceSection, ok := headedSection(out, `id="transaction-evidence"`, `id="ddl-timeline"`); ok {
			evidence = htmlTxnKeys(evidenceSection)
		}
	default:
		t.Fatalf("unknown format %s", format)
	}
	return orderedTxnKeys(keys), orderedTxnKeys(evidence)
}

func htmlTxnKeys(section string) []string {
	const marker = `data-transaction-key="`
	var keys []string
	rest := section
	for {
		i := strings.Index(rest, marker)
		if i < 0 {
			return keys
		}
		rest = rest[i+len(marker):]
		end := strings.IndexByte(rest, '"')
		if end < 0 {
			return keys
		}
		keys = append(keys, rest[:end])
		rest = rest[end+1:]
	}
}

func headedSection(s, heading, next string) (string, bool) {
	i := strings.Index(s, heading)
	if i < 0 {
		return "", false
	}
	rest := s[i+len(heading):]
	if j := strings.Index(rest, next); j >= 0 {
		rest = rest[:j]
	}
	return rest, true
}

// orderedTxnKeys keeps the first appearance of each key, in report order. A text row can
// name its key more than once (a champion label and the row itself); order is not sorted
// away, so a format that reorders the list fails the comparison.
func orderedTxnKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func setDifference(left, right []string) []string {
	have := make(map[string]struct{}, len(right))
	for _, key := range right {
		have[key] = struct{}{}
	}
	var out []string
	for _, key := range left {
		if _, ok := have[key]; !ok {
			out = append(out, key)
		}
	}
	return out
}
