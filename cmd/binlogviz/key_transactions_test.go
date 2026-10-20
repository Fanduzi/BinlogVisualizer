// Package binlogviz checks that text, Markdown, JSON, and HTML name one set of key transactions.
// input: analyze on committed ROW fixtures under internal/binlog/testdata.
// output: a failure when any format's key-transaction set differs, or when HTML evidence names a transaction outside that set.
// pos: the A3 check in docs/production-readiness.md. It runs under go test ./...
// note: if this file changes, update this header and that checklist row.
package binlogviz

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestAnalyzeFormatsShareKeyTransactions(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, name := range []string{
		"mysql-8.0.46-busiest-minute.binlog",
		"mysql-8.0.46-committed-duration.binlog",
		"mysql-8.0.46-dml-full.binlog",
	} {
		t.Run(name, func(t *testing.T) {
			fixture := mustFixturePath(t, name)
			sets := map[string][]string{}
			var evidence []string
			for _, format := range []string{"text", "markdown", "json", "html"} {
				stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", format, "-q")
				if err != nil {
					t.Fatalf("analyze %s: %v\n%s", format, err, stderr)
				}
				keys, extra := keyTransactions(t, format, stdout)
				sets[format] = keys
				if format == "html" {
					evidence = extra
				}
			}
			if len(sets["json"]) == 0 {
				t.Fatal("fixture produced no key transactions")
			}
			want := strings.Join(sets["json"], ",")
			for _, format := range []string{"text", "markdown", "html"} {
				if got := strings.Join(sets[format], ","); got != want {
					t.Fatalf("key transactions differ\njson:     %s\n%s: %s", want, format, got)
				}
			}
			if outside := setDifference(evidence, sets["json"]); len(outside) > 0 {
				t.Fatalf("HTML evidence names transactions outside the key set: %s", strings.Join(outside, ","))
			}
		})
	}
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
	return uniqueTxnKeys(keys), uniqueTxnKeys(evidence)
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

func uniqueTxnKeys(keys []string) []string {
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
	sort.Strings(out)
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
