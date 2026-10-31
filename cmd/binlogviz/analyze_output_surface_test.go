// Package binlogviz verifies the analyze csv/tsv formats and the
// statement-fingerprint surface end to end on a real fixture.
// input: analyze runs over mysql-8.0.46-dml-minimal.binlog (Rows_query on).
// output: parseable CSV/TSV datasets on stdout, an opt-in fingerprint text
// section, and usage errors for invalid flag combinations (ADR 0026).
// pos: CLI regression for the --format/--csv-dataset/--show-fingerprints
// plumbing in analyze.go and analyze_options.go.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
)

func TestAnalyzeCSVDatasetRequiresDelimitedFormat(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, _, stderr := runRoot(t, "analyze",
		"--format", "json", "--csv-dataset", "tables",
		mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"))
	if code != 3 {
		t.Fatalf("exit %d, want 3 (usage)\nstderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "--csv-dataset requires --format csv or tsv") {
		t.Fatalf("stderr %q", stderr)
	}
}

func TestAnalyzeCSVDatasetRejectsUnknownName(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, _, stderr := runRoot(t, "analyze",
		"--format", "csv", "--csv-dataset", "minutes",
		mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"))
	if code != 3 {
		t.Fatalf("exit %d, want 3 (usage)\nstderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, `invalid --csv-dataset "minutes"`) {
		t.Fatalf("stderr %q", stderr)
	}
}

func TestAnalyzeCSVDatasetRejectsEmptyValue(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, _, stderr := runRoot(t, "analyze",
		"--format", "csv", "--csv-dataset", "",
		mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"))
	if code != 3 {
		t.Fatalf("exit %d, want 3 (usage)\nstderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, `invalid --csv-dataset ""`) {
		t.Fatalf("stderr %q", stderr)
	}
}

func TestAnalyzeCSVTablesWritesParseableRows(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, stdout, stderr := runRoot(t, "analyze",
		"--format", "csv",
		mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"))
	if code != 0 {
		t.Fatalf("exit %d\nstderr=%s", code, stderr)
	}
	rows, err := csv.NewReader(strings.NewReader(stdout)).ReadAll()
	if err != nil {
		t.Fatalf("csv does not parse: %v\n%s", err, stdout)
	}
	if len(rows) < 2 || rows[0][0] != "schema" || rows[0][2] != "total_rows" {
		t.Fatalf("tables dataset = %v", rows)
	}
	var orders []string
	for _, r := range rows[1:] {
		if r[0] == "shop" && r[1] == "orders" {
			orders = r
		}
	}
	if orders == nil || orders[2] == "0" {
		t.Fatalf("shop.orders row = %v", orders)
	}
}

func TestAnalyzeCSVFingerprintsMatchesJSONRanking(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog")

	code, csvOut, stderr := runRoot(t, "analyze",
		"--format", "csv", "--csv-dataset", "fingerprints", fixture)
	if code != 0 {
		t.Fatalf("csv exit %d\nstderr=%s", code, stderr)
	}
	code, jsonOut, stderr := runRoot(t, "analyze",
		"--format", "json", fixture)
	if code != 0 {
		t.Fatalf("json exit %d\nstderr=%s", code, stderr)
	}

	rows, err := csv.NewReader(strings.NewReader(csvOut)).ReadAll()
	if err != nil {
		t.Fatalf("csv does not parse: %v", err)
	}
	if rows[0][0] != "fingerprint" {
		t.Fatalf("fingerprints header = %v", rows[0])
	}
	var doc struct {
		Fingerprints []struct {
			Fingerprint string `json:"fingerprint"`
			Rows        int    `json:"rows"`
		} `json:"statement_fingerprints"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &doc); err != nil {
		t.Fatal(err)
	}
	// The CSV dataset lists the same bounded ranking the JSON list carries.
	if len(rows)-1 != len(doc.Fingerprints) {
		t.Fatalf("csv rows %d != json fingerprints %d", len(rows)-1, len(doc.Fingerprints))
	}
	for i, fp := range doc.Fingerprints {
		if rows[i+1][0] != fp.Fingerprint {
			t.Fatalf("row %d fingerprint %q != json %q", i, rows[i+1][0], fp.Fingerprint)
		}
	}
}

func TestAnalyzeShowFingerprintsTextSection(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, stdout, stderr := runRoot(t, "analyze",
		"--show-fingerprints",
		mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"))
	if code != 0 {
		t.Fatalf("exit %d\nstderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "Statement Fingerprints") {
		t.Fatalf("text section missing:\n%s", stdout)
	}
	if !strings.Contains(stdout, "delete from shop.orders where id in (?)") {
		t.Fatalf("normalized DELETE fingerprint missing:\n%s", stdout)
	}
}

func TestAnalyzeShowFingerprintsAbsentNote(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, stdout, stderr := runRoot(t, "analyze",
		"--show-fingerprints",
		mustFixturePath(t, "mysql-8.0.46-check-table.binlog"))
	if code != 0 {
		t.Fatalf("exit %d\nstderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "no Rows_query statement text captured") {
		t.Fatalf("explicit absent note missing:\n%s", stdout)
	}
}

func TestAnalyzeDetailsEnablesFingerprintSection(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, stdout, stderr := runRoot(t, "analyze",
		"--details",
		mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"))
	if code != 0 {
		t.Fatalf("exit %d\nstderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "Statement Fingerprints") {
		t.Fatalf("--details must enable the fingerprint section:\n%s", stdout)
	}
}
