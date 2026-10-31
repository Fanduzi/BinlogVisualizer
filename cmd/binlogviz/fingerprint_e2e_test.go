// Package binlogviz runs the analyze fingerprint + csv/tsv surface against a
// live MySQL 8.0, gated on BINLOGVIZ_FLASHBACK_E2E like the other server
// tests (ADR 0026).
// input: a local MySQL 8.0 with FULL row image/metadata and
// binlog_rows_query_log_events=ON — the server emits Rows_query statement
// text the normalizer must digest.
// output: analyze --format json carries statement_fingerprints whose rows
// cover the incident rows, and --format csv --csv-dataset fingerprints
// emits the same ranking as parseable CSV.
// pos: live-server e2e for statement fingerprinting and delimited output;
// unit and fixture tests own the normalizer and emitter details.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestAnalyzeFingerprintsMySQL80 proves the real parse path — live binlog
// Rows_query events through executeAnalyzeLikeMain — produces normalized
// fingerprints, not just fixture bytes. Rows_query text is a server feature
// flag, so the e2e also guards that the captured binlog carried it.
func TestAnalyzeFingerprintsMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to run against a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}
	ver := mysqlVersion(e2eMySQL(t, "SELECT @@version"))
	if !strings.HasPrefix(ver, "8.0") {
		t.Fatalf("MySQL 8.0 required for binlog_row_metadata=FULL, got %s", ver)
	}
	// Rows_query emission is a dynamic global — force it so the incident
	// window carries statement text even on a stock server config.
	got := mysqlBatchValue(e2eMySQL(t, `
SET GLOBAL binlog_rows_query_log_events = ON;
SELECT @@binlog_rows_query_log_events`))
	lines := strings.Split(got, "\n")
	if lines[len(lines)-1] != "1" {
		t.Fatalf("server refused binlog_rows_query_log_events=ON: %q", got)
	}
	e2eMySQL(t, `
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
SET GLOBAL binlog_row_metadata = 'FULL';`)
	e2eMySQL(t, `
DROP DATABASE IF EXISTS fpt;
CREATE DATABASE fpt;
CREATE TABLE fpt.t (id INT NOT NULL PRIMARY KEY, note VARCHAR(32) NULL);
FLUSH LOGS`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS fpt;")
	})

	e2eMySQL(t, `
INSERT INTO fpt.t VALUES (1, 'secret-a'), (2, 'secret-b');
UPDATE fpt.t SET note = 'secret-c' WHERE id = 1;
DELETE FROM fpt.t WHERE id = 2;`)
	path := e2eIncidentBinlog(t)

	stdout, stderr, err := executeAnalyzeLikeMain(t, path,
		"--include-schema", "fpt", "--format", "json")
	if err != nil {
		t.Fatalf("analyze json: %v\n%s", err, stderr)
	}
	var doc struct {
		SQLContext struct {
			Available bool `json:"available"`
		} `json:"sql_context"`
		Summary struct {
			TotalRows int `json:"total_rows"`
		} `json:"summary"`
		Fingerprints []struct {
			Fingerprint string         `json:"fingerprint"`
			Statements  int            `json:"statements"`
			Rows        int            `json:"rows"`
			Tables      map[string]int `json:"tables"`
			Sample      string         `json:"sample"`
		} `json:"statement_fingerprints"`
	}
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("json parse: %v\n%s", err, stdout)
	}
	if !doc.SQLContext.Available {
		t.Fatalf("live binlog lost Rows_query text:\n%s", stdout)
	}
	if doc.Summary.TotalRows != 4 {
		t.Fatalf("total_rows = %d, want 4", doc.Summary.TotalRows)
	}
	var rowsSeen int
	var foundInsert, foundUpdate, foundDelete bool
	for _, fp := range doc.Fingerprints {
		rowsSeen += fp.Rows
		if fp.Sample == "" {
			t.Fatalf("fingerprint %q has no sample", fp.Fingerprint)
		}
		// The fingerprint is literal-free; the sample keeps the real text.
		if strings.Contains(fp.Fingerprint, "secret") {
			t.Fatalf("literal leaked into fingerprint:\n%s", fp.Fingerprint)
		}
		switch {
		case strings.Contains(fp.Fingerprint, "insert"):
			foundInsert = true
			if !strings.Contains(fp.Fingerprint, "values (?)") {
				t.Fatalf("multi-row VALUES not collapsed: %q", fp.Fingerprint)
			}
		case strings.Contains(fp.Fingerprint, "update"):
			foundUpdate = true
		case strings.Contains(fp.Fingerprint, "delete"):
			foundDelete = true
		}
		if len(fp.Tables) == 0 || fp.Tables["fpt.t"] == 0 {
			t.Fatalf("fingerprint %q tables %v, want fpt.t", fp.Fingerprint, fp.Tables)
		}
	}
	if !foundInsert || !foundUpdate || !foundDelete {
		t.Fatalf("missing DML shapes (i=%v u=%v d=%v):\n%s",
			foundInsert, foundUpdate, foundDelete, stdout)
	}
	if rowsSeen != doc.Summary.TotalRows {
		t.Fatalf("fingerprint rows %d != total_rows %d", rowsSeen, doc.Summary.TotalRows)
	}

	// The delimited dataset carries the same bounded ranking.
	stdout, stderr, err = executeAnalyzeLikeMain(t, path,
		"--include-schema", "fpt", "--format", "csv", "--csv-dataset", "fingerprints")
	if err != nil {
		t.Fatalf("analyze csv: %v\n%s", err, stderr)
	}
	rows, err := csv.NewReader(strings.NewReader(stdout)).ReadAll()
	if err != nil || len(rows) != len(doc.Fingerprints)+1 {
		t.Fatalf("csv rows %d (err=%v) != fingerprints %d",
			len(rows), err, len(doc.Fingerprints))
	}
	if rows[0][0] != "fingerprint" || rows[1][0] != doc.Fingerprints[0].Fingerprint {
		t.Fatalf("csv header/top = %v", rows)
	}
}
