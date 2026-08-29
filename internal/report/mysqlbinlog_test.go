// Package report verifies copy-paste replay commands for usable transaction spans.
// input: Transaction positions, per-transaction/report FormatDescription server versions, and renderer entry points.
// output: regression coverage for absolute paths, mixed-producer binary selection, and XID-only omission.
// pos: report-layer tests for the operator copy-paste replay contract.
// note: if this file changes, update this header and module README.md.
package report

import (
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestMysqlbinlogCmdForUsableSameFileSpan(t *testing.T) {
	txn := model.Transaction{
		TotalRows:       400000,
		EventCount:      9524,
		BinlogBytes:     77914563,
		BinlogPathStart: "/data/mysql/mysql-bin.000008",
		BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
		PositionStart:   385,
		PositionEnd:     77914948,
	}
	got := mysqlbinlogCmd(txn, "8.0.36-log")
	want := "mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=385 --stop-position=77914948 /data/mysql/mysql-bin.000008"
	if got != want {
		t.Fatalf("mysqlbinlog_cmd=%q, want %q", got, want)
	}
	loc := formatTxnEvidenceLocation(txn)
	if loc != "mysql-bin.000008:385-77914948 (78MB, 400000 rows)" &&
		loc != "/data/mysql/mysql-bin.000008:385-77914948 (78MB, 400000 rows)" {
		t.Fatalf("evidence location=%q", loc)
	}
}

func TestMysqlbinlogCmdResolvesRelativePathToAbsolute(t *testing.T) {
	txn := model.Transaction{
		TotalRows:       10,
		EventCount:      3,
		BinlogBytes:     4096,
		BinlogPathStart: "mysql-bin.000008",
		BinlogPathEnd:   "mysql-bin.000008",
		PositionStart:   385,
		PositionEnd:     1200,
	}
	got := mysqlbinlogCmd(txn, "")
	abs, err := filepath.Abs("mysql-bin.000008")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=385 --stop-position=1200 ") {
		t.Fatalf("flags/positions changed: %q", got)
	}
	if !strings.HasSuffix(got, " "+abs) {
		t.Fatalf("copy-paste command must use absolute path %q, got %q", abs, got)
	}
	if strings.HasSuffix(got, " mysql-bin.000008") {
		t.Fatalf("basename-only path is not runnable from another cwd: %q", got)
	}
}

func TestMysqlbinlogCmdUsesMariadbBinlogForMariaDBServerVersion(t *testing.T) {
	txn := model.Transaction{
		TotalRows:       400000,
		EventCount:      9524,
		BinlogBytes:     77914563,
		BinlogPathStart: "/data/mysql/mysql-bin.000008",
		BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
		PositionStart:   385,
		PositionEnd:     77914948,
	}
	got := mysqlbinlogCmd(txn, "11.4.2-MariaDB-log")
	want := "mariadb-binlog --base64-output=DECODE-ROWS -v --start-position=385 --stop-position=77914948 /data/mysql/mysql-bin.000008"
	if got != want {
		t.Fatalf("mysqlbinlog_cmd=%q, want %q", got, want)
	}
}

func TestMysqlbinlogCmdPrefersTransactionServerVersionForMixedProducers(t *testing.T) {
	txn := model.Transaction{
		ServerVersion:   "11.8.3-MariaDB-log",
		TotalRows:       2,
		EventCount:      1,
		BinlogBytes:     200,
		BinlogPathStart: "/data/mysql/mariadb-bin.000001",
		BinlogPathEnd:   "/data/mysql/mariadb-bin.000001",
		PositionStart:   100,
		PositionEnd:     300,
	}
	got := mysqlbinlogCmd(txn, "8.4.6")
	if !strings.HasPrefix(got, "mariadb-binlog ") {
		t.Fatalf("mixed-producer replay must use transaction provenance, got %q", got)
	}
}

func TestMysqlbinlogCmdOmittedForXIDOnlySpan(t *testing.T) {
	txn := model.Transaction{
		TotalRows:       400000,
		EventCount:      9524,
		BinlogBytes:     31,
		BinlogPathStart: "mysql-bin.000008",
		BinlogPathEnd:   "mysql-bin.000008",
		PositionStart:   77914917,
		PositionEnd:     77914948,
	}
	if cmd := mysqlbinlogCmd(txn, "11.4.2-MariaDB-log"); cmd != "" {
		t.Fatalf("XID-only span must not emit mysqlbinlog_cmd, got %q", cmd)
	}
}

func TestMysqlbinlogCmdCrossFileListsBothFiles(t *testing.T) {
	txn := model.Transaction{
		TotalRows:       20,
		EventCount:      4,
		BinlogBytes:     4096,
		BinlogPathStart: "/data/mysql/mysql-bin.000044",
		BinlogPathEnd:   "/data/mysql/mysql-bin.000045",
		PositionStart:   300,
		PositionEnd:     520,
	}
	got := mysqlbinlogCmd(txn, "")
	if !strings.Contains(got, "--start-position=300 /data/mysql/mysql-bin.000044") {
		t.Fatalf("missing first-file start: %q", got)
	}
	if !strings.Contains(got, "--stop-position=520 /data/mysql/mysql-bin.000045") {
		t.Fatalf("missing last-file stop: %q", got)
	}
	if strings.Contains(got, "--start-position=300 --stop-position=520 /data/mysql/mysql-bin.000044") {
		t.Fatalf("must not pretend a cross-file span is one file: %q", got)
	}
}

func TestRenderJSONIncludesMysqlbinlogCmdOnlyWhenUsable(t *testing.T) {
	forceEnglishReportLocale(t)
	usable := model.Transaction{
		TxnKey:          "txn-real",
		TotalRows:       400000,
		EventCount:      10,
		BinlogBytes:     77914563,
		BinlogPathStart: "/data/mysql/mysql-bin.000008",
		BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
		PositionStart:   385,
		PositionEnd:     77914948,
	}
	xidOnly := model.Transaction{
		TxnKey:          "txn-xid",
		TotalRows:       400000,
		EventCount:      9524,
		BinlogBytes:     31,
		BinlogPathStart: "/data/mysql/mysql-bin.000008",
		BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
		PositionStart:   77914917,
		PositionEnd:     77914948,
	}

	out, err := RenderJSON(model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{usable, xidOnly},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"mysqlbinlog_cmd": "mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=385 --stop-position=77914948 /data/mysql/mysql-bin.000008"`) {
		t.Fatalf("expected usable mysqlbinlog_cmd in JSON\n%s", out)
	}
	// Second txn is XID-only; the invented start must not appear, and cmd should be omitted for that object.
	if strings.Count(out, `"mysqlbinlog_cmd"`) != 1 {
		t.Fatalf("expected mysqlbinlog_cmd only on the usable txn, got\n%s", out)
	}
	if strings.Contains(out, "--start-position=77914917") {
		t.Fatalf("must not emit XID start as --start-position\n%s", out)
	}
}

func TestRenderJSONMariaDBServerVersionUsesMariadbBinlog(t *testing.T) {
	forceEnglishReportLocale(t)
	out, err := RenderJSON(model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			ServerVersion: "10.11.6-MariaDB-log",
			LargestTransactions: []model.Transaction{{
				TxnKey:          "txn-real",
				TotalRows:       400000,
				EventCount:      10,
				BinlogBytes:     77914563,
				BinlogPathStart: "/data/mysql/mysql-bin.000008",
				BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
				PositionStart:   385,
				PositionEnd:     77914948,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `"mysqlbinlog_cmd": "mariadb-binlog --base64-output=DECODE-ROWS -v --start-position=385 --stop-position=77914948 /data/mysql/mysql-bin.000008"`
	if !strings.Contains(out, want) {
		t.Fatalf("expected MariaDB replay binary in JSON\n%s", out)
	}
}

func TestRenderTextAndHTMLIncludeMysqlbinlogCopy(t *testing.T) {
	forceEnglishReportLocale(t)
	cmd := "mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=385 --stop-position=77914948 /data/mysql/mysql-bin.000008"
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{{
				TxnKey:          "txn-1",
				TotalRows:       400000,
				EventCount:      10,
				BinlogBytes:     77914563,
				BinlogPathStart: "/data/mysql/mysql-bin.000008",
				BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
				PositionStart:   385,
				PositionEnd:     77914948,
				Tables:          map[string]int{"dogfood_big.t": 400000},
			}},
		},
	}

	textOut, err := RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(textOut, "mysql-bin.000008:385-77914948 (78MB, 400000 rows)") {
		t.Fatalf("expected span evidence in text\n%s", textOut)
	}
	if !strings.Contains(textOut, cmd) {
		t.Fatalf("expected mysqlbinlog command in text\n%s", textOut)
	}

	htmlOut, err := RenderHTML(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(htmlOut, cmd) {
		t.Fatalf("expected mysqlbinlog command in HTML\n%s", htmlOut)
	}
	if !strings.Contains(htmlOut, `data-copy="`+cmd+`"`) {
		t.Fatal("expected HTML Copy button")
	}
}

func TestRenderTextAndHTMLMatchJSONMariadbBinlogCommand(t *testing.T) {
	forceEnglishReportLocale(t)
	cmd := "mariadb-binlog --base64-output=DECODE-ROWS -v --start-position=385 --stop-position=77914948 /data/mysql/mysql-bin.000008"
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			ServerVersion: "10.11.6-MariaDB-log",
			LargestTransactions: []model.Transaction{{
				TxnKey:          "txn-1",
				TotalRows:       400000,
				EventCount:      10,
				BinlogBytes:     77914563,
				BinlogPathStart: "/data/mysql/mysql-bin.000008",
				BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
				PositionStart:   385,
				PositionEnd:     77914948,
				Tables:          map[string]int{"dogfood_big.t": 400000},
			}},
		},
	}

	jsonOut, err := RenderJSON(result)
	if err != nil {
		t.Fatal(err)
	}
	textOut, err := RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	htmlOut, err := RenderHTML(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonOut, `"mysqlbinlog_cmd": "`+cmd+`"`) {
		t.Fatalf("JSON missing MariaDB command\n%s", jsonOut)
	}
	if !strings.Contains(textOut, cmd) {
		t.Fatalf("text missing MariaDB command\n%s", textOut)
	}
	if !strings.Contains(htmlOut, `data-copy="`+cmd+`"`) {
		t.Fatalf("HTML copy missing MariaDB command")
	}
}

func TestRenderTextDDLTimelineListsOperations(t *testing.T) {
	forceEnglishReportLocale(t)
	out, err := RenderText(model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			DDLEvents: []model.DDLEvent{
				{Operation: "CREATE DATABASE", Schema: "dogfood"},
				{Operation: "CREATE TABLE", Schema: "dogfood", Table: "users"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "DDL Timeline: 2 (CREATE DATABASE, CREATE TABLE)") {
		t.Fatalf("expected DDL timeline summary, got\n%s", out)
	}
}
