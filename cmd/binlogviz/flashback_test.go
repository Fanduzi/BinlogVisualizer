package binlogviz

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestFlashbackSQLOnMySQL80FullFixture(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	stdout, stderr, err := executeFlashbackLikeMain(t, path)
	if err != nil {
		t.Fatalf("flashback: %v\n%s", err, stderr)
	}
	if strings.Count(stderr, "Error:") != 0 {
		t.Fatalf("stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "shop.wide") || !strings.Contains(stderr, "shop.heap") || !strings.Contains(stderr, "generated columns cannot be ruled out") {
		t.Fatalf("incident fixture warning:\n%s", stderr)
	}
	for _, want := range []string{
		"SET NAMES utf8mb4;",
		"SET time_zone = '+00:00';",
		"-9223372036854775808",
		"18446744073709551615",
		"4294967295",
		"-2147483648",
		"-123456789012.3400",
		"2026-10-06 14:05:01.123456",
		"2026-10-06 14:05:01.500000",
		`it\'s "bad" \\ 雪`,
		"X'DEADBEEFFF00'",
		"X'00FFFE'",
		"JSON_OBJECT(",
		", 2, 5)",
		"WHERE `id` <=> 30 AND `bucket` <=> 9",
		"-- no primary key on shop.heap; this matches every column and LIMIT 1",
		"LIMIT 1;",
		"-- gtid: ",
		"-- binlog: mysql-8.0.46-flashback-full.binlog:",
		"START TRANSACTION;",
		"COMMIT;",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("missing %q\n%s", want, stdout)
		}
	}
	if strings.Count(stdout, "START TRANSACTION;") != 3 {
		t.Fatalf("transactions:\n%s", stdout)
	}
	undoLast := strings.Index(stdout, "DELETE FROM `shop`.`heap` WHERE `id` <=> 5")
	undoDelete := strings.Index(stdout, "18446744073709551615")
	if undoLast < 0 || undoDelete < 0 || undoLast > undoDelete {
		t.Fatalf("reverse order:\n%s", stdout)
	}
	if strings.Contains(stdout, "CREATE ") || strings.Contains(stdout, "ALTER ") || strings.Contains(stdout, "DROP ") {
		t.Fatalf("emitted DDL:\n%s", stdout)
	}

	onlyDelete, deleteErr, err := executeFlashbackLikeMain(t, path, "--dml", "delete")
	if err != nil {
		t.Fatalf("dml delete: %v", err)
	}
	if strings.Count(onlyDelete, "START TRANSACTION;") != 1 || !strings.Contains(onlyDelete, "18446744073709551615") || strings.Contains(onlyDelete, "`id` <=> 10") {
		t.Fatalf("dml delete kept the wrong rows:\n%s", onlyDelete)
	}
	if strings.Contains(deleteErr, "only partly undone") {
		t.Fatalf("dml delete warned:\n%s", deleteErr)
	}

	heap, heapErr, err := executeFlashbackLikeMain(t, path, "--include-table", "shop.heap")
	if err != nil {
		t.Fatalf("heap: %v", err)
	}
	if !strings.Contains(heapErr, "only partly undone") {
		t.Fatalf("split warning missing:\n%s", heapErr)
	}
	if strings.Contains(heap, "shop`.`wide") || !strings.Contains(heap, "shop`.`heap") {
		t.Fatalf("table filter:\n%s", heap)
	}

	pos := flashbackBinlogPos(t, stdout)
	narrow, _, err := executeFlashbackLikeMain(t, path, "--start-position", strconv.FormatInt(pos, 10))
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if strings.Count(narrow, "START TRANSACTION;") != 1 || !strings.Contains(narrow, "`id` <=> 5") || strings.Contains(narrow, "18446744073709551615") {
		t.Fatalf("position window:\n%s", narrow)
	}
}

func TestFlashbackSchemaFileClearsGeneratedWarning(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	schema := filepath.Join(t.TempDir(), "wide.sql")
	if err := os.WriteFile(schema, []byte("USE `shop`;\n"+shopWideCreateSQL), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := executeFlashbackLikeMain(t, path, "--include-table", "shop.wide", "--schema-file", schema)
	if err != nil {
		t.Fatalf("schema file: %v\n%s", err, stderr)
	}
	if strings.Contains(stderr, "generated columns cannot be ruled out") || !strings.Contains(stdout, "18446744073709551615") {
		t.Fatalf("stdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
}

func TestFlashbackSchemaFileRefusesMismatch(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	dir := t.TempDir()
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "missing",
			body: "USE `shop`;\nCREATE TABLE `wide` (\n  `id` bigint NOT NULL,\n  `bucket` int NOT NULL,\n  PRIMARY KEY (`id`, `bucket`)\n);\n",
			want: "missing",
		},
		{
			name: "extra",
			body: "USE `shop`;\n" + strings.Replace(shopWideCreateSQL, "  PRIMARY KEY (id, bucket)\n", "  extra_col INT NULL,\n  PRIMARY KEY (id, bucket)\n", 1),
			want: "extra extra_col",
		},
		{
			name: "reordered",
			body: "USE `shop`;\n" + strings.Replace(shopWideCreateSQL, "  note VARCHAR(255) NULL,\n  raw BLOB NULL,\n", "  raw BLOB NULL,\n  note VARCHAR(255) NULL,\n", 1),
			want: "reordered",
		},
		{
			name: "generated",
			body: "USE `shop`;\n" + strings.Replace(shopWideCreateSQL, "  note VARCHAR(255) NULL,\n", "  note VARCHAR(255) GENERATED ALWAYS AS (1) VIRTUAL,\n", 1),
			want: "note is generated",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := filepath.Join(dir, tc.name+".sql")
			if err := os.WriteFile(schema, []byte(tc.body), 0o644); err != nil {
				t.Fatal(err)
			}
			stdout, stderr, err := executeFlashbackLikeMain(t, path, "--include-table", "shop.wide", "--schema-file", schema)
			assertFlashbackRefused(t, stdout, stderr, err, "shop.wide")
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}

// shopWideCreateSQL is the incident-time shop.wide definition from the MySQL 8.0.46 flashback fixture.
const shopWideCreateSQL = `CREATE TABLE shop.wide (
  id BIGINT NOT NULL,
  bucket INT NOT NULL,
  i_tiny TINYINT NULL,
  i_tiny_u TINYINT UNSIGNED NULL,
  i_small SMALLINT NULL,
  i_int INT NULL,
  i_int_u INT UNSIGNED NULL,
  i_big BIGINT NULL,
  i_big_u BIGINT UNSIGNED NULL,
  qty INT NULL,
  price DECIMAL(18,4) NULL,
  created_at DATETIME(6) NULL,
  updated_at TIMESTAMP(6) NULL,
  note VARCHAR(255) NULL,
  raw BLOB NULL,
  bits VARBINARY(16) NULL,
  payload JSON NULL,
  color ENUM('red','blue','green') NULL,
  flags SET('a','b','c') NULL,
  PRIMARY KEY (id, bucket)
);
`

func TestFlashbackRefusesIncompleteMetadataImageAndDDL(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	minimal := mustFixturePath(t, "mysql-8.0.46-flashback-minimal-image.binlog")
	stdout, stderr, err := executeFlashbackLikeMain(t, minimal)
	assertFlashbackRefused(t, stdout, stderr, err, "shop.wide: row image is not FULL (before- or after-image is incomplete)")

	names := mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog")
	stdout, stderr, err = executeFlashbackLikeMain(t, names, "--include-gtids", "f4d3a60a-c1de-11f1-a30a-822b383dbcd0:4")
	assertFlashbackRefused(t, stdout, stderr, err, "shop.orders: column names are unavailable (binlog_row_metadata is not FULL)")

	ddl := mustFixturePath(t, "mysql-8.0.46-index-ddl.binlog")
	stdout, stderr, err = executeFlashbackLikeMain(t, ddl)
	assertFlashbackRefused(t, stdout, stderr, err, "idxbug: DDL in the selected range (CREATE DATABASE idxbug); flashback does not undo schema changes")

	full := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	stdout, stderr, err = executeFlashbackLikeMain(t, full, "--sql-context", "off")
	assertFlashbackRefused(t, stdout, stderr, err, "flashback prints row values; --sql-context off refuses that")
}

func TestFlashbackNothingSelectedUsesAnalyzeExit(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")

	stdout, stderr, err := executeFlashbackLikeMain(t, path, "--include-table", "shop.missing")
	assertAnalyzeNoDataExit(t, stdout, stderr, err, "schema/table filter matched no events")

	stdout, stderr, err = executeFlashbackLikeMain(t, path, "--include-gtids", "00000000-0000-0000-0000-000000000000:1")
	assertAnalyzeNoDataExit(t, stdout, stderr, err, "window matched 0 events")

	stdout, stderr, err = executeFlashbackLikeMain(t, path, "--dml", "insert", "--include-table", "shop.missing")
	assertAnalyzeNoDataExit(t, stdout, stderr, err, "dml filter matched no events")
}

func TestAnalyzeOutputDoesNotIncludeFlashbackSQL(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	stdout, stderr, err := executeAnalyzeLikeMain(t, path, "--format", "text")
	if err != nil {
		t.Fatalf("analyze: %v\n%s", err, stderr)
	}
	if strings.Contains(stdout, "flashback reverses") || strings.Contains(stdout, "SET NAMES utf8mb4") || strings.Contains(stdout, "START TRANSACTION;") {
		t.Fatalf("analyze printed undo SQL:\n%s", stdout)
	}
	_, stderr, err = executeAnalyzeLikeMain(t, path, "--include-table", "shop.heap", "--format", "text")
	if err != nil {
		t.Fatalf("analyze heap: %v\n%s", err, stderr)
	}
	if strings.Contains(stderr, "only partly undone") {
		t.Fatalf("analyze warned:\n%s", stderr)
	}
}

func TestFlashbackHelpWording(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	stdout, stderr, err := executeFlashbackLikeMain(t, "--help")
	if err != nil {
		t.Fatalf("help: %v\n%s", err, stderr)
	}
	out := stdout + stderr
	if strings.Contains(out, "Only count these ROW kinds") || strings.Contains(out, "Only analyze") || strings.Contains(out, "allow-unverified-generated") || !strings.Contains(out, "Only undo these ROW kinds") || !strings.Contains(out, "Only undo these schemas") || !strings.Contains(out, "Only undo these tables") || !strings.Contains(out, "SQL file of table definitions") || !strings.Contains(out, "Print SQL that reverses selected ROW changes") || !strings.Contains(out, "checks that each omitted generated column is generated on the target") {
		t.Fatalf("english help:\n%s", out)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"--lang", "zh-CN", "flashback", "--help"})
	stdout, stderr, err = captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("zh help: %v\n%s", err, stderr)
	}
	out = stdout + stderr
	for _, want := range []string{
		"只撤销这些 ROW 类型",
		"打印 SQL，把选中的 ROW 变更撤回去",
		"只撤销这些 schema",
		"只撤销这些表",
		"起始位点",
		"结束位点",
		"只撤销匹配该 GTID 集合的完整事务",
		"跳过匹配该 GTID 集合的完整事务",
		"表定义 SQL 文件",
		"始终离线",
		"无法计算生成列表达式",
		"成员序号 / 位掩码",
		"用法:",
		"选项:",
		"全局选项:",
		"输出语言",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("zh help missing %q:\n%s", want, out)
		}
	}
	for _, banned := range []string{"Only analyze", "Start position", "Flags:", "Global Flags:", "Usage:", "Omit a generated column"} {
		if strings.Contains(out, banned) {
			t.Fatalf("zh help still has %q:\n%s", banned, out)
		}
	}
}

func executeFlashbackLikeMain(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.SetArgs(append([]string{"flashback"}, args...))
	return captureStdoutStderrRun(t, func() error {
		err := cmd.Execute()
		if err != nil {
			PrintCommandError(os.Stderr, err)
		}
		return err
	})
}

func assertFlashbackRefused(t *testing.T, stdout, stderr string, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected refusal %q, stdout=%q stderr=%q", want, stdout, stderr)
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit %d, want 1; err=%v", got, err)
	}
	if stdout != "" {
		t.Fatalf("refusal printed SQL:\n%s", stdout)
	}
	assertNoUsageDump(t, stderr)
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("Error count, stderr:\n%s", stderr)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %v, want %q", err, want)
	}
}

func flashbackBinlogPos(t *testing.T, sql string) int64 {
	t.Helper()
	const marker = "-- binlog: mysql-8.0.46-flashback-full.binlog:"
	i := strings.Index(sql, marker)
	if i < 0 {
		t.Fatalf("missing binlog comment:\n%s", sql)
	}
	rest := sql[i+len(marker):]
	end := strings.IndexByte(rest, '\n')
	if end < 0 {
		end = len(rest)
	}
	pos, err := strconv.ParseInt(strings.TrimSpace(rest[:end]), 10, 64)
	if err != nil || pos <= 0 {
		t.Fatalf("position %q: %v", rest[:end], err)
	}
	return pos
}

func TestFlashbackRoundTripMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL 8.0 server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}

	vars := e2eMySQL(t, "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@gtid_mode, @@version")
	fields := strings.Fields(vars)
	if len(fields) < 6 || fields[0] != "1" || fields[1] != "ROW" || fields[2] != "FULL" || fields[3] != "FULL" || fields[4] != "ON" || !strings.HasPrefix(fields[5], "8.0") {
		t.Fatalf("server is not MySQL 8.0 ROW/GTID/FULL: %s", vars)
	}

	mode := e2eMySQL(t, "SELECT @@SESSION.sql_mode")
	if !strings.Contains(mode, "STRICT_TRANS_TABLES") && !strings.Contains(mode, "STRICT_ALL_TABLES") {
		t.Fatalf("server sql_mode is not strict: %s", mode)
	}

	const checksumSQL = "CHECKSUM TABLE shop.wide, shop.heap, shop.jdoc, shop.jheap, shop.chars, shop.gen, shop.es, shop.cj, shop.yearnum"
	e2eMySQL(t, "RESET MASTER")
	e2eMySQL(t, readTestdata(t, "flashback_setup.sql"))
	beforeSum := e2eMySQL(t, checksumSQL)
	beforeRows := e2eMySQL(t, flashbackOrderSQL)
	genBefore := e2eMySQL(t, "CHECKSUM TABLE shop.gen")
	cjBefore := e2eMySQL(t, "CHECKSUM TABLE shop.cj")
	esBefore := e2eMySQL(t, "CHECKSUM TABLE shop.es")
	yearBefore := e2eMySQL(t, "CHECKSUM TABLE shop.yearnum")
	executed := e2eGTIDSet(e2eMySQL(t, "SELECT @@GLOBAL.gtid_executed"))
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, readTestdata(t, "flashback_incident.sql"))
	afterSum := e2eMySQL(t, checksumSQL)
	if afterSum == beforeSum {
		t.Fatalf("incident did not change checksums:\n%s", afterSum)
	}
	e2eMySQL(t, "FLUSH LOGS")

	datadir := strings.TrimSpace(e2eMySQL(t, "SELECT @@datadir"))
	names := binlogNames(t, e2eMySQL(t, "SHOW BINARY LOGS"))
	dir := t.TempDir()
	closed := make([]string, 0, len(names)-1)
	for _, name := range names[:len(names)-1] {
		dst := dir + "/" + name
		e2eCopy(t, datadir+"/"+name, dst)
		closed = append(closed, dst)
	}
	incidentPath := closed[len(closed)-1]
	if !binlogHasEventType(t, incidentPath, transactionPayloadEventType) {
		t.Fatal("incident binlog has no TRANSACTION_PAYLOAD_EVENT; binlog_transaction_compression did not wrap the JSON delete")
	}

	warnSQL, warnErr, err := executeFlashbackLikeMain(t, incidentPath, "--include-table", "shop.gen")
	if err != nil {
		t.Fatalf("gen warning path: %v\n%s", err, warnErr)
	}
	if !strings.Contains(warnErr, "shop.gen") || !strings.Contains(warnErr, "generated columns cannot be ruled out") || !strings.Contains(warnErr, "--schema-file") {
		t.Fatalf("gen warning:\n%s", warnErr)
	}
	if !strings.Contains(warnSQL, "`virt`") || !strings.Contains(warnSQL, "`stor`") {
		t.Fatalf("warning path dropped generated columns:\n%s", warnSQL)
	}

	schemaPath := filepath.Join(dir, "gen.sql")
	schemaBody := "USE `shop`;\n" + e2eMySQL(t, "SHOW CREATE TABLE shop.gen")
	if !strings.Contains(schemaBody, "GENERATED") {
		t.Fatalf("SHOW CREATE TABLE shop.gen:\n%s", schemaBody)
	}
	if err := os.WriteFile(schemaPath, []byte(schemaBody), 0o644); err != nil {
		t.Fatal(err)
	}
	genSQL, genErr, err := executeFlashbackLikeMain(t, incidentPath, "--include-table", "shop.gen", "--schema-file", schemaPath)
	if err != nil {
		t.Fatalf("gen schema file: %v\n%s\nschema:\n%s", err, genErr, schemaBody)
	}
	if strings.Contains(genErr, "generated columns cannot be ruled out") || strings.Contains(genSQL, "`virt`") || strings.Contains(genSQL, "`stor`") {
		t.Fatalf("schema file sql:\n%s\nstderr:\n%s", genSQL, genErr)
	}

	cjSQL, cjErr, err := executeFlashbackLikeMain(t, incidentPath, "--include-table", "shop.cj")
	if err != nil {
		t.Fatalf("compressed json: %v\n%s", err, cjErr)
	}
	if !strings.Contains(cjSQL, "JSON_OBJECT(") || strings.Contains(cjErr, "JSON") {
		t.Fatalf("compressed json sql:\n%s\nstderr:\n%s", cjSQL, cjErr)
	}

	esSQL, esErr, err := executeFlashbackLikeMain(t, incidentPath, "--include-table", "shop.es")
	if err != nil {
		t.Fatalf("enum: %v\n%s", err, esErr)
	}
	if !utf8.ValidString(esSQL) || strings.Contains(esSQL, "café") || strings.Contains(esSQL, "thé") || strings.Contains(esSQL, "中文") || !strings.Contains(esSQL, "INSERT INTO `shop`.`es`") {
		t.Fatalf("enum sql:\n%s", esSQL)
	}

	sql, stderr, err := executeFlashbackLikeMain(t, append(closed, "--exclude-gtids", executed)...)
	if err != nil {
		t.Fatalf("flashback exclude %s: %v\n%s", executed, err, stderr)
	}
	if !strings.Contains(sql, "18446744073709551615") || !strings.Contains(sql, "4000000000") || !strings.Contains(sql, "3000000000") || strings.Contains(sql, "-294967296") || strings.Contains(sql, "-1294967296") || !strings.Contains(sql, "JSON_OBJECT(") || !strings.Contains(sql, "_latin1 ") || !strings.Contains(sql, "_utf16 ") {
		t.Fatalf("live flashback SQL:\n%s", sql)
	}
	if strings.Contains(sql, "`virt`") || strings.Contains(sql, "`stor`") {
		t.Fatalf("wrote generated columns:\n%s", sql)
	}
	e2eMySQL(t, sql)
	restored := e2eMySQL(t, checksumSQL)
	if restored != beforeSum {
		t.Fatalf("checksum\nbefore:\n%s\nafter incident:\n%s\nrestored:\n%s", beforeSum, afterSum, restored)
	}
	rows := e2eMySQL(t, flashbackOrderSQL)
	if rows != beforeRows {
		t.Fatalf("ordered rows\nbefore:\n%s\nrestored:\n%s", beforeRows, rows)
	}

	e2eMySQL(t, `
START TRANSACTION;
UPDATE shop.gen SET base = 11 WHERE id = 1;
DELETE FROM shop.gen WHERE id = 2;
INSERT INTO shop.gen (id, base) VALUES (4, 40);
COMMIT;`)
	e2eMySQL(t, genSQL)
	if got := e2eMySQL(t, "CHECKSUM TABLE shop.gen"); got != genBefore {
		t.Fatalf("gen checksum\nbefore:\n%s\nrestored:\n%s", genBefore, got)
	}

	e2eMySQL(t, "DELETE FROM shop.cj")
	e2eMySQL(t, cjSQL)
	if got := e2eMySQL(t, "CHECKSUM TABLE shop.cj"); got != cjBefore {
		t.Fatalf("cj checksum\nbefore:\n%s\nrestored:\n%s", cjBefore, got)
	}

	e2eMySQL(t, "DELETE FROM shop.es")
	e2eMySQL(t, esSQL)
	if got := e2eMySQL(t, "CHECKSUM TABLE shop.es"); got != esBefore {
		t.Fatalf("strict es checksum\nbefore:\n%s\nrestored:\n%s", esBefore, got)
	}
	e2eMySQL(t, "DELETE FROM shop.es")
	e2eMySQL(t, "SET SESSION sql_mode='';\n"+esSQL)
	if got := e2eMySQL(t, "CHECKSUM TABLE shop.es"); got != esBefore {
		t.Fatalf("non-strict es checksum\nbefore:\n%s\nrestored:\n%s", esBefore, got)
	}

	yearSQL, yearErr, err := executeFlashbackLikeMain(t, incidentPath, "--include-table", "shop.yearnum")
	if err != nil {
		t.Fatalf("year: %v\n%s", err, yearErr)
	}
	for _, needle := range []string{"4000000000", "3000000000", "18446744073709551615", "-9223372036854775808", "255", "-32768", "-12.5"} {
		if !strings.Contains(yearSQL, needle) {
			t.Fatalf("year sql missing %s:\n%s", needle, yearSQL)
		}
	}
	for _, bad := range []string{"-294967296", "-1294967296"} {
		if strings.Contains(yearSQL, bad) {
			t.Fatalf("year sql still has shifted value %s:\n%s", bad, yearSQL)
		}
	}
	yearSchema := filepath.Join(t.TempDir(), "yearnum.sql")
	yearSchemaBody := "USE `shop`;\n" + e2eMySQL(t, "SHOW CREATE TABLE shop.yearnum")
	if err := os.WriteFile(yearSchema, []byte(yearSchemaBody), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, yearSchemaErr, err := executeFlashbackLikeMain(t, incidentPath, "--include-table", "shop.yearnum", "--schema-file", yearSchema); err != nil {
		t.Fatalf("year schema file: %v\n%s\n%s", err, yearSchemaErr, yearSchemaBody)
	}
	for _, mode := range []struct{ name, prefix string }{
		{name: "strict", prefix: ""},
		{name: "non-strict", prefix: "SET SESSION sql_mode='';\n"},
	} {
		e2eMySQL(t, mode.prefix+yearIncidentReplay)
		if got := e2eMySQL(t, "CHECKSUM TABLE shop.yearnum"); got == yearBefore {
			t.Fatalf("%s year incident did not change checksum:\n%s", mode.name, got)
		}
		e2eMySQL(t, mode.prefix+yearSQL)
		if got := e2eMySQL(t, "CHECKSUM TABLE shop.yearnum"); got != yearBefore {
			t.Fatalf("%s year checksum\nbefore:\n%s\nrestored:\n%s\nsql:\n%s", mode.name, yearBefore, got, yearSQL)
		}
	}

	flashbackSchemaMatchE2E(t)
	flashbackSchemaLayoutE2E(t)
	flashbackEnumZeroE2E(t)
	flashbackGeneratedExprE2E(t)
	flashbackGeneratedLossE2E(t)
	flashbackTargetGuardE2E(t)
	flashbackDecimalGeneratedE2E(t)
}

// flashbackSchemaMatchE2E refuses a schema file that is newer than the incident table.
// The CREATE stays in the previous binlog, so the file is the only definition.
func flashbackSchemaMatchE2E(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP TABLE IF EXISTS shop.stale;
CREATE TABLE shop.stale (
  id INT NOT NULL,
  a INT NULL,
  c INT NULL,
  PRIMARY KEY (id)
);
INSERT INTO shop.stale VALUES (1, 10, 99);`)
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, "DELETE FROM shop.stale WHERE id = 1")
	path := e2eIncidentBinlog(t)
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "generated",
			body: "USE `shop`;\nCREATE TABLE `stale` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  `c` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n  PRIMARY KEY (`id`)\n);\n",
			want: "c is generated",
		},
		{
			name: "extra",
			body: "USE `shop`;\nCREATE TABLE `stale` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  `c` int DEFAULT NULL,\n  `d` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
			want: "extra d",
		},
		{
			name: "missing",
			body: "USE `shop`;\nCREATE TABLE `stale` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
			want: "missing c",
		},
		{
			name: "reordered",
			body: "USE `shop`;\nCREATE TABLE `stale` (\n  `id` int NOT NULL,\n  `c` int DEFAULT NULL,\n  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
			want: "reordered",
		},
	}
	for _, tc := range cases {
		schema := filepath.Join(t.TempDir(), tc.name+".sql")
		if err := os.WriteFile(schema, []byte(tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
		stdout, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", schema, "--include-table", "shop.stale")
		assertFlashbackRefused(t, stdout, stderr, err, "shop.stale")
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: error %v, want %q\nstderr:\n%s", tc.name, err, tc.want, stderr)
		}
		for _, hint := range []string{"does not match", "--include-table", "incident time"} {
			if !strings.Contains(err.Error(), hint) || strings.Contains(err.Error(), "cannot verify") {
				t.Fatalf("%s: error %v, want a real mismatch hint", tc.name, err)
			}
		}
	}
}

// flashbackSchemaLayoutE2E restores shop.gen and shop.wide from a plain mysqldump
// and from two SHOW CREATE TABLE rows that have no semicolon between them.
func flashbackSchemaLayoutE2E(t *testing.T) {
	t.Helper()
	const sumSQL = "CHECKSUM TABLE shop.gen, shop.wide"
	const mutate = `
START TRANSACTION;
UPDATE shop.gen SET base = 11 WHERE id = 1;
DELETE FROM shop.gen WHERE id = 2;
INSERT INTO shop.gen (id, base) VALUES (4, 40);
UPDATE shop.wide SET note = 'schema-file' WHERE id = 1 AND bucket = 1;
COMMIT;`
	restore := func(schemaPath, label string) {
		t.Helper()
		before := e2eMySQL(t, sumSQL)
		e2eMySQL(t, "FLUSH LOGS")
		e2eMySQL(t, mutate)
		path := e2eIncidentBinlog(t)
		sql, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", schemaPath, "--include-table", "shop.gen,shop.wide")
		if err != nil {
			t.Fatalf("%s: %v\n%s", label, err, stderr)
		}
		if strings.Contains(sql, "`virt`") || strings.Contains(sql, "`stor`") || !strings.Contains(sql, "shop`.`wide") {
			t.Fatalf("%s sql:\n%s\nstderr:\n%s", label, sql, stderr)
		}
		e2eMySQL(t, sql)
		if got := e2eMySQL(t, sumSQL); got != before {
			t.Fatalf("%s checksum\nbefore:\n%s\nrestored:\n%s", label, before, got)
		}
	}

	dump := e2eTool(t, "mysqldump", []string{"--no-data", "--default-character-set=utf8mb4", "--set-gtid-purged=OFF", "shop"}, "")
	if !strings.Contains(dump, "Database: shop") || strings.Contains(dump, "\nUSE ") {
		t.Fatalf("mysqldump --no-data shop:\n%s", dump)
	}
	dumpPath := filepath.Join(t.TempDir(), "shop.sql")
	if err := os.WriteFile(dumpPath, []byte(dump), 0o644); err != nil {
		t.Fatal(err)
	}
	restore(dumpPath, "mysqldump")

	show := e2eMySQL(t, "SHOW CREATE TABLE shop.gen; SHOW CREATE TABLE shop.wide;")
	if strings.Count(show, "CREATE TABLE") < 2 || strings.Contains(show, "USE ") {
		t.Fatalf("show create:\n%s", show)
	}
	showPath := filepath.Join(t.TempDir(), "show.sql")
	if err := os.WriteFile(showPath, []byte(show), 0o644); err != nil {
		t.Fatal(err)
	}
	restore(showPath, "show create")
}

func flashbackEnumZeroE2E(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP TABLE IF EXISTS shop.ezero;
CREATE TABLE shop.ezero (
  id INT NOT NULL,
  e ENUM('red','blue') NOT NULL,
  PRIMARY KEY (id)
);`)
	e2eMySQL(t, "SET SESSION sql_mode=''; INSERT INTO shop.ezero VALUES (1, 0), (2, 1);")
	before := e2eMySQL(t, "CHECKSUM TABLE shop.ezero")
	indexes := e2eMySQL(t, "SELECT id, e+0 FROM shop.ezero ORDER BY id")
	if indexes != "1\t0\n2\t1\n" && indexes != "1\t0\n2\t1" {
		t.Fatalf("enum indexes:\n%q", indexes)
	}
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
START TRANSACTION;
DELETE FROM shop.ezero WHERE id = 2;
DELETE FROM shop.ezero WHERE id = 1;
COMMIT;`)
	path := e2eIncidentBinlog(t)
	sql, stderr, err := executeFlashbackLikeMain(t, path, "--include-table", "shop.ezero")
	if err != nil {
		t.Fatalf("enum 0: %v\n%s", err, stderr)
	}
	const save = "SET @binlogviz_sql_mode = @@SESSION.sql_mode;"
	const restoreMode = "SET SESSION sql_mode = @binlogviz_sql_mode;"
	if strings.Count(sql, save) != 1 || strings.Count(sql, restoreMode) != 1 {
		t.Fatalf("enum 0 sql:\n%s", sql)
	}
	i := strings.Index(sql, save)
	j := strings.Index(sql, restoreMode)
	mid := sql[i:j]
	if !strings.Contains(mid, "(1, 0)") || strings.Contains(mid, "(2, 1)") || !strings.Contains(sql, "(2, 1)") {
		t.Fatalf("enum 0 wrap:\n%s", sql)
	}
	for _, mode := range []struct {
		name    string
		prefix  string
		restore bool
	}{
		{name: "strict", restore: true},
		{name: "empty", prefix: "SET SESSION sql_mode='';\n", restore: true},
		{name: "no-backslash", prefix: "SET SESSION sql_mode='NO_BACKSLASH_ESCAPES';\n"},
		{name: "traditional", prefix: "SET SESSION sql_mode='TRADITIONAL';\n", restore: true},
	} {
		baseline := e2eSessionMode(t, mode.prefix+"DO 0;")
		e2eMySQL(t, "DELETE FROM shop.ezero")
		gotMode := e2eSessionMode(t, mode.prefix+sql)
		if got := e2eMySQL(t, "CHECKSUM TABLE shop.ezero"); got != before {
			t.Fatalf("%s enum 0 checksum\nbefore:\n%s\nrestored:\n%s", mode.name, before, got)
		}
		if got := e2eMySQL(t, "SELECT id, e+0 FROM shop.ezero ORDER BY id"); got != indexes {
			t.Fatalf("%s enum 0 indexes\nbefore:\n%q\nrestored:\n%q", mode.name, indexes, got)
		}
		if mode.restore {
			if gotMode != baseline {
				t.Fatalf("%s sql_mode\nbefore: %q\nafter: %q", mode.name, baseline, gotMode)
			}
			if mode.name == "traditional" && !strings.Contains(gotMode, "TRADITIONAL") {
				t.Fatalf("traditional mode was not restored: %q", gotMode)
			}
			continue
		}
		if gotMode != "" {
			t.Fatalf("%s sql_mode = %q, want empty after the header strips NO_BACKSLASH_ESCAPES", mode.name, gotMode)
		}
	}
}

func e2eSessionMode(t *testing.T, sql string) string {
	t.Helper()
	out := e2eMySQL(t, sql+"\nSELECT CONCAT('MODE=[', @@SESSION.sql_mode, ']');")
	const marker = "MODE=["
	i := strings.Index(out, marker)
	if i < 0 {
		t.Fatalf("mode probe:\n%s", out)
	}
	rest := out[i+len(marker):]
	j := strings.IndexByte(rest, ']')
	if j < 0 {
		t.Fatalf("mode probe:\n%s", out)
	}
	return rest[:j]
}

// flashbackGeneratedExprE2E restores tables whose generated columns use JSON,
// UPPER, CONCAT, integer division, and DIV from a mysqldump --no-data taken at incident time.
func flashbackGeneratedExprE2E(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP DATABASE IF EXISTS p160;
DROP DATABASE IF EXISTS p164;
CREATE DATABASE p160;
CREATE DATABASE p164;
CREATE TABLE p160.gen (
  id INT NOT NULL,
  j JSON,
  jv VARCHAR(20) AS (j->>'$.k') VIRTUAL,
  PRIMARY KEY (id)
);
CREATE TABLE p160.gen_nopk (
  a INT,
  v VARCHAR(10) AS (UPPER(a)) VIRTUAL
);
CREATE TABLE p160.names (
  id INT NOT NULL,
  first VARCHAR(40),
  last VARCHAR(40),
  full_name VARCHAR(80) AS (CONCAT(first, ' ', last)) STORED,
  PRIMARY KEY (id)
);
CREATE TABLE p164.ar (
  id INT NOT NULL,
  a INT,
  b INT,
  s1 INT AS ((a + b) * 3 - 1) STORED,
  s2 INT AS (a / 2) STORED,
  s3 BIGINT AS (-a * b) VIRTUAL,
  s4 INT AS (a DIV 2) VIRTUAL,
  PRIMARY KEY (id)
);
INSERT INTO p160.gen (id, j) VALUES (1, '{"k":"ab"}'), (2, '{"k":"cd"}');
INSERT INTO p160.gen_nopk (a) VALUES (1), (2);
INSERT INTO p160.names (id, first, last) VALUES (1, 'Ada', 'Lovelace'), (2, 'Grace', NULL);
INSERT INTO p164.ar (id, a, b) VALUES (1, 5, 7), (2, -5, NULL), (3, 4, 0);
`)
	stored := e2eMySQL(t, "SELECT id, a, b, s1, s2, s3, s4 FROM p164.ar ORDER BY id")
	const wantStored = "1\t5\t7\t35\t3\t-35\t2\n2\t-5\tNULL\tNULL\t-3\tNULL\t-2\n3\t4\t0\t11\t2\t0\t2"
	if strings.TrimSpace(stored) != wantStored {
		t.Fatalf("MySQL stored generated values:\n%q", stored)
	}
	const sumSQL = "CHECKSUM TABLE p160.gen, p160.gen_nopk, p160.names, p164.ar"
	before := e2eMySQL(t, sumSQL)
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
START TRANSACTION;
UPDATE p160.gen SET j = '{"k":"zz"}' WHERE id = 1;
DELETE FROM p160.gen WHERE id = 2;
INSERT INTO p160.gen (id, j) VALUES (3, '{"k":"new"}');
UPDATE p160.gen_nopk SET a = 9 WHERE a = 1;
DELETE FROM p160.gen_nopk WHERE a = 2;
INSERT INTO p160.gen_nopk (a) VALUES (8);
UPDATE p164.ar SET a = 9, b = 1 WHERE id = 1;
DELETE FROM p164.ar WHERE id = 2;
INSERT INTO p164.ar (id, a, b) VALUES (4, 8, 3);
UPDATE p160.names SET first = 'Augusta' WHERE id = 1;
DELETE FROM p160.names WHERE id = 2;
INSERT INTO p160.names (id, first, last) VALUES (3, 'Grace', 'Hopper');
COMMIT;`)
	if e2eMySQL(t, sumSQL) == before {
		t.Fatal("incident did not change generated-column checksums")
	}
	path := e2eIncidentBinlog(t)
	dump := e2eTool(t, "mysqldump", []string{
		"--no-data", "--default-character-set=utf8mb4", "--set-gtid-purged=OFF",
		"--databases", "p160", "p164",
	}, "")
	lowerDump := strings.ToLower(dump)
	if !strings.Contains(lowerDump, "json_extract") && !strings.Contains(dump, "->>") {
		t.Fatalf("dump missing json generated column:\n%s", dump)
	}
	if !strings.Contains(lowerDump, "upper") || !strings.Contains(lowerDump, "div") || !strings.Contains(lowerDump, "concat") {
		t.Fatalf("dump missing UPPER, DIV, or CONCAT:\n%s", dump)
	}
	dumpPath := filepath.Join(t.TempDir(), "generated.sql")
	if err := os.WriteFile(dumpPath, []byte(dump), 0o644); err != nil {
		t.Fatal(err)
	}
	sql, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", dumpPath, "--include-table", "p160.gen,p160.gen_nopk,p160.names,p164.ar")
	if err != nil {
		t.Fatalf("generated expr: %v\n%s\ndump:\n%s", err, stderr, dump)
	}
	if strings.Contains(stderr, "does not match") || strings.Contains(stderr, "cannot verify") || strings.Contains(stderr, "not verified") || strings.Contains(sql, "-- WARNING: generated column") {
		t.Fatalf("verifiable generated columns were not checked:\nstderr:\n%s\nsql:\n%s\ndump:\n%s", stderr, sql, dump)
	}
	for _, col := range []string{"`jv`", "`v`", "`s1`", "`s2`", "`s3`", "`s4`", "`full_name`"} {
		if sqlAssignsColumn(sql, col) {
			t.Fatalf("sql still assigns %s:\n%s", col, sql)
		}
	}
	if !strings.Contains(sql, "schema file does not match the target") || !strings.Contains(sql, "p160.gen.jv") || !guardBeforeFirstGTID(sql) {
		t.Fatalf("missing generated-column guard:\n%s", sql)
	}
	e2eMySQL(t, sql)
	if got := e2eMySQL(t, sumSQL); got != before {
		t.Fatalf("generated checksum\nbefore:\n%s\nrestored:\n%s\nsql:\n%s", before, got, sql)
	}
}

// flashbackGeneratedLossE2E refuses a schema file that marks a real column as generated
// when the logged values contradict the expression. One file is a wrong-environment
// dump. The other is a post-ALTER dump used on the pre-ALTER binlog. Both exit 1
// with no SQL.
func flashbackGeneratedLossE2E(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP DATABASE IF EXISTS p178;
DROP DATABASE IF EXISTS p178b;
CREATE DATABASE p178;
CREATE DATABASE p178b;
CREATE TABLE p178.t (
  id INT NOT NULL,
  x VARCHAR(20),
  c VARCHAR(20),
  n VARCHAR(20),
  PRIMARY KEY (id)
);
CREATE TABLE p178.cust (
  id INT NOT NULL,
  first VARCHAR(40),
  last VARCHAR(40),
  full_name VARCHAR(80),
  PRIMARY KEY (id)
);
INSERT INTO p178.t VALUES (1, 'abc', 'manual-1', 'n1'), (3, 'def', 'keep me', 'n3');
INSERT INTO p178.cust VALUES (1, 'Ada', 'Lovelace', 'Countess of Lovelace'), (3, 'Grace', 'Hopper', 'Rear Adm. Hopper');
CREATE TABLE p178b.t (
  id INT NOT NULL,
  x VARCHAR(20),
  c VARCHAR(20),
  PRIMARY KEY (id)
);
INSERT INTO p178b.t VALUES (1, 'abc', 'manual-1'), (2, 'def', 'keep');
`)
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
START TRANSACTION;
UPDATE p178.t SET c = 'oops' WHERE id = 1;
UPDATE p178.t SET c = NULL WHERE id = 3;
UPDATE p178.cust SET full_name = 'WRONG' WHERE id = 1;
UPDATE p178.cust SET full_name = NULL WHERE id = 3;
UPDATE p178b.t SET c = 'oops' WHERE id = 1;
DELETE FROM p178b.t WHERE id = 2;
COMMIT;`)
	path := e2eIncidentBinlog(t)
	wrong := "USE `p178`;\n" +
		"CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `c` varchar(20) GENERATED ALWAYS AS (upper(`x`)) STORED,\n" +
		"  `n` varchar(20) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n);\n" +
		"CREATE TABLE `cust` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `first` varchar(40) DEFAULT NULL,\n" +
		"  `last` varchar(40) DEFAULT NULL,\n" +
		"  `full_name` varchar(80) GENERATED ALWAYS AS (concat(`first`,_utf8mb4' ',`last`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	wrongPath := filepath.Join(t.TempDir(), "wrong-env.sql")
	if err := os.WriteFile(wrongPath, []byte(wrong), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, table := range []struct{ name, want string }{
		{name: "p178.t", want: "manual-1"},
		{name: "p178.cust", want: "Countess"},
	} {
		stdout, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", wrongPath, "--include-table", table.name)
		assertFlashbackRefused(t, stdout, stderr, err, table.want)
		if !strings.Contains(err.Error(), table.name) || strings.Contains(err.Error(), "cannot verify") {
			t.Fatalf("%s: %v", table.name, err)
		}
	}

	e2eMySQL(t, "ALTER TABLE p178b.t CHANGE c c VARCHAR(20) GENERATED ALWAYS AS (UPPER(x)) STORED")
	dump := e2eTool(t, "mysqldump", []string{
		"--no-data", "--default-character-set=utf8mb4", "--set-gtid-purged=OFF",
		"--databases", "p178b",
	}, "")
	if !strings.Contains(strings.ToLower(dump), "upper") {
		t.Fatalf("post-alter dump missing UPPER:\n%s", dump)
	}
	dumpPath := filepath.Join(t.TempDir(), "post-alter.sql")
	if err := os.WriteFile(dumpPath, []byte(dump), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", dumpPath, "--include-table", "p178b.t")
	assertFlashbackRefused(t, stdout, stderr, err, "p178b.t")
	if !strings.Contains(strings.ToLower(err.Error()), "upper") || !strings.Contains(err.Error(), "manual-1") {
		t.Fatalf("post-alter contradiction: %v", err)
	}
}

// flashbackTargetGuardE2E covers a wrong dump that the evaluator can contradict,
// a matching-values wrong dump that must die in the apply guard (including a
// no-primary-key duplicate), a real post-ALTER generated column, and JSON
// null / decimal generated columns.
func flashbackTargetGuardE2E(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP DATABASE IF EXISTS p178a;
DROP DATABASE IF EXISTS p183;
DROP DATABASE IF EXISTS p183ok;
DROP DATABASE IF EXISTS p182;
CREATE DATABASE p178a;
CREATE DATABASE p183;
CREATE DATABASE p183ok;
CREATE DATABASE p182;
CREATE DATABASE p186;
CREATE TABLE p178a.t (
  id INT NOT NULL,
  code VARCHAR(20) CHARACTER SET ascii,
  c VARCHAR(20) CHARACTER SET ascii,
  PRIMARY KEY (id)
);
INSERT INTO p178a.t VALUES (1, 'ab', 'note-1');
CREATE TABLE p183.t (
  id INT NOT NULL,
  x VARCHAR(20),
  c VARCHAR(20),
  PRIMARY KEY (id)
);
INSERT INTO p183.t VALUES (1, 'ab', 'AB');
CREATE TABLE p183.heap (
  x VARCHAR(20),
  note VARCHAR(40),
  n INT
);
INSERT INTO p183.heap VALUES ('dup', 'kept-note', 1);
CREATE TABLE p183ok.t (
  id INT NOT NULL,
  x VARCHAR(20),
  c VARCHAR(20),
  PRIMARY KEY (id)
);
ALTER TABLE p183ok.t CHANGE c c VARCHAR(20) GENERATED ALWAYS AS (UPPER(x)) STORED;
INSERT INTO p183ok.t (id, x) VALUES (1, 'ab');
CREATE TABLE p182.j (
  id INT NOT NULL,
  doc JSON,
  v VARCHAR(64) AS (doc->>'$.v'),
  PRIMARY KEY (id)
);
INSERT INTO p182.j (id, doc) VALUES
  (1, JSON_OBJECT('v', CAST('null' AS JSON))),
  (2, JSON_OBJECT('v', CAST(1.0 AS JSON))),
  (3, JSON_OBJECT('v', CAST(0.1 AS JSON))),
  (4, JSON_OBJECT('v', CAST(1e2 AS JSON)));
CREATE TABLE p186.guardwide (
  id INT NOT NULL,
  `+wideGuardCols()+`,
  PRIMARY KEY (id)
);
INSERT INTO p186.guardwide VALUES (1, `+wideGuardVals()+`);
`)
	stored := e2eMySQL(t, "SELECT id, v FROM p182.j ORDER BY id")
	t.Logf("JSON generated values:\n%s", stored)

	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
START TRANSACTION;
DELETE FROM p178a.t;
UPDATE p183.t SET x = 'cd', c = 'CD' WHERE id = 1;
INSERT INTO p183.heap VALUES ('dup', 'DUP', 1);
UPDATE p183ok.t SET x = 'zz' WHERE id = 1;
DELETE FROM p182.j;
DELETE FROM p186.guardwide;
COMMIT;`)
	asciiPath := e2eIncidentBinlog(t)
	const asciiSum = "CHECKSUM TABLE p178a.t"
	asciiAfter := e2eMySQL(t, asciiSum)
	asciiRows := e2eMySQL(t, "SELECT COUNT(*) FROM p178a.t")
	asciiSchema := "USE `p178a`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `code` varchar(20) CHARACTER SET ascii DEFAULT NULL,\n" +
		"  `c` varchar(20) CHARACTER SET ascii GENERATED ALWAYS AS (upper(`code`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	asciiFile := filepath.Join(t.TempDir(), "ascii.sql")
	if err := os.WriteFile(asciiFile, []byte(asciiSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := executeFlashbackLikeMain(t, asciiPath, "--schema-file", asciiFile, "--include-table", "p178a.t")
	assertFlashbackRefused(t, stdout, stderr, err, "note-1")
	if got := e2eMySQL(t, asciiSum); got != asciiAfter || e2eMySQL(t, "SELECT COUNT(*) FROM p178a.t") != asciiRows {
		t.Fatalf("ascii refusal changed rows\nbefore %s\nafter %s", asciiAfter, got)
	}

	const matchSum = "CHECKSUM TABLE p183.t, p183.heap"
	matchAfter := e2eMySQL(t, matchSum)
	matchSchema := "USE `p183`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `c` varchar(20) GENERATED ALWAYS AS (upper(`x`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n" +
		"CREATE TABLE `heap` (\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `note` varchar(40) GENERATED ALWAYS AS (upper(`x`)) STORED,\n" +
		"  `n` int DEFAULT NULL\n);\n"
	matchFile := filepath.Join(t.TempDir(), "match.sql")
	if err := os.WriteFile(matchFile, []byte(matchSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	sql, stderr, err := executeFlashbackLikeMain(t, asciiPath, "--schema-file", matchFile, "--include-table", "p183.t,p183.heap")
	if err != nil {
		t.Fatalf("matching dump: %v\n%s", err, stderr)
	}
	for _, name := range []string{"p183.t.c", "p183.heap.note"} {
		if !strings.Contains(sql, "'"+name+"'") {
			t.Fatalf("guard missing %s:\n%s", name, sql)
		}
	}
	if !strings.Contains(sql, "schema file does not match the target") || !guardBeforeFirstGTID(sql) {
		t.Fatalf("guard:\n%s", sql)
	}
	if !strings.Contains(sql, "`note` <=> 'DUP'") {
		t.Fatalf("no-pk WHERE dropped the generated column:\n%s", sql)
	}
	if sqlAssignsColumn(sql, "`c`") || sqlAssignsColumn(sql, "`note`") {
		t.Fatalf("matching dump assigned a generated column:\n%s", sql)
	}
	out, applyErr := e2eMySQLResult(t, sql)
	t.Logf("GUARD_APPLY_OUTPUT_BEGIN\n%s\nGUARD_APPLY_OUTPUT_END", out)
	if applyErr == nil || !strings.Contains(out, "schema file does not match the target") || !strings.Contains(out, "p183.t.c") || !strings.Contains(out, "p183.heap.note") {
		t.Fatalf("guard apply err=%v\n%s\nsql:\n%s", applyErr, out, sql)
	}
	if got := e2eMySQL(t, matchSum); got != matchAfter {
		t.Fatalf("guard apply changed rows\nbefore:\n%s\nafter:\n%s", matchAfter, got)
	}
	assertGuardForceKeepsRows(t, "FORCE", sql, matchSum, matchAfter, func(out string) {
		msg := "binlogviz: schema file does not match the target: p183.t.c, p183.heap.note"
		errLine := mysqlErrorLine(out, "1231")
		msgAt := strings.Index(out, msg)
		errAt := strings.Index(out, errLine)
		if msgAt < 0 || errLine == "" || errAt < 0 || msgAt > errAt {
			t.Fatalf("guard message was not printed before the error\n%s", out)
		}
		if strings.Contains(errLine, ",") || strings.Contains(errLine, "(2 columns)") || !strings.Contains(errLine, "p183.t.c") || !strings.Contains(errLine, "p183.heap.note") || !strings.Contains(errLine, " | ") {
			t.Fatalf("error line: %s", errLine)
		}
		if mysqlErrorLine(out, "1792") == "" {
			t.Fatalf("missing read-only refusal\n%s", out)
		}
	})
	assertGuardForceKeepsRows(t, "AUTOCOMMIT0", "SET SESSION autocommit=0;\n"+sql+"COMMIT;\n", matchSum, matchAfter, func(out string) {
		errLine := mysqlErrorLine(out, "1231")
		if errLine == "" || !strings.Contains(errLine, "p183.t.c") || !strings.Contains(errLine, "p183.heap.note") || mysqlErrorLine(out, "1792") == "" {
			t.Fatalf("autocommit=0 guard:\n%s", out)
		}
	})
	// #194/#195: this server evaluates the rendered version check for other servers' version strings.
	assertGuardVersionChoice(t, sql)
	// #196: a block cut off from the header stays read-only.
	if cut := strings.Index(sql, "-- gtid:"); cut > 0 {
		assertGuardForceKeepsRows(t, "HEADERLESS", sql[cut:], matchSum, matchAfter, func(out string) {
			if mysqlErrorLine(out, "1792") == "" {
				t.Fatalf("header-less block was not read-only\n%s", out)
			}
		})
	}
	// #196: a server that refuses new prepared statements still cannot write.
	prevPrep := strings.TrimSpace(e2eMySQL(t, "SELECT @@GLOBAL.max_prepared_stmt_count"))
	e2eMySQL(t, "SET GLOBAL max_prepared_stmt_count = 0")
	restorePrep := func() { e2eMySQL(t, "SET GLOBAL max_prepared_stmt_count = "+prevPrep) }
	t.Cleanup(func() { _, _ = e2eMySQLResult(t, "SET GLOBAL max_prepared_stmt_count = "+prevPrep) })
	assertGuardForceKeepsRows(t, "NOPREPARE", sql, matchSum, matchAfter, func(out string) {
		if mysqlErrorLine(out, "1461") == "" || mysqlErrorLine(out, "1792") == "" {
			t.Fatalf("max_prepared_stmt_count=0 guard:\n%s", out)
		}
	})
	restorePrep()
	heap := e2eMySQL(t, "SELECT x, note, n FROM p183.heap ORDER BY note")
	if !strings.Contains(heap, "kept-note") || !strings.Contains(heap, "DUP") {
		t.Fatalf("duplicate rows:\n%s", heap)
	}

	okBefore := e2eMySQL(t, "SELECT id, x, c FROM p183ok.t")
	okWant := "1\tab\tAB"
	dump := e2eTool(t, "mysqldump", []string{
		"--no-data", "--default-character-set=utf8mb4", "--set-gtid-purged=OFF",
		"--databases", "p183ok",
	}, "")
	dumpPath := filepath.Join(t.TempDir(), "p183ok.sql")
	if err := os.WriteFile(dumpPath, []byte(dump), 0o644); err != nil {
		t.Fatal(err)
	}
	sql, stderr, err = executeFlashbackLikeMain(t, asciiPath, "--schema-file", dumpPath, "--include-table", "p183ok.t")
	if err != nil {
		t.Fatalf("post-alter restore: %v\n%s\ndump:\n%s", err, stderr, dump)
	}
	if !strings.Contains(sql, "schema file does not match the target") || !strings.Contains(sql, "'p183ok.t.c'") {
		t.Fatalf("post-alter guard:\n%s", sql)
	}
	probe := e2eMySQL(t, sql+"SELECT CONCAT('RO=', @@SESSION.transaction_read_only, ' AC=', @@SESSION.autocommit);\n")
	if !strings.Contains(probe, "RO=0") || !strings.Contains(probe, "AC=1") {
		t.Fatalf("matching guard changed the session: %q", probe)
	}
	if got := e2eMySQL(t, "SELECT id, x, c FROM p183ok.t"); strings.TrimSpace(got) != okWant {
		t.Fatalf("post-alter row %q want %q\nwas %q\nsql:\n%s", strings.TrimSpace(got), okWant, strings.TrimSpace(okBefore), sql)
	}
	jsonDump := e2eTool(t, "mysqldump", []string{
		"--no-data", "--default-character-set=utf8mb4", "--set-gtid-purged=OFF",
		"--databases", "p182",
	}, "")
	jsonPath := filepath.Join(t.TempDir(), "p182.sql")
	if err := os.WriteFile(jsonPath, []byte(jsonDump), 0o644); err != nil {
		t.Fatal(err)
	}
	sql, stderr, err = executeFlashbackLikeMain(t, asciiPath, "--schema-file", jsonPath, "--include-table", "p182.j")
	if err != nil {
		t.Fatalf("json flashback: %v\n%s\nstored before delete:\n%s\ndump:\n%s", err, stderr, stored, jsonDump)
	}
	if strings.Contains(stderr, "does not match") {
		t.Fatalf("json contradiction:\n%s\nstored:\n%s\nsql:\n%s", stderr, stored, sql)
	}
	e2eMySQL(t, sql)
	if got := e2eMySQL(t, "SELECT id, v FROM p182.j ORDER BY id"); got != stored {
		t.Fatalf("json restored\nwant:\n%s\ngot:\n%s\nsql:\n%s", stored, got, sql)
	}

	const wideSum = "CHECKSUM TABLE p186.guardwide"
	wideAfter := e2eMySQL(t, wideSum)
	var wideSchema strings.Builder
	wideSchema.WriteString("USE `p186`;\nCREATE TABLE `guardwide` (\n  `id` int NOT NULL,\n")
	for i := 1; i <= 12; i++ {
		fmt.Fprintf(&wideSchema, "  `col_%02d` varchar(40) GENERATED ALWAYS AS (md5(`id`)) STORED,\n", i)
	}
	wideSchema.WriteString("  PRIMARY KEY (`id`)\n);\n")
	wideFile := filepath.Join(t.TempDir(), "p186.sql")
	if err := os.WriteFile(wideFile, []byte(wideSchema.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	wideSQL, stderr, err := executeFlashbackLikeMain(t, asciiPath, "--schema-file", wideFile, "--include-table", "p186.guardwide")
	if err != nil {
		t.Fatalf("wide guard: %v\n%s", err, stderr)
	}
	assertGuardForceKeepsRows(t, "WIDE_FORCE", wideSQL, wideSum, wideAfter, func(out string) {
		errLine := mysqlErrorLine(out, "1231")
		if errLine == "" || !strings.Contains(errLine, "(12 columns)") || !strings.Contains(errLine, "p186.guardwide.col_01") || !strings.Contains(errLine, "p186.guardwide.col_05") || strings.Contains(errLine, "p186.guardwide.col_06") || strings.Contains(errLine, ",") {
			t.Fatalf("wide error line: %s\n%s", errLine, out)
		}
		if !strings.Contains(out, "p186.guardwide.col_12") || mysqlErrorLine(out, "1792") == "" {
			t.Fatalf("wide guard output:\n%s", out)
		}
	})
	assertGuardForceKeepsRows(t, "WIDE_AUTOCOMMIT0", "SET SESSION autocommit=0;\n"+wideSQL+"COMMIT;\n", wideSum, wideAfter, nil)
}

func wideGuardCols() string {
	parts := make([]string, 12)
	for i := range parts {
		parts[i] = fmt.Sprintf("`col_%02d` VARCHAR(40)", i+1)
	}
	return strings.Join(parts, ",\n  ")
}

func wideGuardVals() string {
	parts := make([]string, 12)
	for i := range parts {
		parts[i] = "'x'"
	}
	return strings.Join(parts, ", ")
}

// assertGuardVersionChoice runs the guard's own version check from script on
// this server with @@version replaced by each string, and checks which servers
// are refused and which read-only variable is read.
func assertGuardVersionChoice(t *testing.T, script string) {
	t.Helper()
	start := strings.Index(script, "SET @binlogviz_maria = ")
	roAt := strings.Index(script, "SET @binlogviz_ro = ")
	if start < 0 || roAt < start {
		t.Fatalf("version check not found:\n%s", script)
	}
	end := roAt + strings.Index(script[roAt:], "\n") + 1
	block := strings.ReplaceAll(script[start:end], "@@version", "@binlogviz_v")
	cases := []struct {
		version, old, ro string
	}{
		{"5.5.62", "1", "tx_read_only"},
		{"5.6.51-log", "1", "tx_read_only"},
		{"5.7.0", "0", "tx_read_only"},
		{"5.7.9", "0", "tx_read_only"},
		{"5.7.19-log", "0", "tx_read_only"},
		{"5.7.20", "0", "transaction_read_only"},
		{"5.7.44-48-log", "0", "transaction_read_only"},
		{"8.0.46-0ubuntu0.24.04.4", "0", "transaction_read_only"},
		{"8.0.36-28", "0", "transaction_read_only"},
		{"8.4.3", "0", "transaction_read_only"},
		{"9.1.0", "0", "transaction_read_only"},
		{"10.1.48-MariaDB", "1", "tx_read_only"},
		{"10.6.28-MariaDB-log", "0", "tx_read_only"},
		{"10.11.19-MariaDB-ubu2204-log", "0", "tx_read_only"},
		{"5.5.5-10.11.6-MariaDB", "0", "tx_read_only"},
		{"11.0.6-MariaDB", "0", "tx_read_only"},
		{"11.1.6-MariaDB", "0", "transaction_read_only"},
		{"11.4.13-MariaDB", "0", "transaction_read_only"},
		{"12.0.2-MariaDB", "0", "transaction_read_only"},
	}
	var b strings.Builder
	for _, tc := range cases {
		fmt.Fprintf(&b, "SET @binlogviz_v = '%s';\n%sSELECT CONCAT(@binlogviz_v, ' ', @binlogviz_old, ' ', @binlogviz_ro);\n", tc.version, block)
	}
	got := strings.Split(strings.TrimSpace(e2eMySQL(t, b.String())), "\n")
	if len(got) != len(cases) {
		t.Fatalf("version check output:\n%s", strings.Join(got, "\n"))
	}
	for i, tc := range cases {
		if want := tc.version + " " + tc.old + " " + tc.ro; strings.TrimSpace(got[i]) != want {
			t.Errorf("version check %q want %q", got[i], want)
		}
	}
}

func assertGuardForceKeepsRows(t *testing.T, label, script, checksumSQL, before string, check func(string)) {
	t.Helper()
	out, err := e2eMySQLForce(t, script)
	t.Logf("GUARD_%s_OUTPUT_BEGIN\n%s\nGUARD_%s_OUTPUT_END", label, out, label)
	if err != nil {
		t.Fatalf("%s force exit: %v\n%s", label, err, out)
	}
	if got := e2eMySQL(t, checksumSQL); got != before {
		t.Fatalf("%s changed rows\nbefore:\n%s\nafter:\n%s", label, before, got)
	}
	if check != nil {
		check(out)
	}
}

// flashbackDecimalGeneratedE2E restores DECIMAL generated columns that divide,
// including exact quotients, repeating fractions, negatives, and BIGINT edges.
// A wrong expression is refused. A non-strict clip only warns. A quotient
// written with div_precision_increment=0 is refused.
func flashbackDecimalGeneratedE2E(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP DATABASE IF EXISTS p179;
CREATE DATABASE p179;
CREATE TABLE p179.qd (
  id INT NOT NULL,
  a BIGINT,
  b BIGINT,
  g DECIMAL(40,4) AS (a / b) STORED,
  g9 DECIMAL(40,9) AS (a / b) STORED,
  half DECIMAL(20,1) AS (a / b) STORED,
  mul DECIMAL(40,4) AS ((a / b) * b) STORED,
  gi BIGINT AS (a / b) STORED,
  gimul BIGINT AS ((a / b) * b) STORED,
  note VARCHAR(10),
  PRIMARY KEY (id)
);
CREATE TABLE p179.price (
  id INT NOT NULL,
  total DECIMAL(14,2),
  qty DECIMAL(10,2),
  unit_price DECIMAL(12,4) AS (total / qty) STORED,
  PRIMARY KEY (id)
);
CREATE TABLE p179.wide (
  id INT NOT NULL,
  au BIGINT UNSIGNED,
  bu BIGINT UNSIGNED,
  g DECIMAL(40,4) AS (au / bu) STORED,
  g9 DECIMAL(40,9) AS (au / bu) STORED,
  mul DECIMAL(40,4) AS ((au / bu) * bu) STORED,
  PRIMARY KEY (id)
);
CREATE TABLE p179.ops (
  id INT NOT NULL,
  a INT,
  b INT,
  s1 DECIMAL(40,4) AS ((a + b) * 3 - 1) STORED,
  neg DECIMAL(40,4) AS (-(a * b)) STORED,
  dv DECIMAL(20,4) AS (a DIV b) STORED,
  md DECIMAL(20,4) AS (MOD(a, b)) STORED,
  half DECIMAL(20,4) AS (a / 2) STORED,
  PRIMARY KEY (id)
);
CREATE TABLE p179.small (
  id INT NOT NULL,
  a INT,
  t TINYINT AS (a * 100) STORED,
  PRIMARY KEY (id)
);
INSERT INTO p179.qd (id, a, b, note) VALUES
  (1, 5, 2, 'half'),
  (2, 15, 4, 'exact'),
  (3, 1, 3, 'rep'),
  (4, 2, 3, 'rep'),
  (5, 1, 7, 'rep'),
  (6, -5, 2, 'neg'),
  (7, 5, -2, 'neg'),
  (8, -5, -2, 'neg'),
  (9, 9, 2, 'up'),
  (10, 1, 2, 'half'),
  (11, -1, 2, 'half'),
  (12, 1, 8, 'eighth'),
  (13, -3, 8, 'eighth'),
  (14, 49999, 20000, 'edge'),
  (15, 1, 30000, 'tiny'),
  (16, 1, 10000000000, 'loss'),
  (17, 1, 1000000000, 'keep'),
  (18, 9223372036854775807, 2, 'max'),
  (19, -9223372036854775808, 2, 'min'),
  (20, NULL, 2, 'null'),
  (21, 7, 4, 'threeq'),
  (22, 22, 7, 'pi'),
  (23, 9999999995, 10000000000, 'near1');
INSERT INTO p179.price (id, total, qty) VALUES
  (1, 15.00, 4.00),
  (2, 10.00, 3.00),
  (3, 1.00, 7.00),
  (4, -15.50, 2.00),
  (5, 5.00, 2.50),
  (6, 0.00, 1.00);
INSERT INTO p179.wide (id, au, bu) VALUES
  (1, 18446744073709551615, 2),
  (2, 18446744073709551615, 3),
  (3, 18446744073709551615, 7),
  (4, 5, 2);
INSERT INTO p179.ops (id, a, b) VALUES
  (1, 5, 7),
  (2, -5, 2),
  (3, 9, 2),
  (4, 5, NULL);
INSERT INTO p179.small (id, a) VALUES (1, 1), (2, 0);
`)
	qd := e2eMySQL(t, `SELECT CONCAT_WS('|', id, IFNULL(g,'NULL'), IFNULL(g9,'NULL'), IFNULL(half,'NULL'), IFNULL(mul,'NULL'), IFNULL(gi,'NULL'), IFNULL(gimul,'NULL')) FROM p179.qd ORDER BY id`)
	const wantQD = "" +
		"1|2.5000|2.500000000|2.5|5.0000|3|5\n" +
		"2|3.7500|3.750000000|3.8|15.0000|4|15\n" +
		"3|0.3333|0.333333333|0.3|1.0000|0|1\n" +
		"4|0.6667|0.666666666|0.7|2.0000|1|2\n" +
		"5|0.1429|0.142857142|0.1|1.0000|0|1\n" +
		"6|-2.5000|-2.500000000|-2.5|-5.0000|-3|-5\n" +
		"7|-2.5000|-2.500000000|-2.5|5.0000|-3|5\n" +
		"8|2.5000|2.500000000|2.5|-5.0000|3|-5\n" +
		"9|4.5000|4.500000000|4.5|9.0000|5|9\n" +
		"10|0.5000|0.500000000|0.5|1.0000|1|1\n" +
		"11|-0.5000|-0.500000000|-0.5|-1.0000|-1|-1\n" +
		"12|0.1250|0.125000000|0.1|1.0000|0|1\n" +
		"13|-0.3750|-0.375000000|-0.4|-3.0000|0|-3\n" +
		"14|2.5000|2.499950000|2.5|49999.0000|2|49999\n" +
		"15|0.0000|0.000033333|0.0|1.0000|0|1\n" +
		"16|0.0000|0.000000000|0.0|0.0000|0|0\n" +
		"17|0.0000|0.000000001|0.0|1.0000|0|1\n" +
		"18|4611686018427387903.5000|4611686018427387903.500000000|4611686018427387903.5|9223372036854775807.0000|4611686018427387904|9223372036854775807\n" +
		"19|-4611686018427387904.0000|-4611686018427387904.000000000|-4611686018427387904.0|-9223372036854775808.0000|-4611686018427387904|-9223372036854775808\n" +
		"20|NULL|NULL|NULL|NULL|NULL|NULL\n" +
		"21|1.7500|1.750000000|1.8|7.0000|2|7\n" +
		"22|3.1429|3.142857142|3.1|22.0000|3|22\n" +
		"23|1.0000|0.999999999|1.0|9999999990.0000|1|9999999990\n"
	if qd != wantQD {
		t.Fatalf("MySQL DECIMAL generated values:\n%s", qd)
	}
	price := e2eMySQL(t, `SELECT CONCAT_WS('|', id, unit_price) FROM p179.price ORDER BY id`)
	const wantPrice = "1|3.7500\n2|3.3333\n3|0.1429\n4|-7.7500\n5|2.0000\n6|0.0000\n"
	if price != wantPrice {
		t.Fatalf("unit_price:\n%s", price)
	}
	wide := e2eMySQL(t, `SELECT CONCAT_WS('|', id, g, g9, mul) FROM p179.wide ORDER BY id`)
	const wantWide = "" +
		"1|9223372036854775807.5000|9223372036854775807.500000000|18446744073709551615.0000\n" +
		"2|6148914691236517205.0000|6148914691236517205.000000000|18446744073709551615.0000\n" +
		"3|2635249153387078802.1429|2635249153387078802.142857142|18446744073709551615.0000\n" +
		"4|2.5000|2.500000000|5.0000\n"
	if wide != wantWide {
		t.Fatalf("unsigned:\n%s", wide)
	}

	const sumSQL = "CHECKSUM TABLE p179.qd, p179.price, p179.wide, p179.ops, p179.small"
	before := e2eMySQL(t, sumSQL)
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
START TRANSACTION;
DELETE FROM p179.qd;
DELETE FROM p179.price;
DELETE FROM p179.wide;
DELETE FROM p179.ops;
DELETE FROM p179.small;
COMMIT;`)
	if e2eMySQL(t, sumSQL) == before {
		t.Fatal("incident did not change DECIMAL generated checksums")
	}
	path := e2eIncidentBinlog(t)
	dump := e2eTool(t, "mysqldump", []string{
		"--no-data", "--default-character-set=utf8mb4", "--set-gtid-purged=OFF",
		"--databases", "p179",
	}, "")
	if !strings.Contains(dump, "`g` decimal(40,4) GENERATED ALWAYS AS ((`a` / `b`))") || !strings.Contains(dump, "unit_price") {
		t.Fatalf("dump missing DECIMAL generated columns:\n%s", dump)
	}
	dumpPath := filepath.Join(t.TempDir(), "p179.sql")
	if err := os.WriteFile(dumpPath, []byte(dump), 0o644); err != nil {
		t.Fatal(err)
	}
	sql, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", dumpPath, "--include-table", "p179.qd,p179.price,p179.wide,p179.ops,p179.small")
	if err != nil {
		t.Fatalf("decimal generated: %v\n%s\ndump:\n%s", err, stderr, dump)
	}
	if strings.Contains(stderr, "does not match") || strings.Contains(stderr, "cannot verify") || strings.Contains(stderr, "not verified") || strings.Contains(sql, "-- WARNING: generated column") {
		t.Fatalf("correct DECIMAL dump was not accepted:\nstderr:\n%s\nsql:\n%s", stderr, sql)
	}
	for _, col := range []string{"`g`", "`g9`", "`half`", "`mul`", "`gi`", "`gimul`", "`unit_price`", "`s1`", "`neg`", "`dv`", "`md`", "`t`"} {
		if sqlAssignsColumn(sql, col) {
			t.Fatalf("sql still assigns %s:\n%s", col, sql)
		}
	}
	if !strings.Contains(sql, "p179.qd.g") || !guardBeforeFirstGTID(sql) {
		t.Fatalf("missing generated-column guard:\n%s", sql)
	}
	e2eMySQL(t, sql)
	if got := e2eMySQL(t, sumSQL); got != before {
		t.Fatalf("decimal checksum\nbefore:\n%s\nrestored:\n%s\nsql:\n%s", before, got, sql)
	}

	needle := "`g` decimal(40,4) GENERATED ALWAYS AS ((`a` / `b`))"
	if strings.Count(dump, needle) != 1 {
		t.Fatalf("dump needle count %d:\n%s", strings.Count(dump, needle), dump)
	}
	wrong := strings.Replace(dump, needle, "`g` decimal(40,4) GENERATED ALWAYS AS ((`a` + `b`))", 1)
	wrongPath := filepath.Join(t.TempDir(), "p179-wrong.sql")
	if err := os.WriteFile(wrongPath, []byte(wrong), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", wrongPath, "--include-table", "p179.qd")
	assertFlashbackRefused(t, stdout, stderr, err, "does not match")
	if strings.Contains(err.Error(), "cannot verify") || !strings.Contains(err.Error(), "p179.qd") {
		t.Fatalf("wrong decimal dump: %v", err)
	}

	e2eMySQL(t, `
DROP DATABASE IF EXISTS p179clip;
CREATE DATABASE p179clip;
SET SESSION sql_mode='';
CREATE TABLE p179clip.clip (
  id INT NOT NULL,
  a INT,
  t TINYINT AS (a * 100) STORED,
  tu TINYINT UNSIGNED AS (a - 10) STORED,
  PRIMARY KEY (id)
);
INSERT INTO p179clip.clip (id, a) VALUES (1, 1), (2, 5), (3, -5), (4, 20);
`)
	clipped := e2eMySQL(t, "SELECT CONCAT_WS('|', id, t, tu) FROM p179clip.clip ORDER BY id")
	const wantClip = "1|100|0\n2|127|0\n3|-128|0\n4|127|10\n"
	if clipped != wantClip {
		t.Fatalf("clipped values:\n%s", clipped)
	}
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, "DELETE FROM p179clip.clip")
	clipPath := e2eIncidentBinlog(t)
	clipDump := e2eTool(t, "mysqldump", []string{
		"--no-data", "--default-character-set=utf8mb4", "--set-gtid-purged=OFF",
		"--databases", "p179clip",
	}, "")
	clipFile := filepath.Join(t.TempDir(), "p179clip.sql")
	if err := os.WriteFile(clipFile, []byte(clipDump), 0o644); err != nil {
		t.Fatal(err)
	}
	sql, stderr, err = executeFlashbackLikeMain(t, clipPath, "--schema-file", clipFile, "--include-table", "p179clip.clip")
	if err != nil {
		t.Fatalf("clip dump refused: %v\n%s", err, stderr)
	}
	if !strings.Contains(stderr, "not verified") || strings.Contains(stderr, "does not match") || sql == "" || sqlAssignsColumn(sql, "`t`") || sqlAssignsColumn(sql, "`tu`") {
		t.Fatalf("clip should warn and omit:\nstderr:\n%s\nsql:\n%s", stderr, sql)
	}
	badClip := strings.Replace(clipDump, "((`a` * 100))", "((`a` + 100))", 1)
	if badClip == clipDump {
		t.Fatalf("clip dump had no a*100:\n%s", clipDump)
	}
	badClipFile := filepath.Join(t.TempDir(), "p179clip-wrong.sql")
	if err := os.WriteFile(badClipFile, []byte(badClip), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = executeFlashbackLikeMain(t, clipPath, "--schema-file", badClipFile, "--include-table", "p179clip.clip")
	assertFlashbackRefused(t, stdout, stderr, err, "does not match")

	e2eMySQL(t, `
DROP DATABASE IF EXISTS p179inc;
CREATE DATABASE p179inc;
SET SESSION div_precision_increment = 0;
CREATE TABLE p179inc.t (
  id INT NOT NULL,
  a INT,
  b INT,
  g DECIMAL(40,4) AS (a / b) STORED,
  gi INT AS (a / b) STORED,
  PRIMARY KEY (id)
);
INSERT INTO p179inc.t (id, a, b) VALUES (1, 5, 2), (2, 7, 4), (3, 15, 4), (4, 10, 5);
`)
	inc := e2eMySQL(t, "SELECT CONCAT_WS('|', id, g, gi) FROM p179inc.t ORDER BY id")
	const wantInc = "1|2.0000|2\n2|1.0000|1\n3|3.0000|3\n4|2.0000|2\n"
	if inc != wantInc {
		t.Fatalf("div_precision_increment=0 values:\n%s", inc)
	}
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, "DELETE FROM p179inc.t")
	incPath := e2eIncidentBinlog(t)
	incDump := e2eTool(t, "mysqldump", []string{
		"--no-data", "--default-character-set=utf8mb4", "--set-gtid-purged=OFF",
		"--databases", "p179inc",
	}, "")
	incFile := filepath.Join(t.TempDir(), "p179inc.sql")
	if err := os.WriteFile(incFile, []byte(incDump), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = executeFlashbackLikeMain(t, incPath, "--schema-file", incFile, "--include-table", "p179inc.t")
	assertFlashbackRefused(t, stdout, stderr, err, "does not match")
	if strings.Contains(err.Error(), "cannot verify") {
		t.Fatalf("increment 0 was treated as unverifiable: %v", err)
	}
}

func sqlAssignsColumn(sql, col string) bool {
	for _, stmt := range strings.Split(sql, ";") {
		body := stmt
		if i := strings.Index(strings.ToUpper(stmt), " WHERE "); i >= 0 {
			body = stmt[:i]
		}
		if strings.Contains(body, col) {
			return true
		}
	}
	return false
}

func guardBeforeFirstGTID(sql string) bool {
	guard := strings.Index(sql, "@binlogviz_mismatch")
	gtid := strings.Index(sql, "-- gtid:")
	return guard > 0 && gtid > guard
}

func e2eIncidentBinlog(t *testing.T) string {
	t.Helper()
	e2eMySQL(t, "FLUSH LOGS")
	datadir := strings.TrimSpace(e2eMySQL(t, "SELECT @@datadir"))
	names := binlogNames(t, e2eMySQL(t, "SHOW BINARY LOGS"))
	name := names[len(names)-2]
	dst := filepath.Join(t.TempDir(), name)
	e2eCopy(t, datadir+"/"+name, dst)
	return dst
}

func e2eTool(t *testing.T, bin string, args []string, stdin string) string {
	t.Helper()
	base := strings.Fields(os.Getenv("BINLOGVIZ_MYSQL"))
	if len(base) == 0 {
		base = []string{"sudo", "mysql"}
	}
	for i, arg := range base {
		if arg == "mysql" || strings.HasSuffix(arg, "/mysql") {
			base[i] = bin
			break
		}
	}
	cmd := exec.Command(base[0], append(base[1:], args...)...)
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s: %v\n%s", bin, err, stderr.String())
	}
	return stdout.String()
}

// transactionPayloadEventType is MySQL TRANSACTION_PAYLOAD_EVENT.
const transactionPayloadEventType = 40

func binlogHasEventType(t *testing.T, path string, typ byte) bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	off := 4
	for off+19 <= len(data) {
		size := int(binary.LittleEndian.Uint32(data[off+9 : off+13]))
		if size < 19 || off+size > len(data) {
			t.Fatalf("%s: bad event at %d size %d", path, off, size)
		}
		if data[off+4] == typ {
			return true
		}
		off += size
	}
	return false
}

const flashbackOrderSQL = `
SELECT id, bucket, i_tiny, i_tiny_u, i_small, i_int, i_int_u, i_big, i_big_u, qty, price,
       DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'),
       DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f'),
       note, HEX(raw), HEX(bits), payload, color, flags
FROM shop.wide ORDER BY id, bucket;
SELECT id, note FROM shop.heap ORDER BY id, note;
SELECT id, HEX(doc) FROM shop.jdoc ORDER BY id;
SELECT HEX(doc) FROM shop.jheap ORDER BY HEX(doc);
SELECT id, HEX(l1), HEX(u16) FROM shop.chars ORDER BY id;
SELECT id, base, virt, stor FROM shop.gen ORDER BY id;
SELECT id, HEX(e), HEX(s), HEX(eu) FROM shop.es ORDER BY id;
SELECT id, HEX(j) FROM shop.cj ORDER BY id;
SELECT yr, yr2, region, id, note, n, si, ui, sb, tu, mi, ss FROM shop.yearnum ORDER BY yr, yr2, region, id;
`

// yearIncidentReplay is the shop.yearnum DML inside flashback_incident.sql.
// The binlog copy of that DML is wrapped in binlog_transaction_compression.
const yearIncidentReplay = `
START TRANSACTION;
INSERT INTO shop.yearnum VALUES (2026, 1999, 1, 3000000000, 'junk', 1.0, 1, 1, 1, 1, 1, 1);
UPDATE shop.yearnum SET note = 'oops', n = 9.5, si = -9, ui = 1, sb = 1, tu = 1, mi = 1, ss = 1
  WHERE yr = 2026 AND yr2 = 1999 AND region = 1 AND id = 4000000000;
DELETE FROM shop.yearnum WHERE yr = 2026 AND yr2 = 1999 AND region = 1 AND id = 7;
COMMIT;
`

func readTestdata(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(mustFixturePath(t, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(body)
}

func binlogNames(t *testing.T, logs string) []string {
	t.Helper()
	var names []string
	for _, line := range strings.Split(logs, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && strings.Contains(fields[0], ".") {
			names = append(names, fields[0])
		}
	}
	if len(names) < 2 {
		t.Fatalf("binary logs:\n%s", logs)
	}
	return names
}

func e2eGTIDSet(raw string) string {
	var parts []string
	for _, part := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == ' ' || r == '\t'
	}) {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, ",")
}

func e2eMySQL(t *testing.T, stdin string) string {
	t.Helper()
	out, err := e2eMySQLResult(t, stdin)
	if err != nil {
		t.Fatalf("mysql: %v\n%s\nSQL:\n%s", err, out, stdin)
	}
	return out
}

func e2eMySQLResult(t *testing.T, stdin string) (string, error) {
	t.Helper()
	return e2eMySQLRun(t, stdin, false)
}

// e2eMySQLForce applies stdin with the client flag that continues after an error.
// stdbuf keeps the guard's SELECT ahead of the later error lines in the captured log.
func e2eMySQLForce(t *testing.T, stdin string) (string, error) {
	t.Helper()
	return e2eMySQLRun(t, stdin, true)
}

func e2eMySQLRun(t *testing.T, stdin string, force bool) (string, error) {
	t.Helper()
	base := strings.Fields(os.Getenv("BINLOGVIZ_MYSQL"))
	if len(base) == 0 {
		base = []string{"sudo", "mysql"}
	}
	args := make([]string, 0, len(base)+8)
	if force {
		inserted := false
		for _, arg := range base {
			if !inserted && (arg == "mysql" || strings.HasSuffix(arg, "/mysql")) {
				args = append(args, "stdbuf", "-o0", "-e0", arg)
				inserted = true
				continue
			}
			args = append(args, arg)
		}
		args = append(args, "--force", "--quick")
	} else {
		args = append(args, base...)
	}
	args = append(args, "--default-character-set=utf8mb4", "-N", "--batch")
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mysqlErrorLine(out, code string) string {
	needle := "ERROR " + code
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}

func e2eCopy(t *testing.T, src, dst string) {
	t.Helper()
	if out, err := exec.Command("sudo", "cp", src, dst).CombinedOutput(); err != nil {
		t.Fatalf("cp %s: %v\n%s", src, err, out)
	}
	if out, err := exec.Command("sudo", "chmod", "a+r", dst).CombinedOutput(); err != nil {
		t.Fatalf("chmod %s: %v\n%s", dst, err, out)
	}
}
