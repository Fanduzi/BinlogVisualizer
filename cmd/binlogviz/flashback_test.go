package binlogviz

import (
	"encoding/binary"
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
	if strings.Contains(out, "Only count these ROW kinds") || strings.Contains(out, "Only analyze") || !strings.Contains(out, "Only undo these ROW kinds") || !strings.Contains(out, "Only undo these schemas") || !strings.Contains(out, "Only undo these tables") || !strings.Contains(out, "SQL file of table definitions") || !strings.Contains(out, "Print SQL that reverses selected ROW changes") {
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
	for _, banned := range []string{"Only analyze", "Start position", "Flags:", "Global Flags:", "Usage:"} {
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

	const checksumSQL = "CHECKSUM TABLE shop.wide, shop.heap, shop.jdoc, shop.jheap, shop.chars, shop.gen, shop.es, shop.cj"
	e2eMySQL(t, "RESET MASTER")
	e2eMySQL(t, readTestdata(t, "flashback_setup.sql"))
	beforeSum := e2eMySQL(t, checksumSQL)
	beforeRows := e2eMySQL(t, flashbackOrderSQL)
	genBefore := e2eMySQL(t, "CHECKSUM TABLE shop.gen")
	cjBefore := e2eMySQL(t, "CHECKSUM TABLE shop.cj")
	esBefore := e2eMySQL(t, "CHECKSUM TABLE shop.es")
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
	if !strings.Contains(sql, "18446744073709551615") || !strings.Contains(sql, "JSON_OBJECT(") || !strings.Contains(sql, "_latin1 ") || !strings.Contains(sql, "_utf16 ") {
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

	flashbackSchemaMatchE2E(t)
	flashbackSchemaLayoutE2E(t)
	flashbackEnumZeroE2E(t)
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
	e2eMySQL(t, sql)
	if got := e2eMySQL(t, "CHECKSUM TABLE shop.ezero"); got != before {
		t.Fatalf("enum 0 checksum\nbefore:\n%s\nrestored:\n%s", before, got)
	}
	if got := e2eMySQL(t, "SELECT id, e+0 FROM shop.ezero ORDER BY id"); got != indexes {
		t.Fatalf("enum 0 indexes\nbefore:\n%q\nrestored:\n%q", indexes, got)
	}
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
	base := strings.Fields(os.Getenv("BINLOGVIZ_MYSQL"))
	if len(base) == 0 {
		base = []string{"sudo", "mysql"}
	}
	args := append(append([]string{}, base[1:]...), "--default-character-set=utf8mb4", "-N", "--batch")
	cmd := exec.Command(base[0], args...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mysql: %v\n%s\nSQL:\n%s", err, out, stdin)
	}
	return string(out)
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
