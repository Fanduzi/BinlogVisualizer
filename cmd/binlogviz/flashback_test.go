package binlogviz

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
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
		"'blue'",
		"'a,c'",
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
	if strings.Contains(out, "Only count these ROW kinds") || !strings.Contains(out, "Only undo these ROW kinds") || !strings.Contains(out, "Print SQL that reverses selected ROW changes") {
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
	if !strings.Contains(out, "只撤销这些 ROW 类型") || !strings.Contains(out, "打印 SQL，把选中的 ROW 变更撤回去") {
		t.Fatalf("zh help:\n%s", out)
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

	const checksumSQL = "CHECKSUM TABLE shop.wide, shop.heap, shop.jdoc, shop.jheap, shop.chars, shop.gen"
	e2eMySQL(t, "RESET MASTER")
	e2eMySQL(t, readTestdata(t, "flashback_setup.sql"))
	beforeSum := e2eMySQL(t, checksumSQL)
	beforeRows := e2eMySQL(t, flashbackOrderSQL)
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
	args := make([]string, 0, len(names))
	for _, name := range names[:len(names)-1] {
		dst := dir + "/" + name
		e2eCopy(t, datadir+"/"+name, dst)
		args = append(args, dst)
	}
	args = append(args, "--exclude-gtids", executed)

	sql, stderr, err := executeFlashbackLikeMain(t, args...)
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
SELECT id, base, stor FROM shop.gen ORDER BY id;
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
