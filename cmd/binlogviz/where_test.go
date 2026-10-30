package binlogviz

import (
	"strings"
	"testing"
	"time"
)

func TestCompileWhereFlagRejectsInvalid(t *testing.T) {
	// A malformed or out-of-subset predicate is a usage error decided
	// before any input is read (ADR 0024).
	err := compileWhereFlag(&analyzeOptions{where: "a BETWEEN 1 AND 2"})
	if err == nil || !strings.Contains(err.Error(), "invalid --where") {
		t.Fatalf("err = %v", err)
	}
}

func TestCompileWhereFlagMapsToAnalyzerOptions(t *testing.T) {
	opts := &analyzeOptions{where: "user_id < 400"}
	if err := compileWhereFlag(opts); err != nil {
		t.Fatal(err)
	}
	if opts.whereExpr == nil {
		t.Fatal("predicate not compiled")
	}
	got := buildAnalyzerOptions(opts, time.Time{}, time.Time{})
	if got.RowWhere != opts.whereExpr {
		t.Fatal("RowWhere not mapped")
	}
}

func TestCompileWhereFlagEmptyIsNil(t *testing.T) {
	opts := &analyzeOptions{}
	if err := compileWhereFlag(opts); err != nil {
		t.Fatal(err)
	}
	if opts.whereExpr != nil {
		t.Fatal("empty --where should not compile")
	}
}

func TestWhereUsageErrorBeforeInput(t *testing.T) {
	// The predicate compiles at flag time: a bad expression is a usage error
	// even on a valid fixture (ADR 0024).
	_, _, err := redoUsageError(t,
		mustFixturePath(t, "mysql-8.0.46-dml-full.binlog"), "--where", "a BETWEEN 1 AND 2")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("bad --where = %v, want usage error", err)
	}
}

func TestFlashbackWhereFiltersRows(t *testing.T) {
	// shop.wide: the predicate reads each row's matched image. The DELETE
	// (id=1), the UPDATEs (before ids 2, 4, 3 — the 3→30 update matches on
	// before, not on its after 30) all drop; only the three INSERTs
	// (after ids 10, 11, 12) stay (ADR 0024).
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	stdout, stderr, err := executeFlashbackLikeMain(t, path,
		"--include-table", "shop.wide", "--where", "id >= 10")
	if err != nil {
		t.Fatalf("flashback --where: %v\n%s", err, stderr)
	}
	var dml []string
	for _, stmt := range undoStatements(stdout) {
		if strings.HasPrefix(stmt, "INSERT") || strings.HasPrefix(stmt, "UPDATE") || strings.HasPrefix(stmt, "DELETE") {
			dml = append(dml, stmt)
		}
	}
	if len(dml) != 3 {
		t.Fatalf("filtered undo emitted %d DML:\n%s", len(dml), strings.Join(dml, "\n"))
	}
	for _, want := range []string{
		"DELETE FROM `shop`.`wide` WHERE `id` <=> 10 AND `bucket` <=> 1",
		"DELETE FROM `shop`.`wide` WHERE `id` <=> 11 AND `bucket` <=> 1",
		"DELETE FROM `shop`.`wide` WHERE `id` <=> 12 AND `bucket` <=> 2",
	} {
		if !flashbackScriptHas(stdout, want) {
			t.Fatalf("missing %q\n%s", want, stdout)
		}
	}
}

func TestFlashbackWhereRefusesUnknownColumn(t *testing.T) {
	// A predicate naming a column the row does not have is the exit-6
	// where_column refusal — the ERROR 1054 shape (ADR 0024).
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	stdout, stderr, err := executeFlashbackLikeMain(t, path,
		"--include-table", "shop.wide", "--where", "nosuch = 1")
	assertFlashbackRefused(t, stdout, stderr, err, "--where references column 'nosuch'")
}

func TestFlashbackWhereNoMatchIsExit2(t *testing.T) {
	// A predicate no selected row satisfies is the same exit 2 as a
	// selection that matched nothing (ADR 0024).
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	stdout, stderr, err := executeFlashbackLikeMain(t, path,
		"--include-table", "shop.wide", "--where", "id > 100000")
	assertAnalyzeNoDataExit(t, stdout, stderr, err, "no")
}

// redoDML is the emitted DML statements of a redo script: comment lines
// quoting the source statements stay comments and do not count (ADR 0024
// filters the emitted rows, not the provenance text).
func redoDML(script string) []string {
	var out []string
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(line, "INSERT INTO") || strings.HasPrefix(line, "UPDATE ") || strings.HasPrefix(line, "DELETE FROM") {
			out = append(out, line)
		}
	}
	return out
}

func TestRedoWhereFiltersRows(t *testing.T) {
	// shop.orders keeps ids 1,2,3's INSERTs, the id=2 DELETE, and both
	// UPDATEs (before ids 1 and 3). The 1000+ batch, the 3000000000 write,
	// and its DELETE drop; the kept and skipped rows share transactions, so
	// the partial-replay warning fires (ADR 0024).
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	stdout, stderr, err := executeRedoLikeMain(t, path, "--where", "id < 1000")
	if err != nil {
		t.Fatalf("redo --where: %v\n%s", err, stderr)
	}
	dml := strings.Join(redoDML(stdout), "\n")
	for _, want := range []string{
		"VALUES (1, 1, 10.00, 'alpha'",
		"VALUES (2, 2, 10.50, 'beta'",
		"VALUES (3, -4, 0.00, NULL",
		"DELETE FROM `shop`.`orders` WHERE `id` <=> 2",
		"SET `id` = 1",
		"SET `id` = 3",
	} {
		if !strings.Contains(dml, want) {
			t.Fatalf("script missing %q:\n%s", want, stdout)
		}
	}
	for _, drop := range []string{"1000", "3000000000"} {
		if strings.Contains(dml, drop) {
			t.Fatalf("filtered rows emitted %q:\n%s", drop, stdout)
		}
	}
	if !strings.Contains(stderr, "partly replayed") {
		t.Fatalf("no partial-transaction warning:\n%s", stderr)
	}
}

func TestRedoWhereComposesWithDML(t *testing.T) {
	// --where ANDs with --dml: only deletes whose before image matches.
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	stdout, stderr, err := executeRedoLikeMain(t, path,
		"--dml", "delete", "--where", "id < 1000")
	if err != nil {
		t.Fatalf("redo --dml --where: %v\n%s", err, stderr)
	}
	dml := redoDML(stdout)
	if len(dml) != 1 || !strings.Contains(dml[0], "DELETE FROM `shop`.`orders` WHERE `id` <=> 2") {
		t.Fatalf("kept delete missing:\n%s", stdout)
	}
}

func TestRedoWhereRefusesUnknownColumn(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	stdout, stderr, err := executeRedoLikeMain(t,
		mustFixturePath(t, "mysql-8.0.46-dml-full.binlog"), "--where", "nosuch = 1")
	if err == nil || ExitCode(err) != 6 {
		t.Fatalf("exit %d, want 6; err=%v\nstderr=%s", ExitCode(err), err, stderr)
	}
	if stdout != "" {
		t.Fatalf("refusal printed SQL:\n%s", stdout)
	}
	if !strings.Contains(err.Error(), "where") || !strings.Contains(err.Error(), "nosuch") {
		t.Fatalf("err = %v", err)
	}
}

func TestRedoWhereMinimalStillRefusesNames(t *testing.T) {
	// --where does not rescue MINIMAL metadata: the predicate reads named
	// columns, which the row does not carry (ADR 0024).
	forceEnglishRuntimeOutput(t)
	stdout, _, err := executeRedoLikeMain(t,
		mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"), "--where", "id < 1000")
	if err == nil || ExitCode(err) != 6 {
		t.Fatalf("exit %d, want 6; err=%v", ExitCode(err), err)
	}
	if stdout != "" {
		t.Fatalf("refusal printed SQL:\n%s", stdout)
	}
}
