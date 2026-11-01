package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func prepareFlashbackRoundTripSQL(t *testing.T, dir, incidentPath string) (genSQL, cjSQL, esSQL string) {
	t.Helper()
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
	return genSQL, cjSQL, esSQL
}

func assertRoundTripFlashbackSQL(t *testing.T, sql string) {
	t.Helper()
	if !strings.Contains(sql, "18446744073709551615") || !strings.Contains(sql, "4000000000") || !strings.Contains(sql, "3000000000") || strings.Contains(sql, "-294967296") || strings.Contains(sql, "-1294967296") || !strings.Contains(sql, "JSON_OBJECT(") || !strings.Contains(sql, "_latin1 ") || !strings.Contains(sql, "_utf16 ") {
		t.Fatalf("live flashback SQL:\n%s", sql)
	}
	if strings.Contains(sql, "`virt`") || strings.Contains(sql, "`stor`") {
		t.Fatalf("wrote generated columns:\n%s", sql)
	}
}
