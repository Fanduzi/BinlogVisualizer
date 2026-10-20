package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Offline checks for the MySQL 5.7 / 8.4 / 9.x and MariaDB fixtures in
// internal/binlog/testdata. Row images are compared to the committed
// mysqlbinlog or mariadb-binlog -vv decode. No server and no env var.
func TestVersionMatrixAnalyzeMatchesDecode(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, name := range versionMatrixFixtures {
		t.Run(name, func(t *testing.T) {
			path := mustFixturePath(t, name)
			decode := versionMatrixDecode(t, path)
			server := serverVersionFromDecode(decode)
			if server == "" {
				t.Fatal("decode has no server version")
			}
			stdout, stderr, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--format", "json", "--top-transactions", "0")
			if err != nil {
				t.Fatalf("analyze: %v\n%s", err, stderr)
			}
			doc := decodeVersionReport(t, stdout)
			if len(doc.Provenance.ServerVersions) != 1 || doc.Provenance.ServerVersions[0] != server {
				t.Fatalf("server_versions=%v, decode %q", doc.Provenance.ServerVersions, server)
			}
			flavor := "mysql"
			replay := "mysqlbinlog"
			if strings.Contains(server, "MariaDB") {
				flavor = "mariadb"
				replay = "mariadb-binlog"
			}
			if len(doc.Provenance.ServerFlavors) != 1 || doc.Provenance.ServerFlavors[0] != flavor {
				t.Fatalf("server_flavors=%v, want %s", doc.Provenance.ServerFlavors, flavor)
			}
			mb := parseIndexedMysqlbinlogRows(decode)
			viz := vizRowsInOrder(doc.rowReport)
			if len(viz) != len(mb) || len(mb) == 0 {
				t.Fatalf("rows viz=%d mysqlbinlog=%d", len(viz), len(mb))
			}
			for i := range viz {
				if !indexedRowAgrees(viz[i], mb[i], strings.Contains(name, "5.7.44")) {
					t.Fatalf("row %d\nviz=%+v\nmysqlbinlog=%+v", i, viz[i], mb[i])
				}
			}
			if doc.Summary.TotalRows != len(mb) {
				t.Fatalf("total_rows=%d mysqlbinlog rows=%d", doc.Summary.TotalRows, len(mb))
			}
			names := "full"
			if strings.Contains(name, "5.7.44") {
				names = "positional"
			}
			if viz[0].Names != names {
				t.Fatalf("column names %q, want %q (%v)", viz[0].Names, names, viz[0].Columns)
			}
			if names == "positional" && doc.ColumnNamesNote == "" {
				t.Fatal("5.7 report did not say column names are missing")
			}
			if names == "full" && doc.ColumnNamesNote != "" {
				t.Fatalf("FULL metadata report claimed names are missing: %s", doc.ColumnNamesNote)
			}
			for _, txn := range doc.Transactions {
				if !strings.Contains(txn.MysqlbinlogCmd, replay) {
					t.Fatalf("replay %q, want %s", txn.MysqlbinlogCmd, replay)
				}
			}
			if strings.Contains(name, "-ddl") {
				joined := ddlStatementText(doc)
				for _, stmt := range ddlStatementsInDecode(decode) {
					if !strings.Contains(joined, stmt) && !strings.Contains(stdout, trimDDLComment(stmt)) {
						t.Fatalf("report missing DDL %q\n%s", stmt, joined)
					}
				}
			}

			text, textErr, err := executeAnalyzeLikeMain(t, path, "--format", "text")
			if err != nil {
				t.Fatalf("text: %v\n%s", err, textErr)
			}
			if !strings.Contains(text, "Total Rows: "+strconv.Itoa(len(mb))) {
				t.Fatalf("text report rows:\n%s", text)
			}
			unavailable := strings.Contains(text, "commit timestamps unavailable")
			if flavor == "mysql" && strings.HasPrefix(server, "5.7.") && !unavailable {
				t.Fatalf("5.7 text should say commit timestamps are unavailable:\n%s", text)
			}
			if flavor == "mariadb" && !unavailable {
				t.Fatalf("MariaDB text should say commit timestamps are unavailable:\n%s", text)
			}
			if (strings.HasPrefix(server, "8.") || strings.HasPrefix(server, "9.")) && unavailable {
				t.Fatalf("MySQL %s text dropped commit timestamps:\n%s", server, text)
			}
		})
	}
}

func TestVersionMatrixFlashbackBoundary(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	refusals := []struct {
		file string
		args []string
		want string
	}{
		{"mysql-5.7.44-wide-full.binlog", nil, "column names are unavailable"},
		{"mysql-5.7.44-wide-minimal.binlog", nil, "column names are unavailable"},
		{"mysql-5.7.44-ddl.binlog", nil, "DDL in the selected range"},
		{"mysql-8.4.11-wide-minimal.binlog", nil, "row image is not FULL"},
		{"mysql-8.4.11-ddl.binlog", nil, "DDL in the selected range"},
		{"mysql-9.7.2-wide-minimal.binlog", nil, "row image is not FULL"},
		{"mysql-9.7.2-ddl.binlog", nil, "DDL in the selected range"},
		{"mariadb-10.11.19-wide-minimal.binlog", nil, "row image is not FULL"},
		{"mariadb-10.11.19-ddl.binlog", nil, "DDL in the selected range"},
		// 11.8 logs binlog_row_image=FULL but omits generated columns, so the
		// image is incomplete and flashback refuses the whole file.
		{"mariadb-11.8.9-wide-full.binlog", nil, "row image is not FULL"},
		{"mariadb-11.8.9-wide-minimal.binlog", nil, "row image is not FULL"},
		{"mariadb-11.8.9-ddl.binlog", nil, "DDL in the selected range"},
		// DELETE-only selections exercise the before-image check alone: every
		// other minimal case also has an UPDATE, whose after-image refuses.
		{"mysql-8.4.11-wide-minimal.binlog", []string{"--dml", "delete"}, "row image is not FULL"},
		{"mysql-9.7.2-wide-minimal.binlog", []string{"--dml", "delete"}, "row image is not FULL"},
		{"mariadb-10.11.19-wide-minimal.binlog", []string{"--dml", "delete"}, "row image is not FULL"},
		{"mariadb-11.8.9-wide-full.binlog", []string{"--dml", "delete"}, "row image is not FULL"},
		{"mariadb-11.8.9-wide-full.binlog", []string{"--dml", "delete", "--include-table", "shop.wide"}, "row image is not FULL"},
		{"mysql-8.4.11-minimal-delete-only.binlog", nil, "row image is not FULL"},
		// MariaDB's default binlog_row_metadata is NO_LOG. The refusal names
		// the MariaDB setting, not MySQL 8.0.14 or 5.7.
		{"mariadb-10.11.19-nolog.binlog", nil, "which MariaDB has from 10.5"},
		{"mariadb-11.8.9-nolog.binlog", nil, "which MariaDB has from 10.5"},
	}
	for _, tc := range refusals {
		t.Run(strings.Join(append([]string{tc.file}, tc.args...), " "), func(t *testing.T) {
			path := mustFixturePath(t, tc.file)
			stdout, stderr, err := executeFlashbackLikeMain(t, append([]string{path}, tc.args...)...)
			assertFlashbackRefused(t, stdout, stderr, err, tc.want)
			if strings.Contains(tc.file, "5.7.44-wide") && !strings.Contains(err.Error(), "MySQL 5.7") {
				t.Fatalf("5.7 refusal did not name MySQL 5.7: %v", err)
			}
			if strings.Contains(tc.file, "-nolog") && (strings.Contains(err.Error(), "MySQL") || !strings.Contains(err.Error(), "binlog_row_metadata=FULL")) {
				t.Fatalf("MariaDB metadata refusal names MySQL or not the setting: %v", err)
			}
		})
	}

	mysqlSchema := writeVersionMatrixSchema(t, "json")
	// MariaDB stores JSON as longtext. A dump from that server says longtext.
	mariaSchema := writeVersionMatrixSchema(t, "longtext")
	scripts := []struct {
		file   string
		schema string
	}{
		{"mysql-8.4.11-wide-full.binlog", mysqlSchema},
		{"mysql-9.7.2-wide-full.binlog", mysqlSchema},
		{"mariadb-10.11.19-wide-full.binlog", mariaSchema},
	}
	for _, tc := range scripts {
		t.Run(tc.file, func(t *testing.T) {
			path := mustFixturePath(t, tc.file)
			stdout, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", tc.schema)
			if err != nil {
				t.Fatalf("flashback: %v\n%s", err, stderr)
			}
			if strings.Contains(stderr, "Error:") || strings.Contains(stderr, "generated columns cannot be ruled out") {
				t.Fatalf("stderr:\n%s", stderr)
			}
			assertVersionMatrixUndo(t, stdout)
		})
	}

	t.Run("mariadb-11.8.9-heap", func(t *testing.T) {
		path := mustFixturePath(t, "mariadb-11.8.9-wide-full.binlog")
		stdout, stderr, err := executeFlashbackLikeMain(t, path, "--include-table", "shop.heap", "--schema-file", mariaSchema)
		if err != nil {
			t.Fatalf("heap flashback: %v\n%s", err, stderr)
		}
		if strings.Contains(stdout, "shop`.`wide") || strings.Contains(stdout, "`wide`") {
			t.Fatalf("heap filter kept wide:\n%s", stdout)
		}
		if !strings.Contains(stdout, "gone") || !strings.Contains(stdout, "INSERT INTO") {
			t.Fatalf("heap undo:\n%s", stdout)
		}
		if strings.Contains(stdout, "CREATE TABLE") || strings.Contains(stdout, "ALTER ") || strings.Contains(stdout, "DROP TABLE") {
			t.Fatalf("emitted DDL:\n%s", stdout)
		}
	})
}

func assertVersionMatrixUndo(t *testing.T, sql string) {
	t.Helper()
	if strings.Contains(sql, "CREATE TABLE") || strings.Contains(sql, "ALTER ") || strings.Contains(sql, "DROP TABLE") {
		t.Fatalf("emitted DDL:\n%s", sql)
	}
	if strings.Contains(sql, "`virt`") || strings.Contains(sql, "`stor`") {
		t.Fatalf("undo wrote generated columns:\n%s", sql)
	}
	// A primary key undo writes the before image and matches the key.
	// The after image (-12.34, the updated note, DEADBEEF) is not in the SQL.
	for _, want := range []string{
		"3000000000",
		"`price` = 10.00",
		"2026-10-06 14:00:01.000000",
		"`note` = \\'alpha\\'",
		"`color` = 1",
		"`flags` = 1",
		"`base` = 10",
		"DELETE FROM `shop`.`wide` WHERE `id` <=> 3",
		"gone",
		"XA START",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("undo missing %q\n%s", want, sql)
		}
	}
	if strings.Count(sql, "XA START") != 2 {
		t.Fatalf("undo transactions:\n%s", sql)
	}
	// The after image of the updated row is not undone.
	for _, absent := range []string{"-12.34", "DEADBEEF", "14:05:01.123456"} {
		if strings.Contains(sql, absent) {
			t.Fatalf("undo contains after image %q\n%s", absent, sql)
		}
	}
}

func writeVersionMatrixSchema(t *testing.T, payloadType string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), payloadType+".sql")
	schema := "USE `shop`;\n" +
		"CREATE TABLE `wide` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `qty` int unsigned DEFAULT NULL,\n" +
		"  `price` decimal(10,2) DEFAULT NULL,\n" +
		"  `created_at` datetime(6) DEFAULT NULL,\n" +
		"  `updated_at` timestamp(6) NULL DEFAULT NULL,\n" +
		"  `note` text,\n" +
		"  `raw` blob,\n" +
		"  `payload` " + payloadType + " DEFAULT NULL,\n" +
		"  `color` enum('red','blue','green') DEFAULT NULL,\n" +
		"  `flags` set('a','b','c') DEFAULT NULL,\n" +
		"  `base` int DEFAULT NULL,\n" +
		"  `virt` int GENERATED ALWAYS AS ((`base` + 1)) VIRTUAL,\n" +
		"  `stor` int GENERATED ALWAYS AS ((`base` * 2)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n" +
		"CREATE TABLE `heap` (\n" +
		"  `id` int DEFAULT NULL,\n" +
		"  `note` varchar(32) DEFAULT NULL\n" +
		");\n"
	if err := os.WriteFile(path, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

var versionMatrixFixtures = []string{
	"mysql-5.7.44-wide-full.binlog",
	"mysql-5.7.44-wide-minimal.binlog",
	"mysql-5.7.44-ddl.binlog",
	"mysql-8.4.11-wide-full.binlog",
	"mysql-8.4.11-wide-minimal.binlog",
	"mysql-8.4.11-ddl.binlog",
	"mysql-9.7.2-wide-full.binlog",
	"mysql-9.7.2-wide-minimal.binlog",
	"mysql-9.7.2-ddl.binlog",
	"mariadb-10.11.19-wide-full.binlog",
	"mariadb-10.11.19-wide-minimal.binlog",
	"mariadb-10.11.19-ddl.binlog",
	"mariadb-11.8.9-wide-full.binlog",
	"mariadb-11.8.9-wide-minimal.binlog",
	"mariadb-11.8.9-ddl.binlog",
}

type versionReport struct {
	rowReport
	Provenance struct {
		ServerVersions []string `json:"server_versions"`
		ServerFlavors  []string `json:"server_flavors"`
	} `json:"provenance"`
	Diagnostics struct {
		DDLEvents []struct {
			Statement string `json:"statement"`
		} `json:"ddl_events"`
	} `json:"diagnostics"`
}

func decodeVersionReport(t *testing.T, stdout string) versionReport {
	t.Helper()
	var doc versionReport
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("json: %v\n%s", err, stdout)
	}
	return doc
}

func ddlStatementText(doc versionReport) string {
	var b strings.Builder
	for _, ev := range doc.Diagnostics.DDLEvents {
		b.WriteString(ev.Statement)
		b.WriteByte('\n')
	}
	return b.String()
}

type mbCell struct {
	col int
	raw string
}

type mbRow struct {
	op     string
	table  string
	before []mbCell
	after  []mbCell
}

// versionMatrixUnsigned names the UNSIGNED integer columns that
// create_version_matrix.sh creates, by table and 1-based position. For such
// a column mysqlbinlog -vv prints the signed reading first and the unsigned
// one in parentheses.
var versionMatrixUnsigned = map[string]map[int]bool{
	"shop.wide": {2: true}, // qty INT UNSIGNED
}

// mbTable reads `db`.`tbl` after the row verb of a mysqlbinlog -vv line.
func mbTable(line string) string {
	i := strings.IndexByte(line, '`')
	if i < 0 {
		return ""
	}
	return strings.ReplaceAll(strings.TrimSpace(line[i:]), "`", "")
}

func parseIndexedMysqlbinlogRows(text string) []mbRow {
	var rows []mbRow
	var cur *mbRow
	section := ""
	flush := func() {
		if cur != nil {
			rows = append(rows, *cur)
			cur = nil
		}
	}
	for _, line := range strings.Split(text, "\n") {
		switch {
		case strings.HasPrefix(line, "### INSERT INTO"):
			flush()
			cur = &mbRow{op: "INSERT", table: mbTable(line)}
			section = "after"
		case strings.HasPrefix(line, "### UPDATE "):
			flush()
			cur = &mbRow{op: "UPDATE", table: mbTable(line)}
			section = "before"
		case strings.HasPrefix(line, "### DELETE FROM"):
			flush()
			cur = &mbRow{op: "DELETE", table: mbTable(line)}
			section = "before"
		case strings.HasPrefix(line, "### WHERE"):
			section = "before"
		case strings.HasPrefix(line, "### SET"):
			section = "after"
		case strings.HasPrefix(line, "###   @") && cur != nil:
			col, raw, ok := splitMysqlbinlogCell(line)
			if !ok {
				continue
			}
			cell := mbCell{col: col, raw: raw}
			if section == "before" {
				cur.before = append(cur.before, cell)
			} else {
				cur.after = append(cur.after, cell)
			}
		}
	}
	flush()
	return rows
}

func splitMysqlbinlogCell(line string) (int, string, bool) {
	rest := strings.TrimPrefix(line, "###   @")
	eq := strings.IndexByte(rest, '=')
	if eq < 0 {
		return 0, "", false
	}
	col, err := strconv.Atoi(rest[:eq])
	if err != nil || col <= 0 {
		return 0, "", false
	}
	raw := rest[eq+1:]
	if i := strings.LastIndex(raw, " /*"); i >= 0 && strings.HasSuffix(strings.TrimRight(raw, " \t"), "*/") {
		raw = strings.TrimRight(raw[:i], " ")
	}
	return col, raw, true
}

// indexedRowAgrees compares one analyze row with mysqlbinlog's. positional
// is a binlog without row metadata (5.7), where analyze cannot know
// signedness and prints mysqlbinlog's signed/unsigned pair as is.
func indexedRowAgrees(viz vizImage, mb mbRow, positional bool) bool {
	if viz.Op != mb.op {
		return false
	}
	unsigned := versionMatrixUnsigned[mb.table]
	switch viz.Op {
	case "DELETE":
		return sideAgrees(viz.Before, mb.before, unsigned, positional)
	case "UPDATE":
		return sideAgrees(viz.Before, mb.before, unsigned, positional) && sideAgrees(viz.After, mb.after, unsigned, positional)
	default:
		return sideAgrees(viz.After, mb.after, unsigned, positional)
	}
}

func sideAgrees(viz []any, cells []mbCell, unsigned map[int]bool, positional bool) bool {
	if len(cells) == 0 {
		return false
	}
	for _, cell := range cells {
		if cell.col > len(viz) || !cellAgrees(viz[cell.col-1], cell.raw, unsigned[cell.col], positional) {
			return false
		}
	}
	return true
}

// cellAgrees compares one cell. A signed/unsigned pair from mysqlbinlog
// ("-1294967296 (3000000000)") must match exactly one reading: the unsigned
// one for an UNSIGNED column, the signed one otherwise, and the pair as
// printed when the binlog has no signedness. Accepting either half would
// hide a signedness bug.
func cellAgrees(viz any, raw string, unsigned, positional bool) bool {
	if left, right, ok := splitSignedPair(raw); ok && !strings.HasPrefix(raw, "'") {
		text, isText := viz.(string)
		switch {
		case !isText:
			return false
		case positional:
			return text == raw
		case unsigned:
			return text == right
		default:
			return text == left
		}
	}
	if cellMatch(viz, raw) {
		return true
	}
	text, ok := viz.(string)
	if !ok {
		return false
	}
	if bits, ok := mysqlbinlogSetBits(raw); ok && text == strconv.FormatUint(bits, 10) {
		return true
	}
	// mariadb-binlog leaves a literal backslash in a quoted string.
	// mysqlbinlog writes it as \\, which cellMatch already unescapes.
	if strings.HasPrefix(text, "0x") && strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'") && len(raw) >= 2 {
		return blobMatch(text, []byte(raw[1:len(raw)-1]))
	}
	return false
}

func mysqlbinlogSetBits(raw string) (uint64, bool) {
	if !strings.HasPrefix(raw, "b'") || !strings.HasSuffix(raw, "'") {
		return 0, false
	}
	bits := raw[2 : len(raw)-1]
	if bits == "" || strings.IndexFunc(bits, func(r rune) bool { return r != '0' && r != '1' }) >= 0 {
		return 0, false
	}
	v, err := strconv.ParseUint(bits, 2, 64)
	return v, err == nil
}

func serverVersionFromDecode(text string) string {
	const mark = "server v "
	i := strings.Index(text, mark)
	if i < 0 {
		return ""
	}
	rest := text[i+len(mark):]
	end := strings.IndexAny(rest, " \n")
	if end < 0 {
		return rest
	}
	return rest[:end]
}

func ddlStatementsInDecode(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		switch {
		case strings.HasPrefix(line, "DROP "), strings.HasPrefix(line, "CREATE "), strings.HasPrefix(line, "ALTER "):
			out = append(out, trimDDLComment(line))
		}
	}
	return out
}

func trimDDLComment(stmt string) string {
	stmt = strings.TrimSpace(stmt)
	if i := strings.Index(stmt, " /*"); i >= 0 {
		stmt = strings.TrimSpace(stmt[:i])
	}
	return stmt
}

func versionMatrixDecode(t *testing.T, binlogPath string) string {
	t.Helper()
	raw, err := os.ReadFile(strings.TrimSuffix(binlogPath, ".binlog") + ".mysqlbinlog.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
