package binlogviz

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDMLFilterAndShowRowsOnMySQL80Fixture(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, fixture := range []string{
		"mysql-8.0.46-dml-minimal.binlog",
		"mysql-8.0.46-dml-full.binlog",
	} {
		t.Run(fixture, func(t *testing.T) {
			assertDMLRowsFixture(t, fixture)
		})
	}
}

func assertDMLRowsFixture(t *testing.T, name string) {
	t.Helper()
	full := strings.Contains(name, "full")
	path := mustFixturePath(t, name)
	decodePath := strings.TrimSuffix(path, ".binlog") + ".mysqlbinlog.txt"
	checked, err := os.ReadFile(decodePath)
	if err != nil {
		t.Fatalf("read mysqlbinlog decode: %v", err)
	}
	if live, err := exec.LookPath("mysqlbinlog"); err == nil {
		out, err := exec.Command(live, "-v", "--base64-output=DECODE-ROWS", path).Output()
		if err != nil {
			t.Fatalf("mysqlbinlog: %v", err)
		}
		if n, m := len(parseMysqlbinlogRows(string(out))), len(parseMysqlbinlogRows(string(checked))); n != m {
			t.Fatalf("live mysqlbinlog rows %d, checked-in decode %d", n, m)
		}
	}

	stdout, stderr, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--format", "json", "--top-transactions", "0")
	if err != nil {
		t.Fatalf("analyze: %v\n%s", err, stderr)
	}
	doc := decodeRowReport(t, stdout)
	if doc.ColumnNamesNote == "" && !full {
		t.Fatal("MINIMAL report must say column names are missing")
	}
	if full && doc.ColumnNamesNote != "" {
		t.Fatalf("FULL report must not claim names are missing: %s", doc.ColumnNamesNote)
	}
	viz := vizRowsInOrder(doc)
	mb := parseMysqlbinlogRows(string(checked))
	omitted := 0
	for _, txn := range doc.Transactions {
		omitted += txn.RowsOmitted
		if txn.MysqlbinlogCmd == "" || !strings.Contains(txn.MysqlbinlogCmd, "mysqlbinlog") {
			t.Fatalf("transaction missing mysqlbinlog_cmd: %+v", txn.MysqlbinlogCmd)
		}
	}
	if omitted != 11 {
		t.Fatalf("rows_omitted=%d, want 11", omitted)
	}
	matchRowImages(t, viz, mb, omitted)
	assertKnownCells(t, viz, full)

	text, _, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--include-table", "shop.orders", "--dml", "delete")
	if err != nil {
		t.Fatalf("text analyze: %v", err)
	}
	if !strings.Contains(text, "DML filter: DELETE") {
		t.Fatalf("text report did not name the DML filter:\n%s", text)
	}
	if !strings.Contains(text, "19.99") || !strings.Contains(text, `it\'s \"bad\"`) && !strings.Contains(text, `'it\'s "bad"'`) {
		t.Fatalf("text report missing delete row values:\n%s", text)
	}
	if !strings.Contains(text, "mysqlbinlog") {
		t.Fatal("text report dropped mysqlbinlog_cmd")
	}
	if full {
		if !strings.Contains(text, "id=3000000000") {
			t.Fatalf("FULL text should name the unsigned id:\n%s", text)
		}
	} else if !strings.Contains(text, "column names unavailable") {
		t.Fatal("MINIMAL text missing the positional-name note")
	}

	md, _, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--dml", "delete", "--format", "markdown")
	if err != nil {
		t.Fatalf("markdown: %v", err)
	}
	if !strings.Contains(md, "19.99") || !strings.Contains(md, "DELETE") {
		t.Fatalf("markdown missing row values:\n%s", md)
	}
	html, _, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--dml", "delete", "--format", "html", "--output", "-")
	if err != nil {
		t.Fatalf("html: %v", err)
	}
	if !strings.Contains(html, "19.99") || !strings.Contains(html, "DML filter") {
		t.Fatalf("html missing row values")
	}

	filtered, _, err := executeAnalyzeLikeMain(t, path, "--dml", "delete", "--include-table", "orders", "--format", "json")
	if err != nil {
		t.Fatalf("dml filter: %v", err)
	}
	narrow := decodeRowReport(t, filtered)
	if narrow.Summary.TotalRows != 2 || narrow.Summary.TotalTransactions != 1 || narrow.Summary.TotalEvents > 4 {
		t.Fatalf("delete filter summary = %+v", narrow.Summary)
	}
	if len(narrow.Scope.DML) != 1 || narrow.Scope.DML[0] != "DELETE" {
		t.Fatalf("scope dml = %v", narrow.Scope.DML)
	}
	for _, table := range narrow.Tables {
		if table.InsertRows != 0 || table.UpdateRows != 0 || table.DeleteRows != 2 {
			t.Fatalf("table counts = %+v", table)
		}
	}
	for _, txn := range narrow.Transactions {
		if txn.Operations["DELETE"] != 2 || txn.Operations["INSERT"] != 0 || txn.Operations["UPDATE"] != 0 {
			t.Fatalf("txn ops = %v", txn.Operations)
		}
	}

	off, _, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--sql-context", "off", "--dml", "delete")
	if err != nil {
		t.Fatalf("sql-context off: %v", err)
	}
	if strings.Contains(off, "19.99") || strings.Contains(off, "it's") {
		t.Fatalf("--sql-context off leaked row values:\n%s", off)
	}
	if !strings.Contains(off, "row values omitted because --sql-context is off") {
		t.Fatalf("missing suppression note:\n%s", off)
	}

	plain, _, err := executeAnalyzeLikeMain(t, mustFixturePath(t, "minimal.binlog"), "--format", "json")
	if err != nil {
		t.Fatalf("default analyze: %v", err)
	}
	plainDoc := decodeRowReport(t, plain)
	for _, txn := range plainDoc.Transactions {
		if len(txn.Rows) != 0 || txn.RowsOmitted != 0 {
			t.Fatalf("default report kept row images: %+v", txn)
		}
	}
	for _, forbidden := range []string{`"dml"`, `"column_names_note"`, `"row_values_note"`} {
		if strings.Contains(plain, forbidden) {
			t.Fatalf("default JSON contains %s", forbidden)
		}
	}
}

func TestDMLFilterMissExit2(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-flush-tables.binlog")
	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--dml", "delete")
	if err == nil || ExitCode(err) != 2 {
		t.Fatalf("exit=%d err=%v", ExitCode(err), err)
	}
	if stdout != "" {
		t.Fatalf("stdout=%q", stdout)
	}
	if !strings.Contains(err.Error(), "dml filter matched no events") || strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("err=%v\nstderr=%s", err, stderr)
	}
	_, _, bad := executeAnalyzeLikeMain(t, fixture, "--dml", "truncate")
	if bad == nil || ExitCode(bad) != 1 || !strings.Contains(bad.Error(), "invalid --dml") {
		t.Fatalf("invalid dml: %v", bad)
	}
}

func TestMariaDBShowRowsDoesNotCrash(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mariadb-10.11.14-dml.binlog")
	stdout, stderr, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--format", "json", "--top-transactions", "0")
	if err != nil {
		t.Fatalf("mariadb analyze: %v\n%s", err, stderr)
	}
	doc := decodeRowReport(t, stdout)
	if doc.Summary.TotalRows < 1 {
		t.Fatalf("mariadb summary = %+v", doc.Summary)
	}
	rows := vizRowsInOrder(doc)
	if len(rows) == 0 {
		t.Fatal("mariadb --show-rows produced no images")
	}
	for _, row := range rows {
		if row.Names == "full" {
			continue
		}
		if len(row.Columns) == 0 || !strings.HasPrefix(row.Columns[0], "@") {
			t.Fatalf("positional columns = %v", row.Columns)
		}
	}
	if !strings.Contains(stdout, "mysqlbinlog") && !strings.Contains(stdout, "mariadb-binlog") {
		t.Fatal("replay command missing")
	}
}

type rowReport struct {
	Summary struct {
		TotalRows         int `json:"total_rows"`
		TotalTransactions int `json:"total_transactions"`
		TotalEvents       int `json:"total_events"`
	} `json:"summary"`
	Scope struct {
		DML []string `json:"dml"`
	} `json:"scope"`
	ColumnNamesNote string `json:"column_names_note"`
	Tables          []struct {
		Schema     string `json:"schema"`
		Table      string `json:"table"`
		InsertRows int    `json:"insert_rows"`
		UpdateRows int    `json:"update_rows"`
		DeleteRows int    `json:"delete_rows"`
	} `json:"tables"`
	Transactions []struct {
		PosStart       int64          `json:"pos_start"`
		MysqlbinlogCmd string         `json:"mysqlbinlog_cmd"`
		Operations     map[string]int `json:"operations"`
		RowsOmitted    int            `json:"rows_omitted"`
		Rows           []vizImage     `json:"rows"`
	} `json:"transactions"`
}

type vizImage struct {
	Op      string   `json:"op"`
	Columns []string `json:"columns"`
	Names   string   `json:"names"`
	Before  []any    `json:"before"`
	After   []any    `json:"after"`
	Changed []string `json:"changed"`
}

func decodeRowReport(t *testing.T, stdout string) rowReport {
	t.Helper()
	var doc rowReport
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("json: %v\n%s", err, stdout)
	}
	return doc
}

func vizRowsInOrder(doc rowReport) []vizImage {
	txns := append([]struct {
		PosStart       int64          `json:"pos_start"`
		MysqlbinlogCmd string         `json:"mysqlbinlog_cmd"`
		Operations     map[string]int `json:"operations"`
		RowsOmitted    int            `json:"rows_omitted"`
		Rows           []vizImage     `json:"rows"`
	}{}, doc.Transactions...)
	for i := 1; i < len(txns); i++ {
		j := i
		for j > 0 && txns[j].PosStart < txns[j-1].PosStart {
			txns[j], txns[j-1] = txns[j-1], txns[j]
			j--
		}
	}
	var out []vizImage
	for _, txn := range txns {
		out = append(out, txn.Rows...)
	}
	return out
}

type mbImage struct {
	op     string
	before []string
	after  []string
}

func parseMysqlbinlogRows(text string) []mbImage {
	var images []mbImage
	var cur *mbImage
	section := ""
	flush := func() {
		if cur != nil {
			images = append(images, *cur)
			cur = nil
		}
	}
	for _, line := range strings.Split(text, "\n") {
		switch {
		case strings.HasPrefix(line, "### INSERT INTO"):
			flush()
			cur = &mbImage{op: "INSERT"}
			section = "after"
		case strings.HasPrefix(line, "### UPDATE "):
			flush()
			cur = &mbImage{op: "UPDATE"}
			section = "before"
		case strings.HasPrefix(line, "### DELETE FROM"):
			flush()
			cur = &mbImage{op: "DELETE"}
			section = "before"
		case strings.HasPrefix(line, "### WHERE"):
			section = "before"
		case strings.HasPrefix(line, "### SET"):
			section = "after"
		case strings.HasPrefix(line, "###   @") && cur != nil:
			eq := strings.Index(line, "=")
			if eq < 0 {
				continue
			}
			value := line[eq+1:]
			if section == "before" {
				cur.before = append(cur.before, value)
			} else {
				cur.after = append(cur.after, value)
			}
		}
	}
	flush()
	return images
}

func matchRowImages(t *testing.T, viz []vizImage, mb []mbImage, wantSkips int) {
	t.Helper()
	mi := 0
	skips := 0
	for vi, image := range viz {
		found := false
		for mi < len(mb) {
			if mb[mi].op == image.Op && rowCellsMatch(image, mb[mi]) {
				found = true
				mi++
				break
			}
			if vi == 0 && skips == 0 {
				t.Fatalf("first row did not match mysqlbinlog\nviz=%+v\nmb=%+v", image, mb[mi])
			}
			skips++
			mi++
		}
		if !found {
			t.Fatalf("viz row %d op %s not found in mysqlbinlog", vi, image.Op)
		}
	}
	skips += len(mb) - mi
	if skips != wantSkips {
		t.Fatalf("skipped %d mysqlbinlog rows, rows_omitted %d", skips, wantSkips)
	}
}

func rowCellsMatch(viz vizImage, mb mbImage) bool {
	if viz.Op == "DELETE" {
		return cellListMatch(viz.Before, mb.before)
	}
	if viz.Op == "UPDATE" {
		return cellListMatch(viz.Before, mb.before) && cellListMatch(viz.After, mb.after)
	}
	return cellListMatch(viz.After, mb.after)
}

func cellListMatch(viz []any, raw []string) bool {
	if len(viz) != len(raw) {
		return false
	}
	for i := range viz {
		if !cellMatch(viz[i], raw[i]) {
			return false
		}
	}
	return true
}

func cellMatch(viz any, raw string) bool {
	if raw == "NULL" {
		return viz == nil
	}
	text, ok := viz.(string)
	if !ok || viz == nil {
		return false
	}
	if strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'") && len(raw) >= 2 {
		decoded := unescapeMysqlbinlog(raw[1 : len(raw)-1])
		if strings.HasPrefix(text, "0x") {
			return blobMatch(text, []byte(decoded))
		}
		if strings.HasPrefix(strings.TrimSpace(decoded), "{") || strings.HasPrefix(strings.TrimSpace(decoded), "[") {
			return jsonEqual(text, decoded)
		}
		return text == decoded
	}
	if left, right, ok := splitSignedPair(raw); ok {
		return text == raw || text == left || text == right
	}
	if converted, ok := unixFractionalToUTC(raw); ok && (text == converted || text == raw) {
		return true
	}
	return text == raw
}

func splitSignedPair(raw string) (string, string, bool) {
	open := strings.Index(raw, " (")
	if open < 0 || !strings.HasSuffix(raw, ")") {
		return "", "", false
	}
	return raw[:open], raw[open+2 : len(raw)-1], true
}

func unixFractionalToUTC(raw string) (string, bool) {
	secText, frac, ok := strings.Cut(raw, ".")
	if !ok || len(secText) < 10 {
		return "", false
	}
	sec, err := strconv.ParseInt(secText, 10, 64)
	if err != nil {
		return "", false
	}
	for len(frac) < 9 {
		frac += "0"
	}
	nsec, err := strconv.ParseInt(frac[:9], 10, 64)
	if err != nil {
		return "", false
	}
	return time.Unix(sec, nsec).UTC().Format("2006-01-02 15:04:05.000000"), true
}

func unescapeMysqlbinlog(value string) string {
	var buf bytes.Buffer
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' || i+1 >= len(value) {
			buf.WriteByte(value[i])
			continue
		}
		i++
		switch value[i] {
		case '\\', '\'', '"':
			buf.WriteByte(value[i])
		case 'n':
			buf.WriteByte('\n')
		case 'r':
			buf.WriteByte('\r')
		case 't':
			buf.WriteByte('\t')
		case '0':
			buf.WriteByte(0)
		case 'x':
			if i+2 >= len(value) {
				buf.WriteByte('x')
				continue
			}
			decoded, err := hex.DecodeString(value[i+1 : i+3])
			if err != nil {
				buf.WriteByte('x')
				continue
			}
			buf.Write(decoded)
			i += 2
		default:
			buf.WriteByte(value[i])
		}
	}
	return buf.String()
}

func blobMatch(viz string, raw []byte) bool {
	hexPart := strings.TrimPrefix(viz, "0x")
	if i := strings.IndexAny(hexPart, " …"); i >= 0 {
		hexPart = hexPart[:i]
	}
	decoded, err := hex.DecodeString(hexPart)
	if err != nil {
		return false
	}
	if strings.Contains(viz, "truncated") {
		return bytes.HasPrefix(raw, decoded)
	}
	return bytes.Equal(decoded, raw)
}

func jsonEqual(a, b string) bool {
	var left, right any
	if json.Unmarshal([]byte(a), &left) != nil || json.Unmarshal([]byte(b), &right) != nil {
		return false
	}
	lb, _ := json.Marshal(left)
	rb, _ := json.Marshal(right)
	return string(lb) == string(rb)
}

func assertKnownCells(t *testing.T, rows []vizImage, full bool) {
	t.Helper()
	var sawPrice, sawNote, sawUnsigned, sawChanged bool
	for _, row := range rows {
		if full {
			if row.Names != "full" || !strings.Contains(strings.Join(row.Columns, ","), "id,qty,price,note") {
				t.Fatalf("FULL columns = %v names %s", row.Columns, row.Names)
			}
		} else if row.Names != "positional" || row.Columns[0] != "@1" {
			t.Fatalf("MINIMAL columns = %v names %s", row.Columns, row.Names)
		}
		cells := append(append([]any{}, row.Before...), row.After...)
		for _, cell := range cells {
			text, _ := cell.(string)
			if text == "19.99" {
				sawPrice = true
			}
			if text == `it's "bad"` {
				sawNote = true
			}
			if text == "3000000000" || strings.Contains(text, "3000000000") {
				sawUnsigned = true
			}
		}
		for _, changed := range row.Changed {
			if changed == "note" || changed == "@4" {
				sawChanged = true
			}
		}
	}
	if !sawPrice || !sawNote || !sawUnsigned || !sawChanged {
		t.Fatalf("missing known cells price=%v note=%v unsigned=%v changed=%v", sawPrice, sawNote, sawUnsigned, sawChanged)
	}
}

// TestShowRowsTimestampIsUTCInEveryZone re-execs under a real process TZ.
// time.Local is fixed at startup, so setting it in-process is not the same
// check CI needs: runners are UTC, and the bug follows that zone.
func TestShowRowsTimestampIsUTCInEveryZone(t *testing.T) {
	if zone := os.Getenv("BINLOGVIZ_TIMESTAMP_TZ"); zone != "" {
		assertShowRowsTimestampIsUTC(t, zone)
		return
	}
	for _, zone := range []string{"UTC", "Asia/Shanghai", "America/New_York"} {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run", "^TestShowRowsTimestampIsUTCInEveryZone$", "-test.count=1", "-test.timeout", "120s")
			cmd.Env = envWithTZ(zone)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("TZ=%s: %v\n%s", zone, err, out)
			}
		})
	}
}

func assertShowRowsTimestampIsUTC(t *testing.T, zone string) {
	t.Helper()
	forceEnglishRuntimeOutput(t)
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Fatalf("load %s: %v", zone, err)
	}
	if time.Local.String() != loc.String() {
		t.Fatalf("time.Local=%s, want %s", time.Local, loc)
	}
	const layout = "2006-01-02 15:04:05.000000"
	utcFrac := time.Unix(1791295501, 500000000).UTC().Format(layout)
	localFrac := time.Unix(1791295501, 500000000).Format(layout)
	utcZero := time.Unix(1791295201, 0).UTC().Format(layout)
	localZero := time.Unix(1791295201, 0).Format(layout)
	const datetime = "2026-10-06 14:05:01.123456"
	if utcFrac != "2026-10-06 14:05:01.500000" || utcZero != "2026-10-06 14:00:01.000000" {
		t.Fatalf("fixture instants drifted: frac %s zero %s", utcFrac, utcZero)
	}

	cases := []struct {
		fixture string
		tsCol   string
		dtCol   string
	}{
		{fixture: "mysql-8.0.46-dml-full.binlog", tsCol: "updated_at", dtCol: "created_at"},
		{fixture: "mysql-8.0.46-dml-minimal.binlog", tsCol: "@6", dtCol: "@5"},
	}
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			path := mustFixturePath(t, tc.fixture)
			stdout, stderr, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--format", "json", "--top-transactions", "0")
			if err != nil {
				t.Fatalf("json: %v\n%s", err, stderr)
			}
			images := vizRowsInOrder(decodeRowReport(t, stdout))
			requireColumnText(t, images, tc.tsCol, utcFrac)
			requireColumnText(t, images, tc.tsCol, utcZero)
			requireColumnText(t, images, tc.dtCol, datetime)
			requireColumnText(t, images, tc.dtCol, utcZero)
			if localFrac != utcFrac {
				forbidColumnText(t, images, tc.tsCol, localFrac)
				forbidColumnText(t, images, tc.tsCol, localZero)
				parsedDT, err := time.ParseInLocation(layout, datetime, time.UTC)
				if err != nil {
					t.Fatal(err)
				}
				forbidColumnText(t, images, tc.dtCol, parsedDT.In(time.Local).Format(layout))
			}

			for _, format := range []string{"text", "markdown"} {
				out, stderr, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--format", format, "--top-transactions", "0")
				if err != nil {
					t.Fatalf("%s: %v\n%s", format, err, stderr)
				}
				if !strings.Contains(out, utcFrac) || !strings.Contains(out, datetime) {
					t.Fatalf("%s missing UTC TIMESTAMP %q or DATETIME %q (output %d bytes)", format, utcFrac, datetime, len(out))
				}
				if localFrac != utcFrac && (strings.Contains(out, localFrac) || strings.Contains(out, localZero)) {
					t.Fatalf("%s printed TIMESTAMP in %s (%s / %s)", format, zone, localFrac, localZero)
				}
			}
			// HTML row images are the largest/longest/widest transactions. The
			// fractional TIMESTAMP lives on the small DELETE, so check that
			// image with --dml delete, and the opening INSERT on the full report.
			html, stderr, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--format", "html", "--output", "-", "--top-transactions", "0")
			if err != nil {
				t.Fatalf("html: %v\n%s", err, stderr)
			}
			if !strings.Contains(html, utcZero) {
				t.Fatalf("html missing UTC TIMESTAMP %q (output %d bytes)", utcZero, len(html))
			}
			if localZero != utcZero && strings.Contains(html, localZero) {
				t.Fatalf("html printed TIMESTAMP in %s: %s", zone, localZero)
			}
			htmlDelete, stderr, err := executeAnalyzeLikeMain(t, path, "--show-rows", "--dml", "delete", "--format", "html", "--output", "-")
			if err != nil {
				t.Fatalf("html delete: %v\n%s", err, stderr)
			}
			if !strings.Contains(htmlDelete, utcFrac) || !strings.Contains(htmlDelete, datetime) {
				t.Fatalf("html delete missing UTC TIMESTAMP %q or DATETIME %q (output %d bytes)", utcFrac, datetime, len(htmlDelete))
			}
			if localFrac != utcFrac && strings.Contains(htmlDelete, localFrac) {
				t.Fatalf("html delete printed TIMESTAMP in %s: %s", zone, localFrac)
			}
		})
	}
}

func envWithTZ(zone string) []string {
	env := make([]string, 0, len(os.Environ())+2)
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "TZ=") || strings.HasPrefix(entry, "BINLOGVIZ_TIMESTAMP_TZ=") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "TZ="+zone, "BINLOGVIZ_TIMESTAMP_TZ="+zone)
}

func requireColumnText(t *testing.T, images []vizImage, column, want string) {
	t.Helper()
	if columnHasText(images, column, want) {
		return
	}
	t.Fatalf("column %s missing %q", column, want)
}

func forbidColumnText(t *testing.T, images []vizImage, column, bad string) {
	t.Helper()
	if columnHasText(images, column, bad) {
		t.Fatalf("column %s followed the process zone: %s", column, bad)
	}
}

func columnHasText(images []vizImage, column, want string) bool {
	for _, image := range images {
		idx := -1
		for i, name := range image.Columns {
			if name == column {
				idx = i
				break
			}
		}
		if idx < 0 {
			continue
		}
		for _, side := range [][]any{image.Before, image.After} {
			if idx < len(side) {
				if text, ok := side[idx].(string); ok && text == want {
					return true
				}
			}
		}
	}
	return false
}
