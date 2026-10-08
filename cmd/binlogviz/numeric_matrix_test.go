// Package binlogviz proves numeric cells against a live MySQL 8.0 server.
// input: BINLOGVIZ_FLASHBACK_E2E=1 and a local MySQL 8.0 ROW/GTID/FULL server.
// output: failure when analyze or flashback disagrees with the value MySQL holds.
// pos: command-layer oracle for every integer and numeric type, including compressed transactions.
// note: if this file changes, update this header and README.md.
package binlogviz

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestNumericDecodeMySQL80 is the numeric decode oracle.
// It builds every integer and numeric type, several column orders, and the
// boundary values, then checks analyze --show-rows and a flashback checksum
// round trip under strict and non-strict sql_mode.
func TestNumericDecodeMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") != "1" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to run the MySQL 8.0 numeric matrix")
	}
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}
	forceEnglishRuntimeOutput(t)

	tables := numericTables()
	numericMySQL(t, "RESET MASTER")
	numericMySQL(t, "DROP DATABASE IF EXISTS numx; CREATE DATABASE numx")
	var create, insert bytes.Buffer
	for _, tbl := range tables {
		create.WriteString(tbl.create)
		insert.WriteString(tbl.insert)
	}
	numericMySQL(t, create.String())
	numericMySQL(t, "FLUSH LOGS")
	numericMySQL(t, insert.String())
	dataBinlog := numericClosedBinlog(t)
	if !binlogHasEventType(t, dataBinlog, transactionPayloadEventType) {
		t.Fatal("data binlog has no TRANSACTION_PAYLOAD_EVENT; compressed inserts were not recorded")
	}

	stdout, stderr, err := executeAnalyzeLikeMain(t, dataBinlog,
		"--format", "json", "--show-rows",
		"--top-transactions", "0", "--top-rows", "0",
		"--include-schema", "numx")
	if err != nil {
		t.Fatalf("analyze json: %v\n%s", err, stderr)
	}
	var report numericReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("analyze json: %v\n%s", err, stdout[:min(500, len(stdout))])
	}
	if report.TransactionsOmitted != 0 {
		t.Fatalf("transactions_omitted=%d", report.TransactionsOmitted)
	}
	viz := numericViz(t, report)
	for _, txn := range report.Transactions {
		if txn.RowsOmitted != 0 {
			t.Fatalf("%s rows_omitted=%d", txn.GTID, txn.RowsOmitted)
		}
	}
	floatInexact := numericCompare(t, tables, viz)
	numericCrossCheck(t, dataBinlog, tables, viz)

	text, stderr, err := executeAnalyzeLikeMain(t, dataBinlog,
		"--format", "text", "--show-rows",
		"--top-transactions", "0", "--top-rows", "0",
		"--include-schema", "numx")
	if err != nil {
		t.Fatalf("analyze text: %v\n%s", err, stderr)
	}
	numericBanSignExtend(t, "analyze text", text)
	if !strings.Contains(text, "mi_u=9000000") || !strings.Contains(text, "mi_u=16777215") || !strings.Contains(text, "mi_u=8388608") {
		t.Fatalf("analyze text missing a corrected MEDIUMINT UNSIGNED boundary:\n%s", numericSnippet(text, "mi_u="))
	}

	exact, bits, floats := numericGroups(tables)
	beforeExact := numericChecksum(t, exact)
	beforeBits := numericChecksum(t, bits)
	beforeFloat := numericChecksum(t, floats)

	var exactIncident, bitIncident, floatIncident strings.Builder
	for _, tbl := range tables {
		switch {
		case tbl.group == "bit":
			bitIncident.WriteString(tbl.incident)
		case tbl.group == "float":
			floatIncident.WriteString(tbl.incident)
		default:
			exactIncident.WriteString(tbl.incident)
		}
	}
	numericMySQL(t, exactIncident.String())
	exactBinlog := numericClosedBinlog(t)
	if !binlogHasEventType(t, exactBinlog, transactionPayloadEventType) {
		t.Fatal("incident binlog has no TRANSACTION_PAYLOAD_EVENT")
	}
	numericRoundTrip(t, exactBinlog, exactIncident.String(), exact, beforeExact, true)
	numericHotRows(t, exactBinlog)

	// The exact round trip writes its own binlog events. Rotate those away
	// so the BIT file is only the BIT incident.
	numericMySQL(t, "FLUSH LOGS")
	numericMySQL(t, bitIncident.String())
	bitBinlog := numericClosedBinlog(t)
	numericRoundTrip(t, bitBinlog, bitIncident.String(), bits, beforeBits, false)

	numericMySQL(t, "FLUSH LOGS")
	numericMySQL(t, floatIncident.String())
	floatBinlog := numericClosedBinlog(t)
	if floatInexact {
		sql, stderr, err := executeFlashbackLikeMain(t, floatBinlog, "--include-schema", "numx")
		if err == nil {
			t.Fatal("FLOAT/DOUBLE was not exact in analyze, but flashback printed SQL")
		}
		if sql != "" {
			t.Fatalf("inexact float flashback printed SQL:\n%s", sql)
		}
		if !strings.Contains(stderr, "FLOAT") && !strings.Contains(stderr, "DOUBLE") {
			t.Fatalf("float refusal does not name the type:\n%s", stderr)
		}
	} else {
		numericRoundTrip(t, floatBinlog, floatIncident.String(), floats, beforeFloat, false)
	}
}

type numericKind int

const (
	numInt numericKind = iota
	numDec
	numYear
	numBit
	numFloat
	numDouble
	numText
	numSkip
)

type numericCol struct {
	name     string
	ddl      string
	kind     numericKind
	bits     int
	prec     int
	scale    int
	unsigned bool
}

type numericTable struct {
	name     string
	key      string
	group    string // exact, bit, float
	compress bool
	create   string
	insert   string
	incident string
	cols     []numericCol
}

type numericReport struct {
	Transactions []struct {
		GTID        string `json:"gtid"`
		RowsOmitted int    `json:"rows_omitted"`
		Rows        []struct {
			Schema  string   `json:"schema"`
			Table   string   `json:"table"`
			Op      string   `json:"op"`
			Columns []string `json:"columns"`
			After   []any    `json:"after"`
		} `json:"rows"`
	} `json:"transactions"`
	TransactionsOmitted int `json:"transactions_omitted"`
	HotRows             []struct {
		Schema     string `json:"schema"`
		Table      string `json:"table"`
		PrimaryKey string `json:"primary_key"`
		Touches    int    `json:"touches"`
	} `json:"hot_rows"`
}

func numericTables() []numericTable {
	exact := []numericCol{
		intCol("ti", "TINYINT", 8, false),
		intCol("ti_u", "TINYINT UNSIGNED", 8, true),
		intCol("ti_z", "TINYINT ZEROFILL", 8, true),
		intCol("si", "SMALLINT", 16, false),
		intCol("si_u", "SMALLINT UNSIGNED", 16, true),
		intCol("si_z", "SMALLINT ZEROFILL", 16, true),
		intCol("mi", "MEDIUMINT", 24, false),
		intCol("mi_u", "MEDIUMINT UNSIGNED", 24, true),
		intCol("mi_z", "MEDIUMINT ZEROFILL", 24, true),
		intCol("i", "INT", 32, false),
		intCol("i_u", "INT UNSIGNED", 32, true),
		intCol("i_z", "INT ZEROFILL", 32, true),
		intCol("bi", "BIGINT", 64, false),
		intCol("bi_u", "BIGINT UNSIGNED", 64, true),
		intCol("bi_z", "BIGINT ZEROFILL", 64, true),
		decCol("d10", "DECIMAL(10,0)", 10, 0, false),
		decCol("d10u", "DECIMAL(10,0) UNSIGNED", 10, 0, true),
		decCol("d102", "DECIMAL(10,2)", 10, 2, false),
		decCol("d102u", "DECIMAL(10,2) UNSIGNED", 10, 2, true),
		decCol("d184", "DECIMAL(18,4)", 18, 4, false),
		decCol("d65", "DECIMAL(65,0)", 65, 0, false),
		decCol("d65u", "DECIMAL(65,0) UNSIGNED", 65, 0, true),
		decCol("d6530", "DECIMAL(65,30)", 65, 30, false),
		decCol("d11", "DECIMAL(1,0)", 1, 0, false),
		decCol("d310", "DECIMAL(31,10) UNSIGNED", 31, 10, true),
		decCol("dz", "DECIMAL(10,2) ZEROFILL", 10, 2, true),
		{name: "y", ddl: "YEAR NULL", kind: numYear},
	}
	yearP := numericCol{name: "py", ddl: "YEAR NULL", kind: numYear}
	enumP := numericCol{name: "pe", ddl: "ENUM('a','b') NULL", kind: numSkip}
	setP := numericCol{name: "ps", ddl: "SET('a','b','c') NULL", kind: numSkip}
	id := numericCol{name: "id", ddl: "INT NOT NULL", kind: numInt, bits: 32}
	note := numericCol{name: "note", ddl: "VARCHAR(32) NULL", kind: numText}

	var bits []numericCol
	for n := 1; n <= 64; n++ {
		bits = append(bits, numericCol{
			name: fmt.Sprintf("b%d", n), ddl: fmt.Sprintf("BIT(%d) NULL", n),
			kind: numBit, bits: n, unsigned: true,
		})
	}
	bitPrefix := []numericCol{
		bitN(1), bitN(8), bitN(24), bitN(32), bitN(63), bitN(64),
		intCol("mi_u", "MEDIUMINT UNSIGNED", 24, true),
	}
	floats := []numericCol{
		{name: "f", ddl: "FLOAT NULL", kind: numFloat},
		{name: "fu", ddl: "FLOAT UNSIGNED NULL", kind: numFloat, unsigned: true},
		{name: "fz", ddl: "FLOAT ZEROFILL NULL", kind: numFloat, unsigned: true},
		{name: "d", ddl: "DOUBLE NULL", kind: numDouble},
		{name: "du", ddl: "DOUBLE UNSIGNED NULL", kind: numDouble, unsigned: true},
		{name: "dz", ddl: "DOUBLE ZEROFILL NULL", kind: numDouble, unsigned: true},
	}

	rev := append([]numericCol(nil), exact...)
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	layouts := []struct {
		name, group  string
		pk, compress bool
		prefix, cols []numericCol
	}{
		{"wide", "exact", true, false, nil, exact},
		{"py", "exact", true, false, []numericCol{yearP}, exact},
		{"pe", "exact", true, false, []numericCol{enumP}, exact},
		{"ps", "exact", true, false, []numericCol{setP}, exact},
		{"yes", "exact", true, true, []numericCol{yearP, enumP, setP}, rev},
		{"nopk", "exact", false, false, nil, exact},
		{"bits", "bit", true, false, nil, bits},
		{"bitsyes", "bit", true, true, []numericCol{yearP, enumP, setP}, bitPrefix},
		{"fl", "float", true, false, nil, floats},
		{"flyes", "float", true, true, []numericCol{yearP, enumP, setP}, floats},
	}
	var out []numericTable
	for _, layout := range layouts {
		cols := append([]numericCol{id, note}, append(append([]numericCol{}, layout.prefix...), layout.cols...)...)
		out = append(out, newNumericTable(layout.name, "id", layout.group, layout.pk, layout.compress, cols))
	}
	out = append(out, numericHandTables()...)
	return out
}

func intCol(name, ddl string, bits int, unsigned bool) numericCol {
	return numericCol{name: name, ddl: ddl + " NULL", kind: numInt, bits: bits, unsigned: unsigned}
}

func decCol(name, ddl string, prec, scale int, unsigned bool) numericCol {
	return numericCol{name: name, ddl: ddl + " NULL", kind: numDec, prec: prec, scale: scale, unsigned: unsigned}
}

func bitN(n int) numericCol {
	return numericCol{name: fmt.Sprintf("b%d", n), ddl: fmt.Sprintf("BIT(%d) NULL", n), kind: numBit, bits: n, unsigned: true}
}

func newNumericTable(name, key, group string, pk, compress bool, cols []numericCol) numericTable {
	defs := make([]string, len(cols))
	names := make([]string, len(cols))
	for i, col := range cols {
		defs[i] = "`" + col.name + "` " + col.ddl
		names[i] = "`" + col.name + "`"
	}
	body := strings.Join(defs, ",\n  ")
	if pk {
		body += ",\n  PRIMARY KEY (`" + key + "`)"
	}
	create := "CREATE TABLE numx.`" + name + "` (\n  " + body + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
	var values []string
	for i, slot := range numericSlots {
		one := make([]string, len(cols))
		for c, col := range cols {
			one[c] = numericLiteral(col, slot, i)
		}
		values = append(values, "("+strings.Join(one, ", ")+")")
	}
	insert := "INSERT INTO numx.`" + name + "` (" + strings.Join(names, ", ") + ") VALUES\n" + strings.Join(values, ",\n") + ";\n"
	if compress {
		insert = "SET SESSION binlog_transaction_compression=ON;\nSTART TRANSACTION;\n" + insert + "COMMIT;\nSET SESSION binlog_transaction_compression=OFF;\n"
	}
	junk := make([]string, len(cols))
	for c, col := range cols {
		junk[c] = numericJunk(col)
	}
	incident := "START TRANSACTION;\n" +
		"UPDATE numx.`" + name + "` SET `note`='oops';\n" +
		"DELETE FROM numx.`" + name + "` WHERE `id`=1;\n" +
		"INSERT INTO numx.`" + name + "` (" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(junk, ", ") + ");\n" +
		"COMMIT;\n"
	if compress {
		incident = "SET SESSION binlog_transaction_compression=ON;\n" + incident + "SET SESSION binlog_transaction_compression=OFF;\n"
	}
	return numericTable{
		name: name, key: key, group: group, compress: compress,
		create: create, insert: insert, incident: incident, cols: cols,
	}
}

func numericHandTables() []numericTable {
	mipk := numericTable{
		name: "mipk", key: "id", group: "exact", compress: true,
		cols: []numericCol{
			{name: "id", ddl: "MEDIUMINT UNSIGNED NOT NULL", kind: numInt, bits: 24, unsigned: true},
			{name: "note", kind: numText},
		},
		create: "CREATE TABLE numx.mipk (id MEDIUMINT UNSIGNED NOT NULL PRIMARY KEY, note VARCHAR(20)) ENGINE=InnoDB;\n",
		insert: `SET SESSION binlog_transaction_compression=ON;
START TRANSACTION;
INSERT INTO numx.mipk (id, note) VALUES
(0, 'z'), (1, 'one'), (5, 'small'), (8388607, 'powm1'), (8388608, 'pow'),
(9000000, 'original'), (16777214, 'max1'), (16777215, 'max');
COMMIT;
SET SESSION binlog_transaction_compression=OFF;
`,
		incident: `SET SESSION binlog_transaction_compression=ON;
START TRANSACTION;
UPDATE numx.mipk SET note='oops' WHERE id=9000000;
UPDATE numx.mipk SET note='oops2' WHERE id=9000000;
UPDATE numx.mipk SET note='oops3' WHERE id=9000000;
UPDATE numx.mipk SET note='x' WHERE id=8388608;
UPDATE numx.mipk SET note='y' WHERE id=16777215;
DELETE FROM numx.mipk WHERE id=5;
INSERT INTO numx.mipk (id, note) VALUES (2, 'junk');
COMMIT;
SET SESSION binlog_transaction_compression=OFF;
`,
	}
	minopk := numericTable{
		name: "minopk", key: "m", group: "exact",
		cols: []numericCol{
			{name: "m", kind: numInt, bits: 24, unsigned: true},
			{name: "note", kind: numText},
		},
		create: "CREATE TABLE numx.minopk (m MEDIUMINT UNSIGNED, note VARCHAR(20)) ENGINE=InnoDB;\n",
		insert: `INSERT INTO numx.minopk (m, note) VALUES
(0, 'z'), (1, 'one'), (7, 'keep7'), (8388608, 'pow'), (9000000, 'nine'),
(12000000, 'keep'), (16777214, 'max1'), (16777215, 'max');
`,
		incident: `SET SESSION binlog_transaction_compression=ON;
START TRANSACTION;
UPDATE numx.minopk SET note='oops' WHERE m=12000000;
DELETE FROM numx.minopk WHERE m=7;
INSERT INTO numx.minopk (m, note) VALUES (3, 'new');
COMMIT;
SET SESSION binlog_transaction_compression=OFF;
`,
	}
	return []numericTable{mipk, minopk}
}

var numericSlots = []string{"min", "min1", "neg1", "zero", "one", "powm1", "pow", "max1", "max", "null", "m9", "m12"}

func numericLiteral(col numericCol, slot string, row int) string {
	switch col.name {
	case "id":
		return fmt.Sprintf("%d", row+1)
	case "note":
		return "'" + slot + "'"
	case "py":
		return numericCycle([]string{"2026", "1901", "2155", "0", "NULL"}, row)
	case "pe":
		return numericCycle([]string{"'a'", "'b'", "NULL"}, row)
	case "ps":
		return numericCycle([]string{"'a'", "'a,b'", "''", "NULL"}, row)
	}
	switch col.kind {
	case numYear:
		return yearLiteral(slot)
	case numInt:
		return intLiteral(col.bits, col.unsigned, slot)
	case numDec:
		return decLiteral(col.prec, col.scale, col.unsigned, slot)
	case numBit:
		return intLiteral(col.bits, true, slot)
	case numFloat, numDouble:
		return floatLiteral(col.kind, col.unsigned, slot)
	default:
		return "NULL"
	}
}

func numericJunk(col numericCol) string {
	switch col.name {
	case "id":
		return "100"
	case "note":
		return "'junk'"
	case "py":
		return "2026"
	case "pe":
		return "'a'"
	case "ps":
		return "'a'"
	}
	switch col.kind {
	case numYear:
		return "2026"
	case numInt, numBit:
		if col.unsigned && col.bits == 24 {
			return "9000000"
		}
		return "0"
	default:
		return "0"
	}
}

func numericCycle(values []string, row int) string {
	return values[row%len(values)]
}

func yearLiteral(slot string) string {
	switch slot {
	case "min":
		return "1901"
	case "min1":
		return "1902"
	case "zero":
		return "0"
	case "max1":
		return "2154"
	case "max":
		return "2155"
	default:
		return "NULL"
	}
}

func intLiteral(bits int, unsigned bool, slot string) string {
	if slot == "null" || bits <= 0 || bits > 64 {
		return "NULL"
	}
	maxV := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(bits)), big.NewInt(1))
	minV := big.NewInt(0)
	if !unsigned {
		maxV = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(bits-1)), big.NewInt(1))
		minV = new(big.Int).Neg(new(big.Int).Add(new(big.Int).Set(maxV), big.NewInt(1)))
	}
	var v *big.Int
	switch slot {
	case "min":
		v = minV
	case "min1":
		v = new(big.Int).Add(minV, big.NewInt(1))
	case "neg1":
		v = big.NewInt(-1)
	case "zero":
		v = big.NewInt(0)
	case "one":
		v = big.NewInt(1)
	case "powm1":
		v = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(bits-1)), big.NewInt(1))
	case "pow":
		v = new(big.Int).Lsh(big.NewInt(1), uint(bits-1))
	case "max1":
		v = new(big.Int).Sub(maxV, big.NewInt(1))
	case "max":
		v = maxV
	case "m9":
		v = big.NewInt(9000000)
	case "m12":
		v = big.NewInt(12000000)
	default:
		return "NULL"
	}
	if v.Cmp(minV) < 0 || v.Cmp(maxV) > 0 {
		return "NULL"
	}
	return sqlBigInt(v)
}

func sqlBigInt(v *big.Int) string {
	min64 := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 63))
	if v.Cmp(min64) == 0 {
		return "(-9223372036854775807-1)"
	}
	max64 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 63), big.NewInt(1))
	if v.Cmp(max64) > 0 {
		return "CAST(" + v.String() + " AS UNSIGNED)"
	}
	return v.String()
}

func decLiteral(prec, scale int, unsigned bool, slot string) string {
	if slot == "null" {
		return "NULL"
	}
	maxUnits := new(big.Int).Sub(pow10(prec), big.NewInt(1))
	minUnits := big.NewInt(0)
	if !unsigned {
		minUnits = new(big.Int).Neg(new(big.Int).Set(maxUnits))
	}
	one := big.NewInt(1)
	if scale > 0 {
		one = pow10(scale)
	}
	var units *big.Int
	switch slot {
	case "min":
		units = minUnits
	case "min1":
		units = new(big.Int).Add(minUnits, big.NewInt(1))
	case "neg1":
		units = new(big.Int).Neg(new(big.Int).Set(one))
	case "zero":
		units = big.NewInt(0)
	case "one":
		units = one
	case "powm1":
		units = new(big.Int).Mul(big.NewInt(8388607), pow10(scale))
	case "pow":
		units = new(big.Int).Mul(big.NewInt(8388608), pow10(scale))
	case "max1":
		units = new(big.Int).Sub(maxUnits, big.NewInt(1))
	case "max":
		units = maxUnits
	case "m9":
		units = new(big.Int).Mul(big.NewInt(9000000), pow10(scale))
	case "m12":
		units = new(big.Int).Mul(big.NewInt(12000000), pow10(scale))
	default:
		return "NULL"
	}
	if units.Cmp(minUnits) < 0 || units.Cmp(maxUnits) > 0 {
		return "NULL"
	}
	return formatScaled(units, scale)
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func formatScaled(units *big.Int, scale int) string {
	neg := units.Sign() < 0
	abs := new(big.Int).Abs(units)
	digits := abs.String()
	var text string
	if scale == 0 {
		text = digits
	} else if len(digits) <= scale {
		text = "0." + strings.Repeat("0", scale-len(digits)) + digits
	} else {
		text = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if neg {
		return "-" + text
	}
	return text
}

func floatLiteral(kind numericKind, unsigned bool, slot string) string {
	if slot == "null" {
		return "NULL"
	}
	if unsigned && (slot == "min" || slot == "min1" || slot == "neg1" || slot == "m12") {
		return "NULL"
	}
	body := ""
	neg := false
	switch slot {
	case "min":
		neg = true
		if kind == numFloat {
			body = "3.402823466e+38"
		} else {
			body = "1.7976931348623157e+308"
		}
	case "max":
		if kind == numFloat {
			body = "3.402823466e+38"
		} else {
			body = "1.7976931348623157e+308"
		}
	case "min1":
		neg = true
		if kind == numFloat {
			body = "1e38"
		} else {
			body = "1e308"
		}
	case "max1":
		if kind == numFloat {
			body = "1e38"
		} else {
			body = "1e308"
		}
	case "neg1":
		return castFloat(kind, "-1")
	case "zero":
		return "0"
	case "one":
		return "1"
	case "powm1":
		return "8388607"
	case "pow":
		return "8388608"
	case "m9":
		return castFloat(kind, "0.1")
	case "m12":
		return castFloat(kind, "-0.1")
	default:
		return "NULL"
	}
	if neg {
		body = "-" + body
	}
	return castFloat(kind, body)
}

func castFloat(kind numericKind, body string) string {
	if kind == numFloat {
		return "CAST('" + body + "' AS FLOAT)"
	}
	return "CAST('" + body + "' AS DOUBLE)"
}

func numericGroups(tables []numericTable) (exact, bits, floats []string) {
	for _, tbl := range tables {
		switch tbl.group {
		case "bit":
			bits = append(bits, tbl.name)
		case "float":
			floats = append(floats, tbl.name)
		default:
			exact = append(exact, tbl.name)
		}
	}
	return exact, bits, floats
}

func numericChecksum(t *testing.T, names []string) string {
	t.Helper()
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = "numx.`" + name + "`"
	}
	return numericMySQL(t, "CHECKSUM TABLE "+strings.Join(quoted, ", "))
}

func numericRoundTrip(t *testing.T, binlog, incident string, names []string, before string, bounds bool) {
	t.Helper()
	sql, stderr, err := executeFlashbackLikeMain(t, binlog, "--include-schema", "numx")
	if err != nil {
		t.Fatalf("flashback %s: %v\n%s", names[0], err, stderr)
	}
	if sql == "" {
		t.Fatalf("flashback %s printed no SQL", names[0])
	}
	numericBanSignExtend(t, "flashback", sql)
	if bounds {
		for _, good := range []string{"9000000", "8388608", "16777215", "12000000"} {
			if !strings.Contains(sql, good) {
				t.Fatalf("flashback SQL missing %s", good)
			}
		}
	}
	numericMySQL(t, sql)
	if got := numericChecksum(t, names); got != before {
		t.Fatalf("strict checksum\nbefore:\n%s\nrestored:\n%s", before, got)
	}
	numericMySQL(t, incident)
	numericMySQL(t, "SET SESSION sql_mode='';\n"+sql)
	if got := numericChecksum(t, names); got != before {
		t.Fatalf("non-strict checksum\nbefore:\n%s\nrestored:\n%s", before, got)
	}
}

func numericHotRows(t *testing.T, binlog string) {
	t.Helper()
	stdout, stderr, err := executeAnalyzeLikeMain(t, binlog,
		"--format", "json", "--top-transactions", "0", "--top-rows", "0",
		"--include-schema", "numx")
	if err != nil {
		t.Fatalf("hot rows: %v\n%s", err, stderr)
	}
	numericBanSignExtend(t, "hot-row json", stdout)
	var report numericReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, row := range report.HotRows {
		if row.Schema == "numx" && row.Table == "mipk" && row.PrimaryKey == "id=9000000" && row.Touches >= 3 {
			found = true
		}
		if strings.Contains(row.PrimaryKey, "4287190080") || strings.Contains(row.PrimaryKey, "4286578688") {
			t.Fatalf("hot row key %s", row.PrimaryKey)
		}
	}
	if !found {
		t.Fatalf("hot rows missing numx.mipk id=9000000 x3: %+v", report.HotRows)
	}
	text, stderr, err := executeAnalyzeLikeMain(t, binlog,
		"--format", "text", "--top-transactions", "0", "--top-rows", "0",
		"--include-schema", "numx")
	if err != nil {
		t.Fatalf("hot rows text: %v\n%s", err, stderr)
	}
	if !strings.Contains(text, "numx.mipk id=9000000") {
		t.Fatalf("hot row text missing id=9000000:\n%s", numericSnippet(text, "mipk"))
	}
	numericBanSignExtend(t, "hot-row text", text)
}

func numericBanSignExtend(t *testing.T, where, body string) {
	t.Helper()
	for _, bad := range []string{"4287190080", "4286578688", "4290190080"} {
		if strings.Contains(body, bad) {
			t.Fatalf("%s still has sign-extended MEDIUMINT %s\n%s", where, bad, numericSnippet(body, bad))
		}
	}
}

func numericSnippet(body, needle string) string {
	i := strings.Index(body, needle)
	if i < 0 {
		if len(body) > 400 {
			return body[:400]
		}
		return body
	}
	start := i - 80
	if start < 0 {
		start = 0
	}
	end := i + 120
	if end > len(body) {
		end = len(body)
	}
	return body[start:end]
}

// numericViz is table -> key -> column -> text. INSERT after-images only.
func numericViz(t *testing.T, report numericReport) map[string]map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]map[string]string{}
	for _, txn := range report.Transactions {
		for _, row := range txn.Rows {
			if row.Op != "INSERT" || row.Schema != "numx" {
				continue
			}
			cells := map[string]string{}
			for i, name := range row.Columns {
				cells[name] = numericCell(nil)
				if i < len(row.After) {
					cells[name] = numericCell(row.After[i])
				}
			}
			key := cells["id"]
			if key == "" {
				key = cells["m"]
			}
			if key == "" {
				t.Fatalf("%s row has no key: %+v", row.Table, cells)
			}
			if out[row.Table] == nil {
				out[row.Table] = map[string]map[string]string{}
			}
			out[row.Table][key] = cells
		}
	}
	return out
}

func numericCell(v any) string {
	if v == nil {
		return "NULL"
	}
	switch typed := v.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func numericCompare(t *testing.T, tables []numericTable, viz map[string]map[string]map[string]string) bool {
	t.Helper()
	var bad []string
	var floats []string
	floatInexact := false
	for _, tbl := range tables {
		truth := numericTruth(t, tbl)
		got := viz[tbl.name]
		if got == nil {
			bad = append(bad, tbl.name+": no decoded rows")
			continue
		}
		for id, cols := range truth {
			row := got[id]
			if row == nil {
				bad = append(bad, fmt.Sprintf("%s id=%s: missing from analyze", tbl.name, id))
				continue
			}
			for _, col := range tbl.cols {
				if col.kind == numSkip {
					continue
				}
				want := cols[col.name]
				have := row[col.name]
				switch col.kind {
				case numFloat, numDouble:
					if numericMarker(have) {
						floatInexact = true
						continue
					}
					if have == "NULL" && want == "NULL" {
						continue
					}
					if ratsEqual(have, want) {
						continue
					}
					floats = append(floats, fmt.Sprintf("%s\t%s\t%s\t%s\t%s", tbl.name, id, col.name, floatCast(col.kind), have))
				case numText:
					if have != want {
						bad = append(bad, fmt.Sprintf("%s %s=%s %s: mysql %q viz %q", tbl.name, tbl.key, id, col.name, want, have))
					}
				default:
					if numericMarker(have) {
						bad = append(bad, fmt.Sprintf("%s %s=%s %s: marker %s for a value MySQL holds as %s", tbl.name, tbl.key, id, col.name, have, want))
						continue
					}
					if !ratsEqual(have, want) {
						bad = append(bad, fmt.Sprintf("%s %s=%s %s: mysql %q viz %q", tbl.name, tbl.key, id, col.name, want, have))
					}
				}
			}
		}
	}
	bad = append(bad, numericFloatEqual(t, floats)...)
	if len(bad) > 0 {
		if len(bad) > 30 {
			bad = append(bad[:30], fmt.Sprintf("... %d more", len(bad)-30))
		}
		t.Fatalf("numeric decode mismatch:\n%s", strings.Join(bad, "\n"))
	}
	return floatInexact
}

func numericTruth(t *testing.T, tbl numericTable) map[string]map[string]string {
	t.Helper()
	var exprs []string
	for _, col := range tbl.cols {
		if col.kind == numSkip {
			continue
		}
		exprs = append(exprs, numericSelectExpr(col))
	}
	query := "SELECT " + strings.Join(exprs, ", ") + " FROM numx.`" + tbl.name + "` ORDER BY `" + tbl.key + "`"
	raw := numericMySQL(t, query)
	out := map[string]map[string]string{}
	names := numericCheckedNames(tbl)
	for _, line := range strings.Split(strings.TrimRight(raw, "\n"), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != len(names) {
			t.Fatalf("%s select fields %d want %d: %q", tbl.name, len(fields), len(names), line)
		}
		row := map[string]string{}
		for i, name := range names {
			row[name] = fields[i]
		}
		out[row[tbl.key]] = row
	}
	return out
}

func numericCheckedNames(tbl numericTable) []string {
	var names []string
	for _, col := range tbl.cols {
		if col.kind == numSkip {
			continue
		}
		names = append(names, col.name)
	}
	return names
}

func numericSelectExpr(col numericCol) string {
	switch col.kind {
	case numBit:
		return fmt.Sprintf("IF(`%s` IS NULL,'NULL',CONV(HEX(`%s`),16,10))", col.name, col.name)
	case numText:
		return fmt.Sprintf("IF(`%s` IS NULL,'NULL',`%s`)", col.name, col.name)
	default:
		return fmt.Sprintf("IF(`%s` IS NULL,'NULL',CAST(`%s` AS CHAR))", col.name, col.name)
	}
}

func floatCast(kind numericKind) string {
	if kind == numFloat {
		return "FLOAT"
	}
	return "DOUBLE"
}

func numericMarker(text string) bool {
	return strings.HasPrefix(text, "<") && strings.HasSuffix(text, ">")
}

func ratsEqual(a, b string) bool {
	if a == "NULL" || b == "NULL" {
		return a == b
	}
	left, okL := new(big.Rat).SetString(a)
	right, okR := new(big.Rat).SetString(b)
	if !okL || !okR {
		return false
	}
	return left.Cmp(right) == 0
}

func numericFloatEqual(t *testing.T, rows []string) []string {
	t.Helper()
	if len(rows) == 0 {
		return nil
	}
	var parts []string
	for i, row := range rows {
		fields := strings.Split(row, "\t")
		table, id, col, cast, viz := fields[0], fields[1], fields[2], fields[3], fields[4]
		if strings.ContainsAny(viz, "'\\") {
			return []string{fmt.Sprintf("%s.%s %s viz %q is not a float literal", table, id, col, viz)}
		}
		parts = append(parts, fmt.Sprintf(
			"SELECT '%d', IF((SELECT `%s` FROM numx.`%s` WHERE `%s`=%s) <=> CAST('%s' AS %s), '1', '0')",
			i, col, table, numericKey(table), id, viz, cast))
	}
	raw := numericMySQL(t, strings.Join(parts, " UNION ALL "))
	var bad []string
	lines := strings.Split(strings.TrimRight(raw, "\n"), "\n")
	if len(lines) != len(rows) {
		return []string{fmt.Sprintf("float equality returned %d lines for %d checks:\n%s", len(lines), len(rows), raw)}
	}
	for i, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[1] != "1" {
			bad = append(bad, "float "+rows[i]+" mysql equality "+line)
		}
	}
	return bad
}

func numericKey(table string) string {
	if table == "minopk" {
		return "m"
	}
	return "id"
}

var (
	numericInsertRE = regexp.MustCompile("^### INSERT INTO `([^`]+)`\\.`([^`]+)`")
	numericFieldRE  = regexp.MustCompile(`^###\s+@(\d+)=(.*)$`)
)

func numericCrossCheck(t *testing.T, path string, tables []numericTable, viz map[string]map[string]map[string]string) {
	t.Helper()
	cmd := exec.Command("mysqlbinlog", "-vv", "--base64-output=DECODE-ROWS", path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mysqlbinlog: %v\n%s", err, out)
	}
	byName := map[string]numericTable{}
	for _, tbl := range tables {
		byName[tbl.name] = tbl
	}
	var bad []string
	var table string
	vals := map[int]string{}
	flush := func() {
		if table == "" || len(vals) == 0 {
			vals = map[int]string{}
			return
		}
		tbl, ok := byName[table]
		if !ok {
			vals = map[int]string{}
			return
		}
		got := map[string]string{}
		for i, col := range tbl.cols {
			text, ok := vals[i+1]
			if !ok {
				continue
			}
			got[col.name] = text
		}
		id := got[tbl.key]
		row := viz[table][id]
		if row == nil {
			bad = append(bad, fmt.Sprintf("mysqlbinlog %s %s=%s not in analyze", table, tbl.key, id))
			vals = map[int]string{}
			return
		}
		for _, col := range tbl.cols {
			if col.kind == numSkip || col.kind == numText || col.kind == numFloat || col.kind == numDouble {
				continue
			}
			want := got[col.name]
			have := row[col.name]
			if want == "" || numericMarker(have) {
				continue
			}
			// mysqlbinlog adds 1900 to a zero YEAR byte. MySQL stores 0000.
			if col.kind == numYear && have == "0" && (want == "1900" || want == "0000" || want == "0") {
				continue
			}
			if !ratsEqual(have, want) {
				bad = append(bad, fmt.Sprintf("mysqlbinlog %s %s=%s %s: binlog %q viz %q", table, tbl.key, id, col.name, want, have))
			}
		}
		vals = map[int]string{}
	}
	for _, line := range strings.Split(string(out), "\n") {
		if m := numericInsertRE.FindStringSubmatch(line); m != nil {
			flush()
			if m[1] == "numx" {
				table = m[2]
			} else {
				table = ""
			}
			continue
		}
		if m := numericFieldRE.FindStringSubmatch(line); m != nil && table != "" {
			n := 0
			fmt.Sscanf(m[1], "%d", &n)
			tbl := byName[table]
			var col numericCol
			if n >= 1 && n <= len(tbl.cols) {
				col = tbl.cols[n-1]
			}
			text, ok := numericBinlogHeld(m[2], col.unsigned && col.kind != numYear)
			if ok {
				vals[n] = text
			}
		}
	}
	flush()
	if len(bad) > 0 {
		if len(bad) > 20 {
			bad = append(bad[:20], fmt.Sprintf("... %d more", len(bad)-20))
		}
		t.Fatalf("mysqlbinlog cross-check:\n%s", strings.Join(bad, "\n"))
	}
}

func numericBinlogHeld(raw string, unsigned bool) (string, bool) {
	if i := strings.Index(raw, " /*"); i >= 0 {
		raw = raw[:i]
	}
	raw = strings.TrimSpace(raw)
	if raw == "NULL" || strings.HasPrefix(raw, "NULL") {
		return "NULL", true
	}
	if strings.HasPrefix(raw, "'") || strings.HasPrefix(raw, "b'") {
		return "", false
	}
	signed := raw
	wide := ""
	if i := strings.Index(raw, " ("); i >= 0 {
		signed = strings.TrimSpace(raw[:i])
		wide = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(raw[i+2:]), ")"))
	}
	if unsigned && wide != "" {
		return wide, true
	}
	return signed, true
}

func numericMySQL(t *testing.T, stdin string) string {
	t.Helper()
	base := strings.Fields(os.Getenv("BINLOGVIZ_MYSQL"))
	if len(base) == 0 {
		base = []string{"sudo", "mysql"}
	}
	args := append(append([]string{}, base[1:]...), "--default-character-set=utf8mb4", "-N", "--batch")
	cmd := exec.Command(base[0], args...)
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		snippet := stdin
		if len(snippet) > 1500 {
			snippet = snippet[:1500] + "\n..."
		}
		t.Fatalf("mysql: %v\nstderr:\n%s\nstdout:\n%s\nSQL:\n%s", err, stderr.String(), stdout.String(), snippet)
	}
	return stdout.String()
}

func numericClosedBinlog(t *testing.T) string {
	t.Helper()
	numericMySQL(t, "FLUSH LOGS")
	datadir := strings.TrimSpace(numericMySQL(t, "SELECT @@datadir"))
	names := binlogNames(t, numericMySQL(t, "SHOW BINARY LOGS"))
	name := names[len(names)-2]
	dst := filepath.Join(t.TempDir(), name)
	e2eCopy(t, strings.TrimRight(datadir, "/")+"/"+name, dst)
	return dst
}
