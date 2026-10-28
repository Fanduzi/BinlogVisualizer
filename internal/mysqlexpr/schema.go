// Package mysqlexpr checks a schema file against TABLE_MAP metadata.
// input: CREATE/ALTER text from --schema-file or the binlog, plus flashback rows that carry column metadata.
// output: an error that names the table (as the caller labels it) and the columns that differ. Generated values are checked later, against every logged image. Which definition applies to a table is decided by the schema catalog (internal/schemacatalog).
// pos: flashback-only helper. Analyze does not call it.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// SchemaCol is one column of a CREATE or ALTER definition, in table order.
type SchemaCol struct {
	Name      string
	Generated bool
	Expr      string
	base      string
	unsigned  bool
	charset   string
	members   []string
	prec      int
	scale     int
	hasPrec   bool
}

func CloneSchemaCols(cols []SchemaCol) []SchemaCol {
	if cols == nil {
		return nil
	}
	out := make([]SchemaCol, len(cols))
	for i, col := range cols {
		out[i] = col
		if col.members != nil {
			out[i].members = append([]string(nil), col.members...)
		}
	}
	return out
}

func GeneratedNameSet(cols []SchemaCol) map[string]struct{} {
	out := map[string]struct{}{}
	for _, col := range cols {
		if col.Generated {
			out[strings.ToLower(col.Name)] = struct{}{}
		}
	}
	return out
}

func ParseSchemaColumns(body string) ([]SchemaCol, bool) {
	var cols []SchemaCol
	for _, part := range SplitComma(body) {
		if strings.TrimSpace(part) == "" || isTableConstraint(part) {
			continue
		}
		col, ok := parseSchemaColumn(part)
		if !ok {
			return nil, false
		}
		cols = append(cols, col)
	}
	return cols, true
}

func parseSchemaColumn(def string) (SchemaCol, bool) {
	sc := &Scan{S: strings.TrimSpace(def)}
	name, ok := sc.Ident()
	if !ok || isTableConstraint(name) {
		return SchemaCol{}, false
	}
	col := SchemaCol{Name: name}
	if !parseSchemaType(&col, sc) {
		return SchemaCol{}, false
	}
	annotateSchemaColumn(&col, def)
	return col, true
}

func parseSchemaType(col *SchemaCol, sc *Scan) bool {
	word := sc.Word()
	if word == "" {
		return false
	}
	base := strings.ToUpper(word)
	switch base {
	case "NATIONAL":
		next := strings.ToUpper(sc.Word())
		switch next {
		case "VARCHAR":
			base = "VARCHAR"
		case "CHAR", "CHARACTER":
			if sc.WordIs("VARYING") {
				base = "VARCHAR"
			} else {
				base = "CHAR"
			}
		default:
			return false
		}
	case "CHARACTER":
		if sc.WordIs("VARYING") {
			base = "VARCHAR"
		} else {
			base = "CHAR"
		}
	case "CHAR":
		if sc.WordIs("VARYING") {
			base = "VARCHAR"
		}
	case "INTEGER":
		base = "INT"
	case "BOOL", "BOOLEAN":
		base = "TINYINT"
	case "NUMERIC", "DEC", "FIXED":
		base = "DECIMAL"
	case "DOUBLE":
		sc.WordIs("PRECISION")
	case "REAL":
		base = "DOUBLE"
	}
	col.base = strings.ToLower(base)
	switch col.base {
	case "tinyint", "smallint", "mediumint", "int", "bigint", "float", "double", "decimal", "bit", "year", "time", "datetime", "timestamp", "char", "varchar", "binary", "varbinary":
		parseSchemaPrecision(col, sc)
		return true
	case "date", "json", "tinyblob", "blob", "mediumblob", "longblob", "tinytext", "text", "mediumtext", "longtext", "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection", "geomcollection", "vector":
		return true
	case "enum", "set":
		members, ok := parseMemberList(sc)
		if !ok {
			return false
		}
		col.members = members
		return true
	default:
		return false
	}
}

func parseSchemaPrecision(col *SchemaCol, sc *Scan) {
	if sc.PeekByte() != '(' {
		return
	}
	body, ok := sc.ParenBody()
	if !ok {
		return
	}
	parts := SplitComma(body)
	if len(parts) >= 1 {
		n, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err == nil {
			col.prec = n
			col.hasPrec = true
		}
	}
	if len(parts) >= 2 {
		n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err == nil {
			col.scale = n
		}
	}
}

func parseMemberList(sc *Scan) ([]string, bool) {
	body, ok := sc.ParenBody()
	if !ok {
		return nil, false
	}
	var members []string
	for _, part := range SplitComma(body) {
		member, ok := sqlStringLiteral(strings.TrimSpace(part))
		if !ok {
			return nil, false
		}
		members = append(members, member)
	}
	return members, true
}

func sqlStringLiteral(s string) (string, bool) {
	if len(s) < 2 {
		return "", false
	}
	quote := s[0]
	if quote != '\'' && quote != '"' {
		return "", false
	}
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] == quote {
			if i+1 < len(s) && s[i+1] == quote {
				b.WriteByte(quote)
				i++
				continue
			}
			if i == len(s)-1 {
				return b.String(), true
			}
			return "", false
		}
		if s[i] == '\\' && i+1 < len(s) {
			b.WriteByte(s[i+1])
			i++
			continue
		}
		b.WriteByte(s[i])
	}
	return "", false
}

func annotateSchemaColumn(col *SchemaCol, def string) {
	sc := &Scan{S: def}
	depth := 0
	for sc.I < len(sc.S) {
		sc.Skip()
		if sc.I >= len(sc.S) {
			return
		}
		switch sc.S[sc.I] {
		case '(':
			depth++
			sc.I++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.I++
		case '\'', '"', '`':
			sc.skipString(sc.S[sc.I])
		default:
			if !identStart(sc.S[sc.I]) {
				sc.I++
				continue
			}
			word := sc.bare()
			if depth != 0 {
				continue
			}
			switch strings.ToUpper(word) {
			case "UNSIGNED":
				col.unsigned = true
			case "CHARSET":
				if name, ok := sc.Ident(); ok {
					col.charset = strings.ToLower(strings.Trim(name, "`"))
				}
			case "CHARACTER":
				if sc.WordIs("SET") {
					if name, ok := sc.Ident(); ok {
						col.charset = strings.ToLower(strings.Trim(name, "`"))
					}
				}
			case "COLLATE":
				if name, ok := sc.Ident(); ok && col.charset == "" {
					if charset := charsetOfCollationName(name); charset != "" {
						col.charset = charset
					}
				}
			case "AS":
				if sc.PeekByte() == '(' {
					body, ok := sc.ParenBody()
					if ok {
						col.Generated = true
						col.Expr = body
					}
				}
			}
		}
	}
}

func ApplyTableCharset(cols []SchemaCol, tail string) []SchemaCol {
	charset := tableDefaultCharset(tail)
	if charset == "" {
		return cols
	}
	for i := range cols {
		if cols[i].charset == "" && charsetApplies(cols[i].base) {
			cols[i].charset = charset
		}
	}
	return cols
}

func tableDefaultCharset(tail string) string {
	sc := &Scan{S: tail}
	depth := 0
	for sc.I < len(sc.S) {
		sc.Skip()
		if sc.I >= len(sc.S) {
			return ""
		}
		switch sc.S[sc.I] {
		case '(':
			depth++
			sc.I++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.I++
		case '\'', '"', '`':
			sc.skipString(sc.S[sc.I])
		case '=':
			sc.I++
		default:
			if !identStart(sc.S[sc.I]) {
				sc.I++
				continue
			}
			word := sc.bare()
			if depth != 0 {
				continue
			}
			upper := strings.ToUpper(word)
			if upper == "CHARSET" || (upper == "CHARACTER" && sc.WordIs("SET")) {
				sc.Skip()
				if sc.I < len(sc.S) && sc.S[sc.I] == '=' {
					sc.I++
				}
				name, ok := sc.Ident()
				if ok {
					return strings.ToLower(strings.Trim(name, "`"))
				}
			}
		}
	}
	return ""
}

func charsetApplies(base string) bool {
	switch base {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
		return true
	default:
		return false
	}
}

func charsetOfCollationName(name string) string {
	n := strings.ToLower(strings.Trim(name, "`"))
	if n == "binary" {
		return "binary"
	}
	for _, charset := range collationCharsetPrefixes {
		if n == charset || strings.HasPrefix(n, charset+"_") {
			return charset
		}
	}
	return ""
}

// collationCharsetPrefixes are MySQL charset names, longest first, so
// utf8mb4 matches before utf8.
var collationCharsetPrefixes = []string{
	"utf8mb4", "utf16le", "utf8mb3", "armscii8", "geostd8", "keybcs2", "macroman",
	"utf16", "utf32", "latin1", "latin2", "latin5", "latin7", "cp1250", "cp1251",
	"cp1256", "cp1257", "cp850", "cp852", "cp866", "cp932", "gb2312", "greek",
	"hebrew", "koi8r", "koi8u", "tis620", "euckr", "ascii", "big5", "dec8",
	"hp8", "sjis", "swe7", "ucs2", "ujis", "gbk", "utf8",
}

func normalizeCharset(charset string) string {
	switch strings.ToLower(charset) {
	case "utf8", "utf8mb3":
		return "utf8mb3"
	default:
		return strings.ToLower(charset)
	}
}

// DumpSegment is the part of a schema file that follows one mysqldump
// "-- Host: ... Database:" header, up to the next header. db is empty for text
// before the first header.
type DumpSegment struct {
	DB  string
	SQL string
}

// DumpSegments splits concatenated single-database dumps so each header sets
// the schema for the dump that follows it, as USE does.
func DumpSegments(sql string) []DumpSegment {
	lines := strings.SplitAfter(sql, "\n")
	var out []DumpSegment
	cur := DumpSegment{}
	var b strings.Builder
	for _, line := range lines {
		if db := DumpDatabase(line); db != "" {
			cur.SQL = b.String()
			if strings.TrimSpace(cur.SQL) != "" || cur.DB != "" {
				out = append(out, cur)
			}
			cur = DumpSegment{DB: db}
			b.Reset()
		}
		b.WriteString(line)
	}
	cur.SQL = b.String()
	out = append(out, cur)
	return out
}

// DumpDatabase reads the mysqldump header "-- Host: ... Database: <db>".
func DumpDatabase(sql string) string {
	for _, line := range strings.Split(sql, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if !strings.HasPrefix(line, "--") {
			continue
		}
		body := strings.TrimSpace(strings.TrimPrefix(line, "--"))
		lower := strings.ToLower(body)
		if !strings.HasPrefix(lower, "host:") {
			continue
		}
		idx := strings.Index(lower, "database:")
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(body[idx+len("database:"):])
		if rest == "" {
			continue
		}
		// mysqldump writes the name unquoted to the end of the line, so a
		// name with spaces must keep everything after "Database:".
		field := strings.TrimSpace(rest)
		if len(field) >= 2 && field[0] == '`' && field[len(field)-1] == '`' {
			field = strings.ReplaceAll(field[1:len(field)-1], "``", "`")
		}
		if field == "" {
			continue
		}
		return field
	}
	return ""
}

// ValidateSchemaRow checks one row's metadata against cols. table is the
// name refusals print (refusalLabel).
func ValidateSchemaRow(table string, row model.FlashRow, cols []SchemaCol) (warns []model.FlashbackWarning, err error) {
	if len(row.Cols) == 0 {
		return nil, nil
	}
	if len(cols) != len(row.Columns) || len(row.Cols) != len(row.Columns) {
		return nil, schemaMismatch(table, columnCountDiff(cols, row.Columns))
	}
	extra, missing := columnNameDiff(cols, row.Columns)
	if len(extra) > 0 || len(missing) > 0 {
		var parts []string
		if len(extra) > 0 {
			parts = append(parts, i18n.Tf("error.flashbackSchemaExtra", map[string]any{"Columns": strings.Join(extra, ", ")}))
		}
		if len(missing) > 0 {
			parts = append(parts, i18n.Tf("error.flashbackSchemaMissing", map[string]any{"Columns": strings.Join(missing, ", ")}))
		}
		return nil, schemaMismatch(table, parts)
	}
	var reordered bool
	for i := range cols {
		if !strings.EqualFold(cols[i].Name, row.Columns[i]) {
			reordered = true
			break
		}
	}
	if reordered {
		return nil, schemaMismatch(table, []string{i18n.Tf("error.flashbackSchemaOrder", map[string]any{
			"File":   SchemaColNames(cols),
			"Binlog": strings.Join(row.Columns, ", "),
		})})
	}
	for i := range cols {
		if diff := schemaTypeDiff(cols[i], row.Cols[i]); diff != "" {
			return nil, schemaMismatch(table, []string{diff})
		}
	}
	return warns, nil
}

func schemaMismatch(table string, parts []string) error {
	return fmt.Errorf("%s", i18n.Tf("error.flashbackSchema", map[string]any{
		"Table": table,
		"Diff":  strings.Join(parts, "; "),
	}))
}

func columnCountDiff(cols []SchemaCol, binlog []string) []string {
	extra, missing := columnNameDiff(cols, binlog)
	if len(extra) == 0 && len(missing) == 0 {
		// Same names, different width is still a count problem. Name the positions.
		return []string{i18n.Tf("error.flashbackSchemaOrder", map[string]any{
			"File":   SchemaColNames(cols),
			"Binlog": strings.Join(binlog, ", "),
		})}
	}
	var parts []string
	if len(extra) > 0 {
		parts = append(parts, i18n.Tf("error.flashbackSchemaExtra", map[string]any{"Columns": strings.Join(extra, ", ")}))
	}
	if len(missing) > 0 {
		parts = append(parts, i18n.Tf("error.flashbackSchemaMissing", map[string]any{"Columns": strings.Join(missing, ", ")}))
	}
	return parts
}

func columnNameDiff(cols []SchemaCol, binlog []string) (extra, missing []string) {
	file := map[string]string{}
	for _, col := range cols {
		file[strings.ToLower(col.Name)] = col.Name
	}
	bin := map[string]string{}
	for _, name := range binlog {
		bin[strings.ToLower(name)] = name
	}
	for key, name := range file {
		if _, ok := bin[key]; !ok {
			extra = append(extra, name)
		}
	}
	for key, name := range bin {
		if _, ok := file[key]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(extra)
	sort.Strings(missing)
	return extra, missing
}

func SchemaColNames(cols []SchemaCol) string {
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name
	}
	return strings.Join(names, ", ")
}

func schemaTypeDiff(col SchemaCol, meta model.FlashCol) string {
	if col.base == "" || meta.Base == "" {
		return ""
	}
	if col.base != meta.Base {
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.Name,
			"File":   col.base,
			"Binlog": meta.Base,
		})
	}
	if schemaSigned(col.base) && meta.HasSign && col.unsigned != meta.Unsigned {
		file, bin := "signed", "signed"
		if col.unsigned {
			file = "unsigned"
		}
		if meta.Unsigned {
			bin = "unsigned"
		}
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.Name,
			"File":   file,
			"Binlog": bin,
		})
	}
	if charsetApplies(col.base) && col.charset != "" && meta.Charset != "" && normalizeCharset(col.charset) != normalizeCharset(meta.Charset) {
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.Name,
			"File":   col.charset,
			"Binlog": meta.Charset,
		})
	}
	if (col.base == "enum" || col.base == "set") && len(col.members) > 0 && len(meta.Members) > 0 {
		same, comparable := membersEqual(col.members, meta.Members)
		if comparable && !same {
			return i18n.Tf("error.flashbackSchemaType", map[string]any{
				"Column": col.Name,
				"File":   memberList(col.members),
				"Binlog": memberList(meta.Members),
			})
		}
	}
	if col.base == "decimal" && col.hasPrec && meta.HasPrec && (col.prec != meta.Prec || col.scale != meta.Scale) {
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.Name,
			"File":   fmt.Sprintf("decimal(%d,%d)", col.prec, col.scale),
			"Binlog": fmt.Sprintf("decimal(%d,%d)", meta.Prec, meta.Scale),
		})
	}
	if schemaFSP(col.base) && col.hasPrec && meta.HasFSP && col.prec != meta.FSP {
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.Name,
			"File":   fmt.Sprintf("%s(%d)", col.base, col.prec),
			"Binlog": fmt.Sprintf("%s(%d)", meta.Base, meta.FSP),
		})
	}
	return ""
}

func schemaSigned(base string) bool {
	switch base {
	case "tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "float", "double":
		return true
	default:
		return false
	}
}

func schemaFSP(base string) bool {
	switch base {
	case "time", "datetime", "timestamp":
		return true
	default:
		return false
	}
}

func membersEqual(file, bin []string) (same, comparable bool) {
	if len(file) != len(bin) {
		return false, true
	}
	for _, member := range bin {
		if !utf8.ValidString(member) {
			return true, false
		}
	}
	for i := range file {
		if file[i] != bin[i] {
			return false, true
		}
	}
	return true, true
}

func memberList(members []string) string {
	quoted := make([]string, len(members))
	for i, member := range members {
		quoted[i] = "'" + member + "'"
	}
	return strings.Join(quoted, ",")
}

// OneLine returns the first line of value, trimmed, with each internal
// whitespace run collapsed to one space (a TAB does not survive into a
// one-line excerpt), cut to limit bytes when limit is positive.
func OneLine(value string, limit int) string {
	value = strings.TrimSpace(value)
	if i := strings.IndexAny(value, "\r\n"); i >= 0 {
		value = value[:i]
	}
	value = strings.Join(strings.Fields(value), " ")
	if limit > 0 && len(value) > limit {
		value = value[:limit] + "..."
	}
	return value
}

func exprText(expr string) string {
	text := OneLine(expr, 80)
	if text == "" {
		return "(empty)"
	}
	return text
}

func schemaColIndexNames(names []string, want string) int {
	for i, name := range names {
		if strings.EqualFold(name, want) {
			return i
		}
	}
	return -1
}

// intVal is an exact rational. The final comparison rounds half away from
// zero, which is how MySQL assigns that result to an integer column.
// `/` keeps the exact quotient. `DIV` and `%`/`MOD` are integer operations
// and truncate toward zero. A zero divisor is NULL, matching a non-strict
// insert that stored NULL.
type intVal struct {
	null bool
	n    *big.Rat
}

func intValMatches(v intVal, lit string) bool {
	if v.null {
		return lit == "NULL"
	}
	if v.n == nil {
		return false
	}
	want, ok := new(big.Int).SetString(lit, 10)
	return ok && roundRatHalfAway(v.n).Cmp(want) == 0
}

func roundRatHalfAway(r *big.Rat) *big.Int {
	num := r.Num()
	den := r.Denom()
	q := new(big.Int)
	rem := new(big.Int)
	q.QuoRem(num, den, rem)
	twice := new(big.Int).Lsh(new(big.Int).Abs(rem), 1)
	if twice.Cmp(den) >= 0 {
		if num.Sign() >= 0 {
			q.Add(q, big.NewInt(1))
		} else {
			q.Sub(q, big.NewInt(1))
		}
	}
	return q
}

func consumeMulOp(sc *Scan) (string, bool) {
	sc.Skip()
	if sc.I >= len(sc.S) {
		return "", false
	}
	switch sc.S[sc.I] {
	case '*', '/', '%':
		op := string(sc.S[sc.I])
		sc.I++
		return op, true
	}
	if !identStart(sc.S[sc.I]) {
		return "", false
	}
	save := *sc
	word := strings.ToUpper(sc.bare())
	if word == "DIV" || word == "MOD" {
		return word, true
	}
	*sc = save
	return "", false
}

func applyMulOp(left, right intVal, op string) (intVal, bool) {
	if left.null || right.null {
		return intVal{null: true}, true
	}
	switch op {
	case "*":
		prod := new(big.Rat).Mul(left.n, right.n)
		if !fitsSignedBigint(prod) {
			return intVal{}, false
		}
		return intVal{n: prod}, true
	case "/":
		if right.n.Sign() == 0 {
			return intVal{null: true}, true
		}
		return intVal{n: new(big.Rat).Quo(left.n, right.n)}, true
	case "DIV":
		return applyIntDiv(left, right, false)
	case "%", "MOD":
		return applyIntDiv(left, right, true)
	default:
		return intVal{}, false
	}
}

// applyIntDiv evaluates DIV (remainder == false) or %/MOD.
// Both operands must already be integers. A fractional operand cannot be
// checked, because MySQL's decimal DIV is not the same as truncating first.
func applyIntDiv(left, right intVal, remainder bool) (intVal, bool) {
	if left.null || right.null {
		return intVal{null: true}, true
	}
	li, ok1 := ratAsInt(left.n)
	ri, ok2 := ratAsInt(right.n)
	if !ok1 || !ok2 {
		return intVal{}, false
	}
	if ri.Sign() == 0 {
		return intVal{null: true}, true
	}
	var out *big.Int
	if remainder {
		out = new(big.Int).Rem(li, ri)
	} else {
		out = new(big.Int).Quo(li, ri)
		if out.Cmp(maxInt64Big) > 0 || out.Cmp(minInt64Big) < 0 {
			return intVal{}, false
		}
	}
	return intVal{n: new(big.Rat).SetInt(out)}, true
}

func ratAsInt(r *big.Rat) (*big.Int, bool) {
	if r == nil || !r.IsInt() {
		return nil, false
	}
	return new(big.Int).Set(r.Num()), true
}

func schemaColIndex(cols []SchemaCol, name string) int {
	for i, col := range cols {
		if strings.EqualFold(col.Name, name) {
			return i
		}
	}
	return -1
}

func removeSchemaCol(cols []SchemaCol, name string) []SchemaCol {
	idx := schemaColIndex(cols, name)
	if idx < 0 {
		return cols
	}
	return removeSchemaColAt(cols, idx)
}

func removeSchemaColAt(cols []SchemaCol, idx int) []SchemaCol {
	out := make([]SchemaCol, 0, len(cols)-1)
	out = append(out, cols[:idx]...)
	out = append(out, cols[idx+1:]...)
	return out
}

func insertSchemaCol(cols []SchemaCol, pos int, col SchemaCol) []SchemaCol {
	if pos < 0 || pos > len(cols) {
		pos = len(cols)
	}
	out := make([]SchemaCol, 0, len(cols)+1)
	out = append(out, cols[:pos]...)
	out = append(out, col)
	out = append(out, cols[pos:]...)
	return out
}

func columnInsertPos(cols []SchemaCol, def string) int {
	sc := &Scan{S: def}
	depth := 0
	for sc.I < len(sc.S) {
		sc.Skip()
		if sc.I >= len(sc.S) {
			break
		}
		switch sc.S[sc.I] {
		case '(':
			depth++
			sc.I++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.I++
		case '\'', '"', '`':
			sc.skipString(sc.S[sc.I])
		default:
			if !identStart(sc.S[sc.I]) {
				sc.I++
				continue
			}
			word := sc.bare()
			if depth != 0 {
				continue
			}
			if strings.EqualFold(word, "FIRST") {
				return 0
			}
			if strings.EqualFold(word, "AFTER") {
				name, ok := sc.Ident()
				if !ok {
					return len(cols)
				}
				idx := schemaColIndex(cols, name)
				if idx < 0 {
					return len(cols)
				}
				return idx + 1
			}
		}
	}
	return len(cols)
}

func AlterNoColumnChange(action string) bool {
	sc := &Scan{S: strings.TrimSpace(action)}
	verb := strings.ToUpper(sc.Word())
	kind := strings.ToUpper(sc.PeekWord())
	switch verb {
	case "ADD":
		switch kind {
		case "INDEX", "KEY", "UNIQUE", "FULLTEXT", "SPATIAL", "CONSTRAINT", "PRIMARY", "FOREIGN":
			return true
		}
	case "DROP":
		switch kind {
		case "INDEX", "KEY", "PRIMARY", "FOREIGN", "CONSTRAINT", "CHECK":
			return true
		}
	case "RENAME":
		return !strings.EqualFold(kind, "COLUMN")
	case "ALGORITHM", "LOCK", "FORCE", "ORDER", "CONVERT", "DISABLE", "ENABLE", "DISCARD", "IMPORT", "COMMENT":
		return true
	}
	return false
}

func AlterSlice(cols *[]SchemaCol, action string) bool {
	if AlterNoColumnChange(action) {
		return true
	}
	sc := &Scan{S: strings.TrimSpace(action)}
	verb := strings.ToUpper(sc.Word())
	switch verb {
	case "ADD":
		if strings.EqualFold(sc.PeekWord(), "COLUMN") {
			sc.Word()
		}
		if sc.WordIs("IF") {
			sc.WordIs("NOT")
			sc.WordIs("EXISTS")
		}
		if sc.PeekByte() == '(' {
			body, ok := sc.ParenBody()
			if !ok {
				return false
			}
			added, ok := ParseSchemaColumns(body)
			if !ok {
				return false
			}
			*cols = append(*cols, added...)
			return true
		}
		return addOneColumn(cols, sc.Rest())
	case "DROP":
		if strings.EqualFold(sc.PeekWord(), "COLUMN") {
			sc.Word()
		}
		if sc.WordIs("IF") {
			sc.WordIs("EXISTS")
		}
		name, ok := sc.Ident()
		if !ok {
			return false
		}
		*cols = removeSchemaCol(*cols, name)
		return true
	case "MODIFY":
		sc.WordIs("COLUMN")
		return replaceSchemaColumn(cols, sc.Rest(), "")
	case "CHANGE":
		sc.WordIs("COLUMN")
		old, ok := sc.Ident()
		if !ok {
			return false
		}
		return replaceSchemaColumn(cols, sc.Rest(), old)
	case "RENAME":
		if !sc.WordIs("COLUMN") {
			return true
		}
		old, ok := sc.Ident()
		if !ok || !sc.WordIs("TO") {
			return false
		}
		next, ok := sc.Ident()
		if !ok {
			return false
		}
		idx := schemaColIndex(*cols, old)
		if idx < 0 {
			return true
		}
		(*cols)[idx].Name = next
		return true
	default:
		return true
	}
}

func addOneColumn(cols *[]SchemaCol, def string) bool {
	col, ok := parseSchemaColumn(def)
	if !ok {
		return false
	}
	// A schema file taken after this ALTER already has the column. MySQL
	// rejects a duplicate ADD, so an identical column means the file
	// includes the ALTER and the definition is already in effect.
	if idx := schemaColIndex(*cols, col.Name); idx >= 0 && sameSchemaCol((*cols)[idx], col) {
		return true
	}
	pos := columnInsertPos(*cols, def)
	*cols = insertSchemaCol(*cols, pos, col)
	return true
}

// sameSchemaCol reports whether two parsed columns have the same name, type
// and generation expression. Quoting, spacing and outer parentheses in the
// expression are ignored because mysqldump rewrites them. A charset counts
// only when both sides name one, since a file applies the table default.
func sameSchemaCol(a, b SchemaCol) bool {
	if !strings.EqualFold(a.Name, b.Name) || a.Generated != b.Generated ||
		!strings.EqualFold(a.base, b.base) || a.unsigned != b.unsigned {
		return false
	}
	if a.Generated && normalizeGenExpr(a.Expr) != normalizeGenExpr(b.Expr) {
		return false
	}
	if a.charset != "" && b.charset != "" && normalizeCharset(a.charset) != normalizeCharset(b.charset) {
		return false
	}
	if a.hasPrec && b.hasPrec && (a.prec != b.prec || a.scale != b.scale) {
		return false
	}
	if len(a.members) != len(b.members) {
		return false
	}
	for i := range a.members {
		if a.members[i] != b.members[i] {
			return false
		}
	}
	return true
}

func normalizeGenExpr(expr string) string {
	var b strings.Builder
	for _, r := range expr {
		if r == '`' || unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(unicode.ToLower(r))
	}
	out := b.String()
	for len(out) >= 2 && out[0] == '(' && out[len(out)-1] == ')' && outerParen(out) {
		out = out[1 : len(out)-1]
	}
	return out
}

// outerParen reports whether the first '(' closes at the last byte.
func outerParen(s string) bool {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}

func replaceSchemaColumn(cols *[]SchemaCol, def, old string) bool {
	col, ok := parseSchemaColumn(def)
	if !ok {
		return false
	}
	name := old
	if name == "" {
		name = col.Name
	}
	idx := schemaColIndex(*cols, name)
	if idx < 0 {
		return false
	}
	move := columnMoves(def)
	*cols = removeSchemaColAt(*cols, idx)
	pos := idx
	if move {
		pos = columnInsertPos(*cols, def)
	}
	*cols = insertSchemaCol(*cols, pos, col)
	return true
}

func columnMoves(def string) bool {
	sc := &Scan{S: def}
	depth := 0
	for sc.I < len(sc.S) {
		sc.Skip()
		if sc.I >= len(sc.S) {
			return false
		}
		switch sc.S[sc.I] {
		case '(':
			depth++
			sc.I++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.I++
		case '\'', '"', '`':
			sc.skipString(sc.S[sc.I])
		default:
			if !identStart(sc.S[sc.I]) {
				sc.I++
				continue
			}
			word := sc.bare()
			if depth != 0 {
				continue
			}
			if strings.EqualFold(word, "FIRST") || strings.EqualFold(word, "AFTER") {
				return true
			}
		}
	}
	return false
}
