// Package analyzer checks a schema file against TABLE_MAP metadata.
// input: CREATE/ALTER text from --schema-file or the binlog, plus flashback rows that carry column metadata.
// output: the column list in effect for one table, or an error that names the table and the columns that differ. Unqualified names are bound only when one schema is unambiguous.
// pos: flashback-only helper. Analyze does not call it.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// schemaCol is one column of a CREATE or ALTER definition, in table order.
type schemaCol struct {
	name      string
	generated bool
	expr      string
	base      string
	unsigned  bool
	charset   string
	members   []string
	prec      int
	scale     int
	hasPrec   bool
}

type pendingDef struct {
	table       string
	cols        []schemaCol
	bad         bool
	abandoned   bool
	boundSchema string
}

type abandonedTable struct {
	table   string
	schemas []string
}

func (g *generatedTables) setDef(key string, cols []schemaCol, fromFile bool) {
	g.ensure()
	if g.defs == nil {
		g.defs = map[string][]schemaCol{}
	}
	copied := cloneSchemaCols(cols)
	g.defs[key] = copied
	g.cols[key] = generatedNameSet(copied)
	delete(g.bad, key)
	if fromFile {
		if g.fromFile == nil {
			g.fromFile = map[string]bool{}
		}
		g.fromFile[key] = true
		return
	}
	delete(g.fromFile, key)
	delete(g.pendingBound, key)
}

func (g *generatedTables) markBad(key string) {
	g.ensure()
	g.bad[key] = struct{}{}
	delete(g.cols, key)
	delete(g.defs, key)
	delete(g.fromFile, key)
	delete(g.pendingBound, key)
}

func (g *generatedTables) putPending(table string, cols []schemaCol) {
	if g.pending == nil {
		g.pending = map[string]*pendingDef{}
	}
	g.pending[table] = &pendingDef{table: table, cols: cloneSchemaCols(cols)}
}

func (g *generatedTables) putPendingBad(table string) {
	g.sawUnqualified = true
	if g.ambiguousDB {
		return
	}
	if g.pending == nil {
		g.pending = map[string]*pendingDef{}
	}
	g.pending[table] = &pendingDef{table: table, bad: true}
}

// observe records one schema that contains table. A schema-file definition
// that was unqualified is bound to that schema when it is the only one.
// A second schema abandons the binding so the definition is not guessed.
// abandoned is true only on the call that drops the binding.
func (g *generatedTables) observe(schema, table string) bool {
	if g == nil || schema == "" || table == "" {
		return false
	}
	if g.seen == nil {
		g.seen = map[string]map[string]struct{}{}
	}
	set := g.seen[table]
	if set == nil {
		set = map[string]struct{}{}
		g.seen[table] = set
	}
	set[schema] = struct{}{}
	p := g.pending[table]
	if p == nil || p.abandoned {
		return false
	}
	if len(set) > 1 {
		return g.abandon(table)
	}
	g.bindPending(schema, table)
	return false
}

func (g *generatedTables) bindPending(schema, table string) {
	p := g.pending[table]
	if p == nil || p.abandoned || p.boundSchema != "" {
		return
	}
	key := generatedKey(schema, table)
	if g.known(key) || g.unknown(schema, table) {
		delete(g.pending, table)
		return
	}
	if p.bad {
		g.markBad(key)
		p.boundSchema = schema
		if g.pendingBound == nil {
			g.pendingBound = map[string]bool{}
		}
		g.pendingBound[key] = true
		return
	}
	g.setDef(key, p.cols, true)
	if g.pendingBound == nil {
		g.pendingBound = map[string]bool{}
	}
	g.pendingBound[key] = true
	p.boundSchema = schema
}

func (g *generatedTables) abandon(table string) bool {
	p := g.pending[table]
	if p == nil || p.abandoned {
		return false
	}
	p.abandoned = true
	if p.boundSchema != "" {
		key := generatedKey(p.boundSchema, p.table)
		if g.pendingBound[key] {
			delete(g.defs, key)
			delete(g.cols, key)
			delete(g.fromFile, key)
			delete(g.bad, key)
			delete(g.pendingBound, key)
		}
	}
	var schemas []string
	for schema := range g.seen[table] {
		schemas = append(schemas, schema)
	}
	sort.Strings(schemas)
	g.abandoned = append(g.abandoned, abandonedTable{table: p.table, schemas: schemas})
	return true
}

// definition returns the column list in effect for schema.table.
// fromFile means the list came from --schema-file and must be checked.
// pending means the list was bound from an unqualified name and the check
// waits until every schema of that table has been seen.
func (g *generatedTables) definition(schema, table string) (cols []schemaCol, fromFile, pending bool, ok bool) {
	if g == nil || g.defs == nil {
		return nil, false, false, false
	}
	key := generatedKey(schema, table)
	cols, ok = g.defs[key]
	if !ok {
		return nil, false, false, false
	}
	return cloneSchemaCols(cols), g.fromFile[key], g.pendingBound[key], true
}

func (g *generatedTables) schemaWarnings() []string {
	if g == nil {
		return nil
	}
	var out []string
	if g.ambiguousDB && g.sawUnqualified {
		out = append(out, i18n.Tf("warning.flashbackSchemaDB", map[string]any{
			"Header": g.headerDB,
			"Flag":   g.flagDB,
		}))
	}
	for _, item := range g.abandoned {
		out = append(out, i18n.Tf("warning.flashbackSchemaTable", map[string]any{
			"Table":   item.table,
			"Schemas": strings.Join(item.schemas, ", "),
		}))
	}
	return out
}

func cloneSchemaCols(cols []schemaCol) []schemaCol {
	if cols == nil {
		return nil
	}
	out := make([]schemaCol, len(cols))
	for i, col := range cols {
		out[i] = col
		if col.members != nil {
			out[i].members = append([]string(nil), col.members...)
		}
	}
	return out
}

func generatedNameSet(cols []schemaCol) map[string]struct{} {
	out := map[string]struct{}{}
	for _, col := range cols {
		if col.generated {
			out[strings.ToLower(col.name)] = struct{}{}
		}
	}
	return out
}

func parseSchemaColumns(body string) ([]schemaCol, bool) {
	var cols []schemaCol
	for _, part := range splitComma(body) {
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

func parseSchemaColumn(def string) (schemaCol, bool) {
	sc := &sqlScan{s: strings.TrimSpace(def)}
	name, ok := sc.ident()
	if !ok || isTableConstraint(name) {
		return schemaCol{}, false
	}
	col := schemaCol{name: name}
	if !parseSchemaType(&col, sc) {
		return schemaCol{}, false
	}
	annotateSchemaColumn(&col, def)
	return col, true
}

func parseSchemaType(col *schemaCol, sc *sqlScan) bool {
	word := sc.word()
	if word == "" {
		return false
	}
	base := strings.ToUpper(word)
	switch base {
	case "NATIONAL":
		next := strings.ToUpper(sc.word())
		switch next {
		case "VARCHAR":
			base = "VARCHAR"
		case "CHAR", "CHARACTER":
			if sc.wordIs("VARYING") {
				base = "VARCHAR"
			} else {
				base = "CHAR"
			}
		default:
			return false
		}
	case "CHARACTER":
		if sc.wordIs("VARYING") {
			base = "VARCHAR"
		} else {
			base = "CHAR"
		}
	case "CHAR":
		if sc.wordIs("VARYING") {
			base = "VARCHAR"
		}
	case "INTEGER":
		base = "INT"
	case "BOOL", "BOOLEAN":
		base = "TINYINT"
	case "NUMERIC", "DEC", "FIXED":
		base = "DECIMAL"
	case "DOUBLE":
		sc.wordIs("PRECISION")
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

func parseSchemaPrecision(col *schemaCol, sc *sqlScan) {
	if sc.peekByte() != '(' {
		return
	}
	body, ok := sc.parenBody()
	if !ok {
		return
	}
	parts := splitComma(body)
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

func parseMemberList(sc *sqlScan) ([]string, bool) {
	body, ok := sc.parenBody()
	if !ok {
		return nil, false
	}
	var members []string
	for _, part := range splitComma(body) {
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

func annotateSchemaColumn(col *schemaCol, def string) {
	sc := &sqlScan{s: def}
	depth := 0
	for sc.i < len(sc.s) {
		sc.skip()
		if sc.i >= len(sc.s) {
			return
		}
		switch sc.s[sc.i] {
		case '(':
			depth++
			sc.i++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.i++
		case '\'', '"', '`':
			sc.skipString(sc.s[sc.i])
		default:
			if !identStart(sc.s[sc.i]) {
				sc.i++
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
				if name, ok := sc.ident(); ok {
					col.charset = strings.ToLower(strings.Trim(name, "`"))
				}
			case "CHARACTER":
				if sc.wordIs("SET") {
					if name, ok := sc.ident(); ok {
						col.charset = strings.ToLower(strings.Trim(name, "`"))
					}
				}
			case "COLLATE":
				if name, ok := sc.ident(); ok && col.charset == "" {
					if charset := charsetOfCollationName(name); charset != "" {
						col.charset = charset
					}
				}
			case "AS":
				if sc.peekByte() == '(' {
					body, ok := sc.parenBody()
					if ok {
						col.generated = true
						col.expr = body
					}
				}
			}
		}
	}
}

func applyTableCharset(cols []schemaCol, tail string) []schemaCol {
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
	sc := &sqlScan{s: tail}
	depth := 0
	for sc.i < len(sc.s) {
		sc.skip()
		if sc.i >= len(sc.s) {
			return ""
		}
		switch sc.s[sc.i] {
		case '(':
			depth++
			sc.i++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.i++
		case '\'', '"', '`':
			sc.skipString(sc.s[sc.i])
		case '=':
			sc.i++
		default:
			if !identStart(sc.s[sc.i]) {
				sc.i++
				continue
			}
			word := sc.bare()
			if depth != 0 {
				continue
			}
			upper := strings.ToUpper(word)
			if upper == "CHARSET" || (upper == "CHARACTER" && sc.wordIs("SET")) {
				sc.skip()
				if sc.i < len(sc.s) && sc.s[sc.i] == '=' {
					sc.i++
				}
				name, ok := sc.ident()
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

// dumpDatabase reads the mysqldump header "-- Host: ... Database: <db>".
func dumpDatabase(sql string) string {
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
		field := strings.Trim(strings.Fields(rest)[0], "`")
		if field == "" {
			continue
		}
		return field
	}
	return ""
}

func validateSchemaRow(row model.FlashRow, cols []schemaCol) error {
	if len(row.Cols) == 0 {
		return nil
	}
	table := flashTable(row.Schema, row.Table)
	if len(cols) != len(row.Columns) || len(row.Cols) != len(row.Columns) {
		return schemaMismatch(table, columnCountDiff(cols, row.Columns))
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
		return schemaMismatch(table, parts)
	}
	var reordered bool
	for i := range cols {
		if !strings.EqualFold(cols[i].name, row.Columns[i]) {
			reordered = true
			break
		}
	}
	if reordered {
		return schemaMismatch(table, []string{i18n.Tf("error.flashbackSchemaOrder", map[string]any{
			"File":   schemaColNames(cols),
			"Binlog": strings.Join(row.Columns, ", "),
		})})
	}
	for i := range cols {
		if diff := schemaTypeDiff(cols[i], row.Cols[i]); diff != "" {
			return schemaMismatch(table, []string{diff})
		}
	}
	for i := range cols {
		if !cols[i].generated {
			continue
		}
		if diff := generatedValueDiff(cols[i], row); diff != "" {
			return schemaMismatch(table, []string{diff})
		}
	}
	return nil
}

func schemaMismatch(table string, parts []string) error {
	return fmt.Errorf("%s", i18n.Tf("error.flashbackSchema", map[string]any{
		"Table": table,
		"Diff":  strings.Join(parts, "; "),
	}))
}

func columnCountDiff(cols []schemaCol, binlog []string) []string {
	extra, missing := columnNameDiff(cols, binlog)
	if len(extra) == 0 && len(missing) == 0 {
		// Same names, different width is still a count problem. Name the positions.
		return []string{i18n.Tf("error.flashbackSchemaOrder", map[string]any{
			"File":   schemaColNames(cols),
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

func columnNameDiff(cols []schemaCol, binlog []string) (extra, missing []string) {
	file := map[string]string{}
	for _, col := range cols {
		file[strings.ToLower(col.name)] = col.name
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

func schemaColNames(cols []schemaCol) string {
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.name
	}
	return strings.Join(names, ", ")
}

func schemaTypeDiff(col schemaCol, meta model.FlashCol) string {
	if col.base == "" || meta.Base == "" {
		return ""
	}
	if col.base != meta.Base {
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.name,
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
			"Column": col.name,
			"File":   file,
			"Binlog": bin,
		})
	}
	if charsetApplies(col.base) && col.charset != "" && meta.Charset != "" && normalizeCharset(col.charset) != normalizeCharset(meta.Charset) {
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.name,
			"File":   col.charset,
			"Binlog": meta.Charset,
		})
	}
	if (col.base == "enum" || col.base == "set") && len(col.members) > 0 && len(meta.Members) > 0 {
		same, comparable := membersEqual(col.members, meta.Members)
		if comparable && !same {
			return i18n.Tf("error.flashbackSchemaType", map[string]any{
				"Column": col.name,
				"File":   memberList(col.members),
				"Binlog": memberList(meta.Members),
			})
		}
	}
	if col.base == "decimal" && col.hasPrec && meta.HasPrec && (col.prec != meta.Prec || col.scale != meta.Scale) {
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.name,
			"File":   fmt.Sprintf("decimal(%d,%d)", col.prec, col.scale),
			"Binlog": fmt.Sprintf("decimal(%d,%d)", meta.Prec, meta.Scale),
		})
	}
	if schemaFSP(col.base) && col.hasPrec && meta.HasFSP && col.prec != meta.FSP {
		return i18n.Tf("error.flashbackSchemaType", map[string]any{
			"Column": col.name,
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

func generatedValueDiff(col schemaCol, row model.FlashRow) string {
	if strings.TrimSpace(col.expr) == "" {
		return uncheckedGenerated(col)
	}
	var images [][]string
	if row.Before != nil {
		images = append(images, row.Before)
	}
	if row.After != nil {
		images = append(images, row.After)
	}
	if len(images) == 0 {
		return uncheckedGenerated(col)
	}
	for _, image := range images {
		env := map[string]string{}
		for i, name := range row.Columns {
			if i < len(image) {
				env[strings.ToLower(name)] = image[i]
			}
		}
		want, ok := evalIntExpr(col.expr, env)
		if !ok {
			return uncheckedGenerated(col)
		}
		idx := schemaColIndexNames(row.Columns, col.name)
		if idx < 0 || idx >= len(image) || !intValMatches(want, image[idx]) {
			return i18n.Tf("error.flashbackSchemaGenerated", map[string]any{
				"Column": col.name,
				"Expr":   oneLine(col.expr, 80),
			})
		}
	}
	return ""
}

func uncheckedGenerated(col schemaCol) string {
	return i18n.Tf("error.flashbackSchemaUnchecked", map[string]any{
		"Column": col.name,
		"Expr":   oneLine(col.expr, 80),
	})
}

func schemaColIndexNames(names []string, want string) int {
	for i, name := range names {
		if strings.EqualFold(name, want) {
			return i
		}
	}
	return -1
}

type intVal struct {
	null bool
	n    int64
}

func intValMatches(v intVal, lit string) bool {
	if v.null {
		return lit == "NULL"
	}
	n, err := strconv.ParseInt(lit, 10, 64)
	return err == nil && n == v.n
}

func evalIntExpr(expr string, env map[string]string) (intVal, bool) {
	sc := &sqlScan{s: expr}
	v, ok := parseAddExpr(sc, env)
	if !ok {
		return intVal{}, false
	}
	sc.skip()
	if sc.i != len(sc.s) {
		return intVal{}, false
	}
	return v, true
}

func parseAddExpr(sc *sqlScan, env map[string]string) (intVal, bool) {
	left, ok := parseMulExpr(sc, env)
	if !ok {
		return intVal{}, false
	}
	for {
		sc.skip()
		if sc.i >= len(sc.s) || (sc.s[sc.i] != '+' && sc.s[sc.i] != '-') {
			return left, true
		}
		op := sc.s[sc.i]
		sc.i++
		right, ok := parseMulExpr(sc, env)
		if !ok {
			return intVal{}, false
		}
		if left.null || right.null {
			left = intVal{null: true}
			continue
		}
		if op == '+' {
			left.n += right.n
		} else {
			left.n -= right.n
		}
	}
}

func parseMulExpr(sc *sqlScan, env map[string]string) (intVal, bool) {
	left, ok := parseUnaryExpr(sc, env)
	if !ok {
		return intVal{}, false
	}
	for {
		sc.skip()
		if sc.i >= len(sc.s) || (sc.s[sc.i] != '*' && sc.s[sc.i] != '/') {
			return left, true
		}
		op := sc.s[sc.i]
		sc.i++
		right, ok := parseUnaryExpr(sc, env)
		if !ok {
			return intVal{}, false
		}
		if left.null || right.null {
			left = intVal{null: true}
			continue
		}
		if op == '*' {
			left.n *= right.n
			continue
		}
		if right.n == 0 {
			left = intVal{null: true}
			continue
		}
		left.n /= right.n
	}
}

func parseUnaryExpr(sc *sqlScan, env map[string]string) (intVal, bool) {
	sc.skip()
	if sc.i < len(sc.s) && (sc.s[sc.i] == '-' || sc.s[sc.i] == '+') {
		op := sc.s[sc.i]
		sc.i++
		v, ok := parseUnaryExpr(sc, env)
		if !ok || v.null || op == '+' {
			return v, ok
		}
		v.n = -v.n
		return v, true
	}
	return parsePrimaryExpr(sc, env)
}

func parsePrimaryExpr(sc *sqlScan, env map[string]string) (intVal, bool) {
	sc.skip()
	if sc.i >= len(sc.s) {
		return intVal{}, false
	}
	if sc.s[sc.i] == '(' {
		body, ok := sc.parenBody()
		if !ok {
			return intVal{}, false
		}
		return evalIntExpr(body, env)
	}
	if sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
		start := sc.i
		for sc.i < len(sc.s) && sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
			sc.i++
		}
		if sc.i < len(sc.s) && sc.s[sc.i] == '.' {
			return intVal{}, false
		}
		n, err := strconv.ParseInt(sc.s[start:sc.i], 10, 64)
		if err != nil {
			return intVal{}, false
		}
		return intVal{n: n}, true
	}
	name, ok := sc.ident()
	if !ok {
		return intVal{}, false
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '.' {
		return intVal{}, false
	}
	lit, ok := env[strings.ToLower(name)]
	if !ok {
		return intVal{}, false
	}
	if lit == "NULL" {
		return intVal{null: true}, true
	}
	n, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		return intVal{}, false
	}
	return intVal{n: n}, true
}

func schemaColIndex(cols []schemaCol, name string) int {
	for i, col := range cols {
		if strings.EqualFold(col.name, name) {
			return i
		}
	}
	return -1
}

func removeSchemaCol(cols []schemaCol, name string) []schemaCol {
	idx := schemaColIndex(cols, name)
	if idx < 0 {
		return cols
	}
	return removeSchemaColAt(cols, idx)
}

func removeSchemaColAt(cols []schemaCol, idx int) []schemaCol {
	out := make([]schemaCol, 0, len(cols)-1)
	out = append(out, cols[:idx]...)
	out = append(out, cols[idx+1:]...)
	return out
}

func insertSchemaCol(cols []schemaCol, pos int, col schemaCol) []schemaCol {
	if pos < 0 || pos > len(cols) {
		pos = len(cols)
	}
	out := make([]schemaCol, 0, len(cols)+1)
	out = append(out, cols[:pos]...)
	out = append(out, col)
	out = append(out, cols[pos:]...)
	return out
}

func columnInsertPos(cols []schemaCol, def string) int {
	sc := &sqlScan{s: def}
	depth := 0
	for sc.i < len(sc.s) {
		sc.skip()
		if sc.i >= len(sc.s) {
			break
		}
		switch sc.s[sc.i] {
		case '(':
			depth++
			sc.i++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.i++
		case '\'', '"', '`':
			sc.skipString(sc.s[sc.i])
		default:
			if !identStart(sc.s[sc.i]) {
				sc.i++
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
				name, ok := sc.ident()
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

func alterNoColumnChange(action string) bool {
	sc := &sqlScan{s: strings.TrimSpace(action)}
	verb := strings.ToUpper(sc.word())
	kind := strings.ToUpper(sc.peekWord())
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

func alterSlice(cols *[]schemaCol, action string) bool {
	if alterNoColumnChange(action) {
		return true
	}
	sc := &sqlScan{s: strings.TrimSpace(action)}
	verb := strings.ToUpper(sc.word())
	switch verb {
	case "ADD":
		if strings.EqualFold(sc.peekWord(), "COLUMN") {
			sc.word()
		}
		if sc.wordIs("IF") {
			sc.wordIs("NOT")
			sc.wordIs("EXISTS")
		}
		if sc.peekByte() == '(' {
			body, ok := sc.parenBody()
			if !ok {
				return false
			}
			added, ok := parseSchemaColumns(body)
			if !ok {
				return false
			}
			*cols = append(*cols, added...)
			return true
		}
		return addOneColumn(cols, sc.rest())
	case "DROP":
		if strings.EqualFold(sc.peekWord(), "COLUMN") {
			sc.word()
		}
		if sc.wordIs("IF") {
			sc.wordIs("EXISTS")
		}
		name, ok := sc.ident()
		if !ok {
			return false
		}
		*cols = removeSchemaCol(*cols, name)
		return true
	case "MODIFY":
		sc.wordIs("COLUMN")
		return replaceSchemaColumn(cols, sc.rest(), "")
	case "CHANGE":
		sc.wordIs("COLUMN")
		old, ok := sc.ident()
		if !ok {
			return false
		}
		return replaceSchemaColumn(cols, sc.rest(), old)
	case "RENAME":
		if !sc.wordIs("COLUMN") {
			return true
		}
		old, ok := sc.ident()
		if !ok || !sc.wordIs("TO") {
			return false
		}
		next, ok := sc.ident()
		if !ok {
			return false
		}
		idx := schemaColIndex(*cols, old)
		if idx < 0 {
			return true
		}
		(*cols)[idx].name = next
		return true
	default:
		return true
	}
}

func addOneColumn(cols *[]schemaCol, def string) bool {
	col, ok := parseSchemaColumn(def)
	if !ok {
		return false
	}
	pos := columnInsertPos(*cols, def)
	*cols = insertSchemaCol(*cols, pos, col)
	return true
}

func replaceSchemaColumn(cols *[]schemaCol, def, old string) bool {
	col, ok := parseSchemaColumn(def)
	if !ok {
		return false
	}
	name := old
	if name == "" {
		name = col.name
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
	sc := &sqlScan{s: def}
	depth := 0
	for sc.i < len(sc.s) {
		sc.skip()
		if sc.i >= len(sc.s) {
			return false
		}
		switch sc.s[sc.i] {
		case '(':
			depth++
			sc.i++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.i++
		case '\'', '"', '`':
			sc.skipString(sc.s[sc.i])
		default:
			if !identStart(sc.s[sc.i]) {
				sc.i++
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
