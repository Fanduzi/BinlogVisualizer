// Package sqlemit assembles SQL statements shared by undo and redo emitters.
// input: quoted identifiers, image literals, primary-key column indexes, and generated-column sets.
// output: WHERE clauses, assignment lists, and sql_mode drop expressions both directions render with.
// pos: leaf emitter vocabulary; internal/flashback and internal/redo delegate to it so no second formatter drifts (ADR 0022).
// note: if this file changes, update this header and README.md.
package sqlemit

import (
	"strings"

	"binlogviz/internal/model"
)

// TableName joins schema and table for messages the way the undo emitter
// spells them.
func TableName(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// QuoteIdent quotes one identifier with backticks, doubling embedded ones.
func QuoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// QuoteColumnList joins columns as a quoted identifier list.
func QuoteColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = QuoteIdent(column)
	}
	return strings.Join(quoted, ", ")
}

// AssignColumns drops generated columns from the column/value pairs an
// INSERT column list or UPDATE SET writes; the server recomputes them.
func AssignColumns(columns, values []string, gen map[string]struct{}) ([]string, []string) {
	cols := make([]string, 0, len(columns))
	vals := make([]string, 0, len(values))
	for i, column := range columns {
		if IsGenerated(gen, column) {
			continue
		}
		cols = append(cols, column)
		val := "NULL"
		if i < len(values) {
			val = values[i]
		}
		vals = append(vals, val)
	}
	return cols, vals
}

// IsGenerated reports whether name is a generated column of this table.
// Generated names compare folded: the schema parser lowercases them.
func IsGenerated(gen map[string]struct{}, name string) bool {
	if len(gen) == 0 {
		return false
	}
	_, ok := gen[strings.ToLower(name)]
	return ok
}

// OmittedGeneratedNull reports whether an omitted generated column logged
// NULL: the server re-evaluates its expression on write, which strict
// ERROR_FOR_DIVISION_BY_ZERO can turn into ERROR 1365.
func OmittedGeneratedNull(columns, values []string, gen map[string]struct{}) bool {
	for i, column := range columns {
		if IsGenerated(gen, column) && i < len(values) && values[i] == "NULL" {
			return true
		}
	}
	return false
}

// KeyMatch is the primary-key match for image.
func KeyMatch(row model.FlashRow, image []string) string {
	parts := make([]string, 0, len(row.PK))
	for _, index := range row.PK {
		if index >= 0 && index < len(row.Columns) && index < len(image) {
			parts = append(parts, QuoteIdent(row.Columns[index])+" <=> "+image[index])
		}
	}
	return strings.Join(parts, " AND ")
}

// WhereImage appends the row-match WHERE clause to head: the primary key, or
// with allColumns (the --conflict guard) every logged column of image, so a
// row changed later matches nothing. ok is false when no column could match
// (an all-generated or empty image); the caller turns that into its refusal.
func WhereImage(row model.FlashRow, head string, image []string, cond string, allColumns bool) (statement string, ok bool) {
	pk := !row.NoPK && len(row.PK) > 0
	indexes := row.PK
	if !pk || allColumns {
		indexes = make([]int, len(row.Columns))
		for i := range indexes {
			indexes[i] = i
		}
	}
	parts := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(row.Columns) || index >= len(image) {
			continue
		}
		parts = append(parts, QuoteIdent(row.Columns[index])+" <=> "+image[index])
	}
	if len(parts) == 0 {
		return "", false
	}
	if cond != "" {
		parts = append(parts, cond)
	}
	statement = head + " WHERE " + strings.Join(parts, " AND ")
	if !pk {
		statement += " LIMIT 1"
		return "-- no primary key on " + TableName(row.Schema, row.Table) + "; this matches every column and LIMIT 1\n" + statement + ";", true
	}
	return statement + ";", true
}

// JoinConds ANDs two optional conditions.
func JoinConds(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + " AND " + b
}

// RelaxedModes lists the sql_mode flags that would reject a value this row
// stored legally under the session that wrote it. Each flag only turns a
// stored value into an error, so dropping it for this one statement changes
// no restored value:
//   - ENUM index 0 (the error member) needs the strict modes off.
//   - A zero month or day needs NO_ZERO_DATE and NO_ZERO_IN_DATE off. Strict
//     stays on, so every other value in the statement is still checked.
//   - A generated column left out of INSERT or UPDATE SET is recomputed by
//     the server. When its logged value is NULL the expression may divide by
//     zero, which ERROR_FOR_DIVISION_BY_ZERO turns into ERROR 1365. Without
//     that flag x/0 is NULL, the value the binlog holds.
//
// TRADITIONAL is removed with any of them: MySQL re-expands that token into
// all of the flags above.
func RelaxedModes(row model.FlashRow, gen map[string]struct{}) []string {
	var drops []string
	if row.NonStrict {
		drops = append(drops, "STRICT_ALL_TABLES", "STRICT_TRANS_TABLES")
	}
	if row.ZeroDate {
		drops = append(drops, "NO_ZERO_DATE", "NO_ZERO_IN_DATE")
	}
	if row.Op != "INSERT" && OmittedGeneratedNull(row.Columns, row.Before, gen) {
		drops = append(drops, "ERROR_FOR_DIVISION_BY_ZERO")
	}
	if len(drops) > 0 {
		drops = append(drops, "TRADITIONAL")
	}
	return drops
}

// DroppedModeExpr is the sql_mode expression with drops removed: the client
// computes it from the real session mode. Callers assign it to their own
// session variable; undo applies it inside the guard CALL and redo applies
// it directly around the statement.
func DroppedModeExpr(drops []string) string {
	expr := "CONCAT(',', @@SESSION.sql_mode, ',')"
	for _, flag := range drops {
		expr = "REPLACE(" + expr + ", '," + flag + ",', ',')"
	}
	return "TRIM(BOTH ',' FROM " + expr + ")"
}
