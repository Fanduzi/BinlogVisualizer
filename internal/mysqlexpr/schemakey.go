// Package mysqlexpr parses the table definitions a schema file supplies.
// pos: primary-key capture shared by CREATE and ALTER parsing (ADR 0030).
// input: one CREATE TABLE body, or one ALTER action.
// output: columns marked with the key they declare, and the predicate that
// tells a key-changing ALTER from one that leaves the key alone.
// note: if this file changes, update this header and README.md.
package mysqlexpr

import "strings"

func ParseSchemaColumns(body string) ([]SchemaCol, bool) {
	var cols []SchemaCol
	var keyNames []string
	for _, part := range SplitComma(body) {
		if strings.TrimSpace(part) == "" {
			continue
		}
		if isTableConstraint(part) {
			if names := primaryKeyColumns(part); len(names) > 0 {
				keyNames = names
			}
			continue
		}
		col, ok := parseSchemaColumn(part)
		if !ok {
			return nil, false
		}
		cols = append(cols, col)
	}
	if len(keyNames) > 0 {
		// A table-level PRIMARY KEY names the key; the declaration wins over an
		// inline PRIMARY KEY on another column.
		for i := range cols {
			cols[i].Primary = false
		}
		for _, name := range keyNames {
			for i := range cols {
				if strings.EqualFold(cols[i].Name, name) {
					cols[i].Primary = true
					break
				}
			}
		}
	}
	return cols, true
}

// primaryKeyColumns returns the column names of a PRIMARY KEY constraint, with
// or without a CONSTRAINT name, or nil when the part is another constraint.
func primaryKeyColumns(part string) []string {
	sc := &Scan{S: strings.TrimSpace(part)}
	if sc.WordIs("CONSTRAINT") {
		if _, ok := sc.Ident(); !ok {
			return nil
		}
	}
	if !sc.WordIs("PRIMARY") || !sc.WordIs("KEY") {
		return nil
	}
	// An index name may sit between KEY and the column list.
	sc.Skip()
	if sc.PeekByte() != '(' {
		if _, ok := sc.Ident(); !ok {
			return nil
		}
		sc.Skip()
	}
	if sc.PeekByte() != '(' {
		return nil
	}
	body, ok := sc.ParenBody()
	if !ok {
		return nil
	}
	var names []string
	for _, item := range SplitComma(body) {
		is := &Scan{S: strings.TrimSpace(item)}
		if name, ok := is.Ident(); ok {
			names = append(names, strings.Trim(name, "`"))
		}
	}
	return names
}

// AlterChangesPrimaryKey reports whether an ALTER action changes the table's
// primary key. Such a change is not modelled: the definition keeps naming the
// columns, but its key stops being trusted, so undo falls back to matching
// every column instead of addressing a row by a key the dump no longer owns
// (ADR 0030).
func AlterChangesPrimaryKey(action string) bool {
	fields := strings.Fields(strings.ToUpper(action))
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] == "PRIMARY" && strings.HasPrefix(fields[i+1], "KEY") {
			return true
		}
	}
	return false
}
