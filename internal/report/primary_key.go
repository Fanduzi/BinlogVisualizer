package report

import (
	"fmt"
	"sort"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

type pkTable struct {
	schema string
	table  string
	update int
	delete int
	insert int
}

func (t pkTable) name() string {
	switch {
	case t.schema != "" && t.table != "":
		return t.schema + "." + t.table
	case t.table != "":
		return t.table
	default:
		return t.schema
	}
}

// DefinitionKeys reports the primary-key columns a schema definition declares
// for a table, and whether the definition covers that table. Nil columns with
// covered=true mean the definition declares no key (ADR 0031).
type DefinitionKeys func(schema, table string) (columns []string, covered bool)

// pkView is the primary-key section shared by every analyze format.
// summaryLine is the single extra line used when there is no lag ranking.
// note is the unknown-metadata sentence, including when a lag section is also shown.
type pkView struct {
	summaryLine string
	note        string
	risks       []pkTable
	insertOnly  []pkTable
}

func primaryKeyView(tables []model.TableStats, def DefinitionKeys) pkView {
	var risks, insertOnly []pkTable
	rowTables := 0
	unknown := false
	known := false
	fromDefinition := false
	fromBinlog := false
	definedNoKey := false
	for _, table := range tables {
		if table.InsertRows+table.UpdateRows+table.DeleteRows == 0 {
			continue
		}
		rowTables++
		status := table.KeyStatus
		if status == "" {
			status = model.KeyStatusUnknown
		}
		// The binlog wins wherever it says something; the definition only fills
		// the gap it leaves (ADR 0031).
		viaDefinition := false
		if status == model.KeyStatusUnknown && def != nil {
			if columns, covered := def(table.Schema, table.Table); covered {
				viaDefinition = true
				fromDefinition = true
				if len(columns) > 0 {
					status = model.KeyStatusHasPK
				} else {
					status = model.KeyStatusNoPK
				}
			}
		}
		switch status {
		case model.KeyStatusHasPK:
			known = true
			if !viaDefinition {
				fromBinlog = true
			}
		case model.KeyStatusNoPK:
			known = true
			if !viaDefinition {
				fromBinlog = true
			}
			row := pkTable{
				schema: table.Schema,
				table:  table.Table,
				update: table.NoPKUpdateRows,
				delete: table.NoPKDeleteRows,
				insert: table.NoPKInsertRows,
			}
			if viaDefinition {
				row.update = table.UpdateRows
				row.delete = table.DeleteRows
				row.insert = table.InsertRows
				definedNoKey = true
			}
			if row.update+row.delete > 0 {
				risks = append(risks, row)
				continue
			}
			if row.insert == 0 {
				row.insert = table.InsertRows
			}
			if row.insert > 0 {
				insertOnly = append(insertOnly, row)
			}
		default:
			unknown = true
		}
	}
	if rowTables == 0 {
		return pkView{}
	}
	sort.Slice(risks, func(i, j int) bool {
		left := risks[i].update + risks[i].delete
		right := risks[j].update + risks[j].delete
		if left != right {
			return left > right
		}
		return risks[i].name() < risks[j].name()
	})
	sort.Slice(insertOnly, func(i, j int) bool {
		return insertOnly[i].name() < insertOnly[j].name()
	})

	var note string
	if unknown {
		if known {
			note = i18n.T("report.text.primaryKeyUnknownSome")
		} else {
			note = i18n.T("report.text.primaryKeyUnknown")
		}
	}
	// The source sentence is true only when the binlog answered nothing: a mixed
	// run has tables whose key came from metadata, so it must not claim one
	// source for all of them (ADR 0031).
	source := ""
	if fromDefinition && !fromBinlog {
		source = i18n.T("report.text.primaryKeyFromDefinition")
		if definedNoKey {
			source = i18n.T("report.text.primaryKeyNoKeyFromDefinition")
		}
		if note == "" {
			note = source
		} else {
			note = source + "; " + note
		}
	}
	if len(risks) > 0 {
		// The summary and the section both name the source (ADR 0031).
		return pkView{summaryLine: source, note: note, risks: risks, insertOnly: insertOnly}
	}
	if len(insertOnly) > 0 {
		line := i18n.Tf("report.text.primaryKeyInsertOnly", map[string]any{
			"Tables": formatInsertOnlyTables(insertOnly),
		})
		if source != "" {
			line = source + "; " + line
		} else if note != "" {
			line = note + "; " + line
		}
		return pkView{summaryLine: line, note: note}
	}
	if note != "" {
		return pkView{summaryLine: note, note: note}
	}
	if note != "" {
		// Both the summary line and the note name the source, so the JSON field
		// and the HTML section say the same thing (ADR 0031).
		return pkView{summaryLine: note, note: note}
	}
	return pkView{summaryLine: i18n.T("report.text.primaryKeyPresent")}
}

func formatInsertOnlyTables(tables []pkTable) string {
	parts := make([]string, len(tables))
	for i, table := range tables {
		parts[i] = fmt.Sprintf("%s (%d INSERT)", table.name(), table.insert)
	}
	return strings.Join(parts, ", ")
}

func tableKeyStatus(table model.TableStats) string {
	if table.InsertRows+table.UpdateRows+table.DeleteRows == 0 {
		return table.KeyStatus
	}
	if table.KeyStatus == "" {
		return model.KeyStatusUnknown
	}
	return table.KeyStatus
}
