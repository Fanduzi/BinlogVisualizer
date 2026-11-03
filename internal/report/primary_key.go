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
	// fromDefinition marks a row whose key answer came from --schema-file, so
	// the reader can see which tables the dump is speaking for (ADR 0031).
	fromDefinition bool
}

func (t pkTable) name() string {
	name := t.table
	switch {
	case t.schema != "" && t.table != "":
		name = t.schema + "." + t.table
	case t.table == "":
		name = t.schema
	}
	if t.fromDefinition {
		name += " [--schema-file]"
	}
	return name
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
	marked := false
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
				row.fromDefinition = true
			}
			marked = marked || row.fromDefinition
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
	unknownNote := ""
	if unknown {
		if known {
			unknownNote = i18n.T("report.text.primaryKeyUnknownSome")
		} else {
			unknownNote = i18n.T("report.text.primaryKeyUnknown")
		}
	}
	// A run states one source only when the definition answered every table:
	// under partial coverage a source claim is false for the tables it does not
	// cover, and the unknown wording stays (ADR 0031).
	source := ""
	if fromDefinition && !fromBinlog && !unknown {
		source = i18n.T("report.text.primaryKeyFromDefinition")
	}
	note = joinNote(unknownNote, source)
	if marked {
		note = joinNote(note, i18n.T("report.text.primaryKeyMarkedFromDefinition"))
	}
	if len(risks) > 0 {
		// The summary repeats the unknown warning when coverage is partial; a
		// source claim would be false for the uncovered tables (ADR 0031).
		summary := source
		if summary == "" {
			summary = unknownNote
		}
		return pkView{summaryLine: summary, note: note, risks: risks, insertOnly: insertOnly}
	}
	if len(insertOnly) > 0 {
		line := i18n.Tf("report.text.primaryKeyInsertOnly", map[string]any{
			"Tables": formatInsertOnlyTables(insertOnly),
		})
		return pkView{summaryLine: joinNote(note, line), note: note}
	}
	if note != "" {
		// The summary line, the JSON field, and the HTML section say the same
		// thing (ADR 0031).
		return pkView{summaryLine: note, note: note}
	}
	return pkView{summaryLine: i18n.T("report.text.primaryKeyPresent")}
}

// joinNote appends sentence to note, keeping the note readable.
func joinNote(note, sentence string) string {
	switch {
	case sentence == "":
		return note
	case note == "":
		return sentence
	default:
		return note + "; " + sentence
	}
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
