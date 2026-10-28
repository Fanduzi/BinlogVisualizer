package flashback

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/redact"
	"binlogviz/internal/schemacatalog"
)

// GenerationRefusal is an exit-6 flashback refusal with a stable reason.
// Table is set when the refusal names one table.
type GenerationRefusal struct {
	Code  string
	Table string
	Err   error
}

func (e *GenerationRefusal) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *GenerationRefusal) Unwrap() error { return e.Err }

func refuse(code, table string, err error) error {
	if err == nil {
		return nil
	}
	return &GenerationRefusal{Code: code, Table: table, Err: err}
}

// RefusalDetails reports the stable reason of a generation refusal.
func RefusalDetails(err error) (reason, table string, ok bool) {
	var gen *GenerationRefusal
	if errors.As(err, &gen) && gen != nil {
		return gen.Code, gen.Table, true
	}
	var row *generatedRowRefusal
	if errors.As(err, &row) && row != nil {
		return "generated_row", row.shownTable(), true
	}
	return "", "", false
}

// flashRowError is the refusal for one row the binlog layer could not
// capture. flavor is the binlog's server flavor; a MariaDB binlog names the
// MariaDB setting.
func flashRowError(row model.FlashRow, table, flavor string) error {
	switch row.ProblemKind {
	case model.FlashProblemNames:
		key := "error.flashbackNames"
		if strings.EqualFold(strings.TrimSpace(flavor), "mariadb") {
			key = "error.flashbackNamesMariaDB"
		}
		return refuse("row_metadata", table, fmt.Errorf("%s", i18n.Tf(key, map[string]any{"Table": table})))
	case model.FlashProblemImage:
		return refuse("row_image", table, fmt.Errorf("%s", i18n.Tf("error.flashbackImage", map[string]any{"Table": table})))
	case model.FlashProblemType:
		column := row.ProblemColumn
		if column == "" {
			column = "?"
		}
		// The column follows the table name, so the plain binlog name is kept.
		named := flashTable(row.Schema, row.Table)
		return refuse("column_type", named, fmt.Errorf("%s", i18n.Tf("error.flashbackType", map[string]any{
			"Table":  named,
			"Column": column,
			"Type":   row.ProblemType,
		})))
	default:
		return refuse("row_capture", table, fmt.Errorf("%s", i18n.Tf("error.flashbackCapture", map[string]any{"Table": table})))
	}
}

// ddlRefusal decides a DDL inside the selected range (ADR 0015). With no
// table or schema filter every DDL refuses. With a filter, the DDL refuses
// unless its whole text was read and none of the tables or schemas it names,
// as a source or a target, could hold a selected row.
func (a *Planner) ddlRefusal(ev model.NormalizedEvent) error {
	sel := a.opts.Selection
	if sel == nil {
		label := a.refusalTable(ev.Schema, ev.Table)
		table := ""
		if ev.Table != "" {
			table = label
		}
		if names := a.dropNames(ev); len(names) > 0 {
			return a.refuseDrop(ev, names)
		}
		if names := a.userNames(ev); len(names) > 0 {
			return a.refuseDrop(ev, names)
		}
		return refuse("ddl_in_range", table, flashDDLError(ev, escapeControls(label)))
	}
	// QuerySchema is the default database of the query event. Empty means
	// it is not known, and an unqualified name then matches any schema.
	var refs []ddlRef
	ok := !ev.QueryTruncated
	if ok {
		refs, ok = ddlReferences(ev.QuerySQL, ev.QuerySchema, ev.Schema, mysqlexpr.EventDialect(ev.ServerVersion, ev.ServerFlavor, ev.SQLMode))
	}
	if !ok {
		return refuse("ddl_in_range", "", flashDDLUnreadError(ev))
	}
	for _, ref := range refs {
		if ref.table == "" {
			if sel.MayHoldSchema(ref.schema) {
				label := ref.schema
				if label == "" {
					label = "?"
				}
				return refuse("ddl_in_range", "", flashDDLError(ev, escapeControls(label)))
			}
			continue
		}
		if sel.MayHoldTable(ref.schema, ref.table) {
			if names := a.dropNames(ev); len(names) > 0 {
				return a.refuseDrop(ev, names)
			}
			label := a.refusalTable(ref.schema, ref.table)
			return refuse("ddl_in_range", label, flashDDLError(ev, escapeControls(label)))
		}
	}
	return nil
}

// refuseDrop puts every name in details.table. The message shows five names
// and "and N more", with control characters escaped.
func (a *Planner) refuseDrop(ev model.NormalizedEvent, names []string) error {
	return refuse("ddl_in_range", strings.Join(names, ", "), flashDDLError(ev, shortDDLNames(names)))
}

const ddlNamesShown = 5

func shortDDLNames(names []string) string {
	n := len(names)
	if n == 0 {
		return ""
	}
	end := n
	if end > ddlNamesShown {
		end = ddlNamesShown
	}
	var b strings.Builder
	for i := 0; i < end; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(escapeControls(names[i]))
	}
	if n > ddlNamesShown {
		b.WriteByte(' ')
		b.WriteString(i18n.Tf("error.flashbackDDLAndMore", map[string]any{"Count": n - ddlNamesShown}))
	}
	return b.String()
}

func escapeControls(s string) string {
	if !nameNeedsEscape(s) {
		return s
	}
	q := strconv.QuoteToASCII(s)
	return q[1 : len(q)-1]
}

func nameNeedsEscape(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f || unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) {
			return true
		}
	}
	return false
}

func flashDDLUnreadError(ev model.NormalizedEvent) error {
	statement := strings.TrimSpace(redact.Statement(ev.QuerySQL))
	if statement == "" {
		statement = "DDL"
	}
	statement = mysqlexpr.OneLine(statement, 120)
	return fmt.Errorf("%s", i18n.Tf("error.flashbackDDLUnread", map[string]any{
		"Statement": statement,
	}))
}

func flashDDLError(ev model.NormalizedEvent, table string) error {
	statement := strings.TrimSpace(redact.Statement(ev.QuerySQL))
	if statement == "" {
		statement = "DDL"
	}
	statement = mysqlexpr.OneLine(statement, 120)
	return fmt.Errorf("%s", i18n.Tf("error.flashbackDDL", map[string]any{
		"Table":     table,
		"Statement": statement,
	}))
}

func (e *generatedRowRefusal) shownTable() string {
	if e == nil {
		return ""
	}
	if e.label != "" {
		return e.label
	}
	return flashTable(e.schema, e.table)
}

func flashTable(schema, table string) string {
	switch {
	case schema != "" && table != "":
		return schema + "." + table
	case table != "":
		return table
	case schema != "":
		return schema
	default:
		return "selected range"
	}
}

// generatedRowRefusal is a row with no base column to write. It names the
// binlog table; FlashbackSQL re-renders it with the catalog label.
type generatedRowRefusal struct {
	schema, table string
	label         string
}

func (e *generatedRowRefusal) Error() string {
	label := e.label
	if label == "" {
		label = flashTable(e.schema, e.table)
	}
	return i18n.Tf("error.flashbackGeneratedRow", map[string]any{"Table": label})
}

func (e *generatedRowRefusal) named(label string) error {
	copied := *e
	copied.label = label
	return &copied
}

// refusalTable names schema.table in a refusal, from its current catalog binding.
func (a *Planner) refusalTable(schema, table string) string {
	return refusalLabel(a.flashCat.Peek(schema, table), schema, table)
}

// refusalLabel is the table name every refusal prints. A table bound by a
// letter-case fold also names the schema-file table, in every language and on
// every refusal line (#225).
func refusalLabel(res schemacatalog.Resolution, schema, table string) string {
	name := flashTable(schema, table)
	if res.FileTable == "" {
		return name
	}
	return i18n.Tf("error.flashbackSchemaFoldLabel", map[string]any{"Binlog": name, "File": res.FileTable})
}
