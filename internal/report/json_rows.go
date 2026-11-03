// Package report renders the row-image part of the analyze JSON document.
// pos: jsonRowImage and its converters, split out of json.go to keep it inside
// the file ceiling.
// note: if this file changes, update this header and module README.md.
package report

import (
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

type jsonRowImage struct {
	Schema  string   `json:"schema,omitempty"`
	Table   string   `json:"table,omitempty"`
	Op      string   `json:"op"`
	Columns []string `json:"columns"`
	Names   string   `json:"names"`
	// NamesFrom is set when the labels came from --schema-file (ADR 0030).
	NamesFrom string `json:"names_from,omitempty"`
	// NamesNote explains a definition that covers the table but does not fit
	// this row, so the labels fell back to @1..N (ADR 0031).
	NamesNote string   `json:"names_note,omitempty"`
	Before    []any    `json:"before,omitempty"`
	After     []any    `json:"after,omitempty"`
	Changed   []string `json:"changed,omitempty"`
}

func convertRowImages(images []model.RowImage) []jsonRowImage {
	if len(images) == 0 {
		return nil
	}
	out := make([]jsonRowImage, len(images))
	for i, image := range images {
		out[i] = jsonRowImage{
			Schema:    image.Schema,
			Table:     image.Table,
			Op:        image.Op,
			Columns:   append([]string(nil), image.Columns...),
			Names:     image.Names,
			NamesFrom: image.NamesFrom,
			NamesNote: rowImageNamesNote(image),
			Before:    jsonCells(image.Before),
			After:     jsonCells(image.After),
			Changed:   append([]string(nil), image.Changed...),
		}
	}
	return out
}

func jsonCells(cells []model.RowCell) []any {
	if cells == nil {
		return nil
	}
	out := make([]any, len(cells))
	for i, cell := range cells {
		if cell.Null {
			out[i] = nil
			continue
		}
		out[i] = cell.Text
	}
	return out
}

// rowImageNamesNote renders why a definition did not name this row.
func rowImageNamesNote(image model.RowImage) string {
	if image.NamesFileCols == 0 {
		return ""
	}
	return i18n.Tf("report.text.columnNamesMismatch", map[string]any{"File": image.NamesFileCols, "Row": image.NamesRowCols})
}
