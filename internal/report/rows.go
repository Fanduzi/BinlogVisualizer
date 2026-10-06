// Package report formats bounded row images for text, markdown, and HTML.
// input: transactions that may carry decoded ROW images, plus SQL-context and --show-rows presentation flags.
// output: one-line cell renderings (DELETE before-image, UPDATE changed columns, INSERT after-image) and the notes for missing names or suppressed values.
// pos: shared row-image presentation helper for the analyze renderers.
// note: if this file changes, update this header and module README.md.
package report

import (
	"regexp"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

var rowNumberPattern = regexp.MustCompile(`^-?\d+(\.\d+)?( \(\d+\))?$`)

func showRowValues(opts Options) bool {
	return opts.ShowRows && opts.SQLContextMode != SQLContextOff
}

func rowValuesSuppressed(opts Options) bool {
	return opts.ShowRows && opts.SQLContextMode == SQLContextOff
}

func columnNamesNote(result model.AnalysisResult) string {
	if positionalRowNames(result.Transactions) ||
		positionalRowNames(result.Diagnostics.LargestTransactions) ||
		positionalRowNames(result.Diagnostics.LongestTransactions) ||
		positionalRowNames(result.Diagnostics.WidestTransactions) {
		return i18n.T("report.text.columnNamesMissing")
	}
	return ""
}

func positionalRowNames(txns []model.Transaction) bool {
	for _, txn := range txns {
		for _, image := range txn.RowImages {
			if image.Names != model.RowNamesFull {
				return true
			}
		}
	}
	return false
}

func appendRowImageLines(lines *[]string, txn model.Transaction, opts Options) {
	if !showRowValues(opts) || (len(txn.RowImages) == 0 && txn.RowImagesOmitted == 0) {
		return
	}
	for _, image := range txn.RowImages {
		*lines = append(*lines, "    "+formatRowImageHeader(image))
		*lines = append(*lines, formatRowImageCells(image)...)
	}
	if txn.RowImagesOmitted > 0 {
		*lines = append(*lines, "    "+i18n.Tf("report.text.rowsOmitted", map[string]any{"Count": txn.RowImagesOmitted}))
	}
}

func formatRowImageBlock(txn model.Transaction, opts Options) string {
	var lines []string
	appendRowImageLines(&lines, txn, opts)
	return strings.Join(lines, "\n")
}

func formatRowImageHeader(image model.RowImage) string {
	name := image.Table
	if image.Schema != "" && image.Table != "" {
		name = image.Schema + "." + image.Table
	}
	if name == "" {
		return image.Op
	}
	return image.Op + " " + name
}

func formatRowImageCells(image model.RowImage) []string {
	if image.Op == "UPDATE" {
		return formatUpdateCells(image)
	}
	cells := image.After
	if image.Op == "DELETE" {
		cells = image.Before
	}
	lines := make([]string, 0, len(image.Columns))
	for i, column := range image.Columns {
		cell := cellAt(cells, i)
		lines = append(lines, "      "+column+"="+quoteCell(cell))
	}
	return lines
}

func formatUpdateCells(image model.RowImage) []string {
	changed := map[string]struct{}{}
	for _, column := range image.Changed {
		changed[column] = struct{}{}
	}
	lines := make([]string, 0, len(image.Changed)+1)
	unchanged := 0
	for i, column := range image.Columns {
		if _, ok := changed[column]; !ok {
			unchanged++
			continue
		}
		before := quoteCell(cellAt(image.Before, i))
		after := quoteCell(cellAt(image.After, i))
		lines = append(lines, "      "+column+": "+before+" -> "+after)
	}
	if len(lines) == 0 {
		lines = append(lines, "      "+i18n.T("report.text.rowUnchanged"))
	} else if unchanged > 0 {
		lines = append(lines, "      "+i18n.Tf("report.text.unchangedColumns", map[string]any{"Count": unchanged}))
	}
	return lines
}

func cellAt(cells []model.RowCell, index int) model.RowCell {
	if index < 0 || index >= len(cells) {
		return model.RowCell{Null: true}
	}
	return cells[index]
}

func quoteCell(cell model.RowCell) string {
	if cell.Null {
		return "NULL"
	}
	if rowNumberPattern.MatchString(cell.Text) || strings.HasPrefix(cell.Text, "0x") {
		return cell.Text
	}
	replacer := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + replacer.Replace(cell.Text) + "'"
}

func dmlFilterLabel(scope *model.SnapshotFilters) string {
	if scope == nil || len(scope.IncludeDML) == 0 {
		return ""
	}
	return strings.Join(scope.IncludeDML, ", ")
}
