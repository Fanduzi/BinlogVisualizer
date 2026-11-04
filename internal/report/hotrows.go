// Package report renders the Hot Rows ranking in every analyze format.
package report

import (
	"fmt"
	"path/filepath"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

func limitHotRows(rows []model.HotRow, limit int) ([]model.HotRow, int) {
	if limit <= 0 || len(rows) <= limit {
		return rows, 0
	}
	return rows[:limit], len(rows) - limit
}

func hotRowKeysHidden(opts Options) bool {
	return opts.SQLContextMode == SQLContextOff
}

func hotRowHasSection(report model.HotRowReport) bool {
	return len(report.Rows) > 0 || len(report.Unavailable) > 0 || report.Overflow
}

func hotRowTable(row model.HotRow) string {
	return row.Schema + "." + row.Table
}

func hotRowGapTable(gap model.HotRowGap) string {
	return gap.Schema + "." + gap.Table
}

func hotRowKey(row model.HotRow, opts Options) string {
	if hotRowKeysHidden(opts) {
		return ""
	}
	return row.PrimaryKey
}

func hotRowTxnPlace(gtid, file string, pos int64) string {
	base := ""
	if file != "" {
		base = filepath.Base(file)
	}
	loc := ""
	if base != "" && pos > 0 {
		loc = fmt.Sprintf("%s:%d", base, pos)
	} else if pos > 0 {
		loc = fmt.Sprintf("%d", pos)
	}
	switch {
	case gtid != "" && loc != "":
		return gtid + " " + loc
	case gtid != "":
		return gtid
	default:
		return loc
	}
}

func hotRowUnavailableLine(gap model.HotRowGap) string {
	key := "report.text.hotRowsUnavailable"
	if gap.Reason == model.HotRowReasonValues {
		key = "report.text.hotRowsUnavailableValues"
	}
	return i18n.Tf(key, map[string]any{"Table": hotRowGapTable(gap)})
}

func hotRowOverflowLine(limit int) string {
	if limit <= 0 {
		limit = 8192
	}
	return i18n.Tf("report.text.hotRowsOverflow", map[string]any{"Limit": limit})
}

func renderHotRows(buf *strings.Builder, report model.HotRowReport, opts Options) {
	if !hotRowHasSection(report) {
		return
	}
	buf.WriteString("=== " + i18n.T("report.text.hotRows") + " ===\n")
	buf.WriteString("  " + i18n.T("report.text.hotRowsLead") + "\n")
	if report.KeySource == model.HotRowKeySourceSchemaFile {
		buf.WriteString("  " + i18n.T("report.text.hotRowsFromSchema") + "\n")
	}
	rows, omitted := limitHotRows(report.Rows, opts.TopRows)
	if hotRowKeysHidden(opts) && len(rows) > 0 {
		buf.WriteString("  " + i18n.T("report.text.hotRowsHidden") + "\n")
	}
	for i, row := range rows {
		title := fmt.Sprintf("  %d. %s", i+1, hotRowTable(row))
		if key := hotRowKey(row, opts); key != "" {
			title += " " + key
		}
		if row.KeySource == model.HotRowKeySourceSchemaFile {
			title += " [--schema-file]"
		}
		buf.WriteString(title + "\n")
		line := fmt.Sprintf("     %s=%d  %s=%d",
			i18n.T("report.text.hotRowsTouches"), row.Touches,
			i18n.T("report.text.hotRowsTransactions"), row.Transactions)
		if row.Approximate {
			line += "  " + i18n.T("report.text.hotRowsApproximate")
		}
		buf.WriteString(line + "\n")
		fmt.Fprintf(buf, "     %s %s  %s\n",
			i18n.T("report.text.hotRowsFirst"), opts.formatTime(row.FirstTime),
			hotRowTxnPlace(row.FirstGTID, row.FirstFile, row.FirstPos))
		fmt.Fprintf(buf, "     %s %s  %s\n",
			i18n.T("report.text.hotRowsLast"), opts.formatTime(row.LastTime),
			hotRowTxnPlace(row.LastGTID, row.LastFile, row.LastPos))
	}
	if omitted > 0 {
		buf.WriteString("  " + i18n.Tf("report.text.hotRowsOmitted", map[string]any{"Count": omitted}) + "\n")
	}
	for _, gap := range report.Unavailable {
		buf.WriteString("  " + hotRowUnavailableLine(gap) + "\n")
	}
	if report.Overflow {
		buf.WriteString("  " + hotRowOverflowLine(report.TrackLimit) + "\n")
	}
	buf.WriteString("\n")
}

func mdHotRows(buf *strings.Builder, report model.HotRowReport, opts Options) {
	if !hotRowHasSection(report) {
		return
	}
	buf.WriteString("## " + i18n.T("report.text.hotRows") + "\n\n")
	buf.WriteString(i18n.T("report.text.hotRowsLead") + "\n\n")
	if report.KeySource == model.HotRowKeySourceSchemaFile {
		buf.WriteString(i18n.T("report.text.hotRowsFromSchema") + "\n\n")
	}
	rows, omitted := limitHotRows(report.Rows, opts.TopRows)
	if hotRowKeysHidden(opts) && len(rows) > 0 {
		buf.WriteString(i18n.T("report.text.hotRowsHidden") + "\n\n")
	}
	if len(rows) > 0 {
		buf.WriteString("| # | Table | Primary key | Touches | Transactions | First | Last | First transaction | Last transaction |\n")
		buf.WriteString("|---|---|---|---:|---:|---|---|---|---|\n")
		for i, row := range rows {
			key := hotRowKey(row, opts)
			if key == "" {
				key = i18n.T("report.text.hotRowsHiddenKey")
			}
			if row.KeySource == model.HotRowKeySourceSchemaFile {
				key += " [--schema-file]"
			}
			touches := fmt.Sprintf("%d", row.Touches)
			if row.Approximate {
				touches += " " + i18n.T("report.text.hotRowsApproximate")
			}
			fmt.Fprintf(buf, "| %d | %s | %s | %s | %d | %s | %s | %s | %s |\n",
				i+1,
				mdCell(hotRowTable(row)),
				mdCell(key),
				touches,
				row.Transactions,
				mdCell(opts.formatTime(row.FirstTime)),
				mdCell(opts.formatTime(row.LastTime)),
				mdCell(hotRowTxnPlace(row.FirstGTID, row.FirstFile, row.FirstPos)),
				mdCell(hotRowTxnPlace(row.LastGTID, row.LastFile, row.LastPos)))
		}
		buf.WriteString("\n")
	}
	if omitted > 0 {
		buf.WriteString(i18n.Tf("report.text.hotRowsOmitted", map[string]any{"Count": omitted}) + "\n\n")
	}
	for _, gap := range report.Unavailable {
		buf.WriteString(hotRowUnavailableLine(gap) + "\n\n")
	}
	if report.Overflow {
		buf.WriteString(hotRowOverflowLine(report.TrackLimit) + "\n\n")
	}
}
