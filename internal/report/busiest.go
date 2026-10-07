package report

import (
	"fmt"
	"sort"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// minuteTable is one table's row count inside a single minute.
type minuteTable struct {
	name string
	rows int
}

// rankedMinuteTables orders a minute's tables by rows, then name.
// A zero or empty entry is dropped. The order is stable for text, Markdown, and HTML.
func rankedMinuteTables(rows map[string]int) []minuteTable {
	if len(rows) == 0 {
		return nil
	}
	out := make([]minuteTable, 0, len(rows))
	for name, n := range rows {
		if name == "" || n <= 0 {
			continue
		}
		out = append(out, minuteTable{name: name, rows: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].rows != out[j].rows {
			return out[i].rows > out[j].rows
		}
		return out[i].name < out[j].name
	})
	return out
}

func limitMinuteTables(tables []minuteTable, limit int) ([]minuteTable, int) {
	if limit <= 0 || len(tables) <= limit {
		return tables, 0
	}
	return tables[:limit], len(tables) - limit
}

func rowBearingMinutes(minutes []model.MinuteBucket) []model.MinuteBucket {
	if len(minutes) == 0 {
		return nil
	}
	out := make([]model.MinuteBucket, 0, len(minutes))
	for _, minute := range minutes {
		if minute.TotalRows > 0 {
			out = append(out, minute)
		}
	}
	return out
}

func minutesHaveTables(minutes []model.MinuteBucket) bool {
	for _, minute := range minutes {
		if len(rankedMinuteTables(minute.TableRows)) > 0 {
			return true
		}
	}
	return false
}

// formatDrivingTables is the one-line table list for a minute.
// limit caps how many tables are named; the rest become the shared omitted-tables label.
func formatDrivingTables(rows map[string]int, limit int) string {
	shown, omitted := limitMinuteTables(rankedMinuteTables(rows), limit)
	if len(shown) == 0 {
		return ""
	}
	parts := make([]string, len(shown))
	for i, table := range shown {
		parts[i] = fmt.Sprintf("%s %d", table.name, table.rows)
	}
	line := strings.Join(parts, ", ")
	if omitted > 0 {
		line += ", " + omittedTablesLabel(omitted)
	}
	return line
}

func renderBusiestMinutes(buf *strings.Builder, minutes []model.MinuteBucket, topN int) {
	shown := rowBearingMinutes(minutes)
	if len(shown) == 0 {
		return
	}
	if topN > 0 && len(shown) > topN {
		shown = shown[:topN]
	}
	buf.WriteString("=== " + i18n.T("report.text.busiestMinutes") + " ===\n")
	if minutesHaveTables(shown) {
		buf.WriteString("  " + i18n.T("report.text.busiestMinutesLead") + "\n")
	}
	for _, minute := range shown {
		buf.WriteString(fmt.Sprintf("  %s  rows=%d  txns=%d\n",
			formatTime(minute.Minute), minute.TotalRows, minute.TxnCount))
		tables, omitted := limitMinuteTables(rankedMinuteTables(minute.TableRows), topN)
		for _, table := range tables {
			buf.WriteString(fmt.Sprintf("    %s  %d\n", table.name, table.rows))
		}
		if omitted > 0 {
			buf.WriteString("    " + omittedTablesLabel(omitted) + "\n")
		}
	}
	buf.WriteString("\n")
}
