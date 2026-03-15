package report

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"binlogviz/internal/model"
)

// RenderText renders an AnalysisResult as human-readable text.
// Sections are always rendered in a fixed order, even if empty.
func RenderText(result model.AnalysisResult) (string, error) {
	var buf strings.Builder

	// Section 1: Workload Summary
	renderWorkloadSummary(&buf, result.Summary)

	// Section 2: Top Tables
	renderTopTables(&buf, result.Tables)

	// Section 3: Top Transactions
	renderTopTransactions(&buf, result.Transactions)

	// Section 4: Minute Activity
	renderMinuteActivity(&buf, result.Minutes)

	// Section 5: Alerts
	renderAlerts(&buf, result.Alerts)

	return buf.String(), nil
}

func renderWorkloadSummary(buf *strings.Builder, summary model.WorkloadSummary) {
	buf.WriteString("=== Workload Summary ===\n")
	buf.WriteString(fmt.Sprintf("  Total Transactions: %d\n", summary.TotalTransactions))
	buf.WriteString(fmt.Sprintf("  Total Rows: %d\n", summary.TotalRows))
	buf.WriteString(fmt.Sprintf("  Total Events: %d\n", summary.TotalEvents))
	buf.WriteString(fmt.Sprintf("  Time Range: %s - %s\n", formatTime(summary.StartTime), formatTime(summary.EndTime)))
	buf.WriteString(fmt.Sprintf("  Duration: %s\n", formatDuration(summary.Duration)))
	buf.WriteString("\n")
}

func renderTopTables(buf *strings.Builder, tables []model.TableStats) {
	buf.WriteString("=== Top Tables ===\n")
	if len(tables) == 0 {
		buf.WriteString("  (no table activity)\n")
	} else {
		for _, t := range tables {
		buf.WriteString(fmt.Sprintf("  %s.%s: %d rows (%d insert, %d update, %d delete, %d txn)\n",
			t.Schema, t.Table, t.TotalRows, t.InsertRows, t.UpdateRows, t.DeleteRows, t.TxnCount))
		}
	}
	buf.WriteString("\n")
}

func renderTopTransactions(buf *strings.Builder, transactions []model.Transaction) {
	buf.WriteString("=== Top Transactions ===\n")
	if len(transactions) == 0 {
		buf.WriteString("  (no transactions)\n")
	} else {
		// Sort by TotalRows descending, with TxnKey ascending as tie-breaker for determinism
		sorted := make([]model.Transaction, len(transactions))
		copy(sorted, transactions)
		sort.Slice(sorted, func(i, j int) bool {
			if sorted[i].TotalRows != sorted[j].TotalRows {
				return sorted[i].TotalRows > sorted[j].TotalRows
			}
			return sorted[i].TxnKey < sorted[j].TxnKey
		})

		for _, txn := range sorted {
		buf.WriteString(fmt.Sprintf("  %s: %d rows in %s (%d events)\n",
			txn.TxnKey, txn.TotalRows, formatDuration(txn.Duration), txn.EventCount))
		}
	}
	buf.WriteString("\n")
}

func renderMinuteActivity(buf *strings.Builder, minutes []model.MinuteBucket) {
	buf.WriteString("=== Minute Activity ===\n")
	if len(minutes) == 0 {
		buf.WriteString("  (no minute activity)\n")
	} else {
		for _, m := range minutes {
		buf.WriteString(fmt.Sprintf("  %s: %d rows, %d txn\n",
			m.Minute.Format("2006-01-02 15:04"), m.TotalRows, m.TxnCount))
		}
	}
	buf.WriteString("\n")
}

func renderAlerts(buf *strings.Builder, alerts []model.Alert) {
	buf.WriteString("=== Alerts ===\n")
	if len(alerts) == 0 {
		buf.WriteString("  (no alerts)\n")
	} else {
		for _, a := range alerts {
		buf.WriteString(fmt.Sprintf("  [%s] %s: %s\n", strings.ToUpper(a.Severity), a.Type, a.Message))
		}
	}
	buf.WriteString("\n")
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "N/A"
	}
	return t.Format("2006-01-02 15:04:05")
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.String()
}

// RenderTextTo writes the text report to the specified writer.
func RenderTextTo(result model.AnalysisResult, w io.Writer) error {
	text, err := RenderText(result)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(w, text)
	return err
}

// RenderTextToStdout writes the text report to stdout.
func RenderTextToStdout(result model.AnalysisResult) error {
	return RenderTextTo(result, os.Stdout)
}
