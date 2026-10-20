// Package report renders replica apply delay in text, Markdown, HTML, and JSON.
// input: diagnostics apply-delay evidence and the report top-N limit.
// output: a replica ranking, a one-line source conclusion, or one unavailable line. JSON omits the object when timestamps are absent.
// pos: shared renderer for the replica apply-delay section.
// note: if this file changes, update this header and module README.md.
package report

import (
	"fmt"
	"strings"
	"time"

	"binlogviz/internal/displaytz"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// visibleApplyDelay trims the ranking to --top. Summary stats stay on the full set.
// A source keeps an empty ranking so reports do not print a table.
func visibleApplyDelay(delay *model.ApplyDelay, topN int) *model.ApplyDelay {
	if delay == nil || delay.Origin != model.ApplyDelayReplica || topN <= 0 || len(delay.Transactions) <= topN {
		return delay
	}
	trimmed := *delay
	trimmed.Transactions = delay.Transactions[:topN]
	return &trimmed
}

func renderApplyDelay(buf *strings.Builder, delay *model.ApplyDelay, topN int, opts Options) {
	buf.WriteString("=== " + i18n.T("report.text.replicaApplyDelay") + " ===\n")
	shown := visibleApplyDelay(delay, topN)
	if shown == nil {
		buf.WriteString("  " + i18n.T("report.text.replicaApplyDelayUnavailable") + "\n\n")
		return
	}
	buf.WriteString("  " + i18n.T("report.text.replicaApplyDelayClock") + "\n")
	if shown.Origin == model.ApplyDelaySource {
		buf.WriteString("  " + i18n.T("report.text.replicaApplyDelaySource") + "\n\n")
		return
	}
	buf.WriteString("  " + i18n.T("report.text.replicaApplyDelayReplica") + "\n")
	buf.WriteString("  " + applyDelayStatsLine(shown, opts) + "\n")
	for _, txn := range shown.Transactions {
		buf.WriteString("  " + formatApplyDelayTxn(txn, opts) + "\n")
		for _, table := range rankedMinuteTables(txn.Tables) {
			buf.WriteString(fmt.Sprintf("    %s  %d\n", table.name, table.rows))
		}
	}
	buf.WriteString("\n")
}

func mdApplyDelay(buf *strings.Builder, delay *model.ApplyDelay, topN int, opts Options) {
	buf.WriteString("## " + i18n.T("report.text.replicaApplyDelay") + "\n\n")
	shown := visibleApplyDelay(delay, topN)
	if shown == nil {
		buf.WriteString(i18n.T("report.text.replicaApplyDelayUnavailable") + "\n\n")
		return
	}
	buf.WriteString(i18n.T("report.text.replicaApplyDelayClock") + "\n\n")
	if shown.Origin == model.ApplyDelaySource {
		buf.WriteString(i18n.T("report.text.replicaApplyDelaySource") + "\n\n")
		return
	}
	buf.WriteString(i18n.T("report.text.replicaApplyDelayReplica") + "\n\n")
	buf.WriteString(applyDelayStatsLine(shown, opts) + "\n\n")
	buf.WriteString("| GTID | " + i18n.T("report.text.ddlTxnStart") + " | " + i18n.T("report.text.replicaApplyDelayOriginal") + " | " + i18n.T("report.text.replicaApplyDelayImmediate") + " | " + i18n.T("report.text.replicaApplyDelayDelay") + " | " + i18n.T("report.label.drivingTables") + " |\n")
	buf.WriteString("|---|---|---|---|---|---|\n")
	for _, txn := range shown.Transactions {
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s |\n",
			escapeMD(txn.GTID),
			escapeMD(ddlTxnStartText(txn.TxnStartPath, txn.TxnStartPos)),
			escapeMD(opts.formatCommitMicros(txn.OriginalCommit)),
			escapeMD(opts.formatCommitMicros(txn.ImmediateCommit)),
			escapeMD(txn.Delay.String()),
			escapeMD(formatDrivingTables(txn.Tables, topN)),
		))
	}
	buf.WriteString("\n")
}

func applyDelayStatsLine(delay *model.ApplyDelay, opts Options) string {
	if delay == nil {
		return ""
	}
	return i18n.Tf("report.text.replicaApplyDelayStats", map[string]any{
		"Max":  delay.Max.String(),
		"P95":  delay.P95.String(),
		"Peak": opts.formatTime(delay.PeakMinute),
	})
}

func formatApplyDelayTxn(txn model.ApplyDelayTxn, opts Options) string {
	parts := make([]string, 0, 5)
	if txn.GTID != "" {
		parts = append(parts, "gtid="+txn.GTID)
	}
	if start := ddlTxnStartText(txn.TxnStartPath, txn.TxnStartPos); start != "" {
		parts = append(parts, start)
	}
	parts = append(parts,
		"original="+opts.formatCommitMicros(txn.OriginalCommit),
		"immediate="+opts.formatCommitMicros(txn.ImmediateCommit),
		"delay="+txn.Delay.String(),
	)
	return strings.Join(parts, "  ")
}

func (o Options) formatCommitMicros(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	if o.Display == nil || o.Display == time.UTC {
		return t.UTC().Format("2006-01-02 15:04:05.000000 UTC")
	}
	return t.In(o.Display).Format("2006-01-02 15:04:05.000000 -07:00")
}

func (o Options) formatCommitJSON(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return displaytz.RFC3339Nano(t, o.Display)
}

func commitMicros(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano() / int64(time.Microsecond))
}

type htmlApplyDelayTxn struct {
	GTID      string
	Start     string
	Original  string
	Immediate string
	Delay     string
	Tables    []htmlTxnTable
}

func buildHTMLApplyDelay(delay *model.ApplyDelay, topN int, opts Options) (unavailable, source bool, stats string, txns []htmlApplyDelayTxn) {
	shown := visibleApplyDelay(delay, topN)
	if shown == nil {
		return true, false, "", nil
	}
	if shown.Origin == model.ApplyDelaySource {
		return false, true, "", nil
	}
	out := make([]htmlApplyDelayTxn, 0, len(shown.Transactions))
	for _, txn := range shown.Transactions {
		item := htmlApplyDelayTxn{
			GTID:      txn.GTID,
			Start:     ddlTxnStartText(txn.TxnStartPath, txn.TxnStartPos),
			Original:  opts.formatCommitMicros(txn.OriginalCommit),
			Immediate: opts.formatCommitMicros(txn.ImmediateCommit),
			Delay:     txn.Delay.String(),
		}
		for _, table := range rankedMinuteTables(txn.Tables) {
			item.Tables = append(item.Tables, htmlTxnTable{Name: table.name, Rows: table.rows})
		}
		out = append(out, item)
	}
	return false, false, applyDelayStatsLine(shown, opts), out
}
