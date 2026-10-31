// Package report renders the opt-in Statement Fingerprints text section.
// input: the workload-level fingerprint ranking, SQL context mode, and the
// --top-fingerprints display bound.
// output: a ranked section whose entries carry the normalized fingerprint,
// rows/stmts/share/tables, and a bounded sample; explicit notes when no
// Rows_query text was captured or no groups were retained, plus omitted and
// tracker-overflow lines (ADR 0026).
// pos: text renderer for the fingerprint surface beside the patterns section.
// note: if this file changes, update this header and module README.md.
package report

import (
	"fmt"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// renderStatementFingerprints prints the normalized statement ranking.
// An opt-in section is never silently empty: it states whether the binlog
// carried no Rows_query text at all or no retained statements (ADR 0026).
func renderStatementFingerprints(buf *strings.Builder, result model.AnalysisResult, opts Options) {
	buf.WriteString("=== " + i18n.T("report.text.statementFingerprints") + " ===\n")
	if len(result.StatementFingerprints) == 0 {
		if !result.SQLContextAvailable {
			buf.WriteString("  " + i18n.T("report.placeholder.noRowsQueryText") + "\n\n")
		} else {
			buf.WriteString("  " + i18n.T("report.placeholder.noFingerprints") + "\n\n")
		}
		return
	}

	listed := limitFingerprints(result.StatementFingerprints, opts.TopFingerprints)
	for i, fp := range listed {
		fmt.Fprintf(buf, "  %d. %s\n", i+1, model.FormatQuerySummary(fp.Fingerprint, len(fp.Fingerprint)))
		fmt.Fprintf(buf, "     rows=%d stmts=%d share=%.1f%% tables=%s\n",
			fp.Rows, fp.Statements, fp.ShareOfRows*100, delimitedCountMap(fp.Tables))
		if opts.SQLContextMode != SQLContextOff && strings.TrimSpace(fp.Sample) != "" {
			fmt.Fprintf(buf, "     %s: %s\n", i18n.T("report.label.sample"), fp.Sample)
		}
	}
	if omitted := len(result.StatementFingerprints) - len(listed); omitted > 0 {
		buf.WriteString("  " + i18n.Tf("report.text.fingerprintsOmitted", map[string]any{"Count": omitted}) + "\n")
	}
	if result.FingerprintOverflow > 0 {
		buf.WriteString("  " + i18n.Tf("report.text.fingerprintsOverflow", map[string]any{"Count": result.FingerprintOverflow}) + "\n")
	}
	buf.WriteString("\n")
}
