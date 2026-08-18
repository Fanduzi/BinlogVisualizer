// Package report decides when a recorded transaction span is safe to show as a rollback hint.
// input: analyzer-produced Transaction positions and byte counts, including XID-only spans.
// output: formatted locations, plus an XID-only signal so HTML never invents file:pos.
// pos: shared location helper for text next-actions and the HTML incident page.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// maxXIDOnlySpanBytes is the largest recorded span we treat as a commit XID
// rather than a usable transaction start. Dogfood #18 records a 31-byte XID.
const maxXIDOnlySpanBytes = 64

func recordedTxnSpanBytes(txn model.Transaction) int64 {
	if txn.BinlogBytes > 0 {
		return txn.BinlogBytes
	}
	if txn.PositionStart > 0 && txn.PositionEnd > txn.PositionStart {
		return txn.PositionEnd - txn.PositionStart
	}
	return 0
}

// isXIDOnlyTxnSpan reports that the analyzer only stored a commit-XID interval.
// It does not invent a real start position; it only withholds a rollback hint.
func isXIDOnlyTxnSpan(txn model.Transaction) bool {
	span := recordedTxnSpanBytes(txn)
	if span <= 0 || span > maxXIDOnlySpanBytes {
		return false
	}
	return txn.EventCount > 1 || txn.TotalRows > 1
}

func usableRollbackLocation(txn model.Transaction) string {
	if isXIDOnlyTxnSpan(txn) {
		return ""
	}
	if txn.BinlogPathStart == "" && txn.PositionStart == 0 && txn.PositionEnd == 0 {
		return ""
	}
	return formatBinlogLocationWithEnd(txn.BinlogPathStart, txn.PositionStart, txn.BinlogPathEnd, txn.PositionEnd)
}

func xidOnlyLocationNote(txn model.Transaction) string {
	if !isXIDOnlyTxnSpan(txn) {
		return ""
	}
	return i18n.Tf("report.html.analyze.xidOnlySpan", map[string]any{
		"Bytes": recordedTxnSpanBytes(txn),
	})
}

func firstUsableRollbackLocation(result model.AnalysisResult) string {
	for _, txn := range [][]model.Transaction{
		result.Diagnostics.LongestTransactions,
		result.Diagnostics.LargestTransactions,
		result.Diagnostics.WidestTransactions,
	} {
		if len(txn) == 0 {
			continue
		}
		if loc := usableRollbackLocation(txn[0]); loc != "" {
			return loc
		}
	}
	if len(result.Diagnostics.DDLEvents) > 0 {
		ddl := result.Diagnostics.DDLEvents[0]
		return formatBinlogLocation(ddl.BinlogPath, ddl.PositionStart, ddl.PositionEnd)
	}
	return ""
}

func formatRecordedTxnLocation(txn model.Transaction) string {
	if txn.BinlogPathStart == "" && txn.PositionStart == 0 && txn.PositionEnd == 0 {
		return ""
	}
	return formatBinlogLocationWithEnd(txn.BinlogPathStart, txn.PositionStart, txn.BinlogPathEnd, txn.PositionEnd)
}
