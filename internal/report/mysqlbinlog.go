// Package report builds copy-paste mysqlbinlog commands from recorded transaction spans.
// input: analyzer-produced Transaction positions after real start/end reconstruction.
// output: a mysqlbinlog command string, or empty when the span is XID-only / unusable.
// pos: shared helper for text, JSON, and HTML Copy so #23 never invents --start-position.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"fmt"
	"path/filepath"
	"strings"

	"binlogviz/internal/model"
)

// maxCommitXIDSpanBytes is the largest recorded span treated as a commit XID
// rather than a usable --start-position. Dogfood #18 records a 31-byte XID.
const maxCommitXIDSpanBytes = 64

func txnRecordedSpanBytes(txn model.Transaction) int64 {
	if txn.BinlogBytes > 0 {
		return txn.BinlogBytes
	}
	if txn.PositionStart > 0 && txn.PositionEnd > txn.PositionStart {
		return txn.PositionEnd - txn.PositionStart
	}
	return 0
}

func txnSpanIsXIDOnly(txn model.Transaction) bool {
	span := txnRecordedSpanBytes(txn)
	if span <= 0 || span > maxCommitXIDSpanBytes {
		return false
	}
	return txn.EventCount > 1 || txn.TotalRows > 1
}

func txnSpanUsableForMysqlbinlog(txn model.Transaction) bool {
	if txnSpanIsXIDOnly(txn) {
		return false
	}
	if txn.PositionStart <= 0 || txn.PositionEnd <= txn.PositionStart {
		return false
	}
	if txn.BinlogPathStart == "" {
		return false
	}
	return true
}

func mysqlbinlogCmd(txn model.Transaction) string {
	if !txnSpanUsableForMysqlbinlog(txn) {
		return ""
	}
	startFile := filepath.Base(txn.BinlogPathStart)
	endFile := startFile
	if txn.BinlogPathEnd != "" {
		endFile = filepath.Base(txn.BinlogPathEnd)
	}
	const flags = "--base64-output=DECODE-ROWS -v"
	if startFile == endFile {
		return fmt.Sprintf("mysqlbinlog %s --start-position=%d --stop-position=%d %s",
			flags, txn.PositionStart, txn.PositionEnd, startFile)
	}
	return strings.Join([]string{
		fmt.Sprintf("mysqlbinlog %s --start-position=%d %s", flags, txn.PositionStart, startFile),
		fmt.Sprintf("mysqlbinlog %s --stop-position=%d %s", flags, txn.PositionEnd, endFile),
	}, "\n")
}

func formatTxnEvidenceLocation(txn model.Transaction) string {
	if txn.BinlogPathStart != "" {
		txn.BinlogPathStart = filepath.Base(txn.BinlogPathStart)
	}
	if txn.BinlogPathEnd != "" {
		txn.BinlogPathEnd = filepath.Base(txn.BinlogPathEnd)
	}
	loc := formatSuspiciousLocation(txn)
	if txn.BinlogPathStart == "" && txn.PositionStart == 0 && txn.PositionEnd == 0 {
		return loc
	}
	span := txnRecordedSpanBytes(txn)
	if span <= 0 && txn.TotalRows <= 0 {
		return loc
	}
	parts := make([]string, 0, 2)
	if span > 0 {
		parts = append(parts, formatCompactBytes(span))
	}
	if txn.TotalRows > 0 {
		parts = append(parts, fmt.Sprintf("%d rows", txn.TotalRows))
	}
	if len(parts) == 0 {
		return loc
	}
	return loc + " (" + strings.Join(parts, ", ") + ")"
}

func formatCompactBytes(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%dMB", (n+500_000)/1_000_000)
	case n >= 1000:
		return fmt.Sprintf("%dKB", (n+500)/1000)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
