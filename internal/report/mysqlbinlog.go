// Package report builds copy-paste mysqlbinlog commands from recorded transaction spans.
// input: analyzer-produced Transaction positions after real start/end reconstruction, plus Format Description server version.
// output: a mysqlbinlog or mariadb-binlog command with absolute file paths, or empty when the span is XID-only / unusable; shared span formatting for downstream reports.
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

func mysqlbinlogCmd(txn model.Transaction, serverVersion string) string {
	if !txnSpanUsableForMysqlbinlog(txn) {
		return ""
	}
	startFile := replayFileArg(txn.BinlogPathStart)
	endFile := startFile
	if txn.BinlogPathEnd != "" {
		endFile = replayFileArg(txn.BinlogPathEnd)
	}
	binary := replayBinlogBinary(serverVersion)
	const flags = "--base64-output=DECODE-ROWS -v"
	if startFile == endFile {
		return fmt.Sprintf("%s %s --start-position=%d --stop-position=%d %s",
			binary, flags, txn.PositionStart, txn.PositionEnd, startFile)
	}
	return strings.Join([]string{
		fmt.Sprintf("%s %s --start-position=%d %s", binary, flags, txn.PositionStart, startFile),
		fmt.Sprintf("%s %s --stop-position=%d %s", binary, flags, txn.PositionEnd, endFile),
	}, "\n")
}

// FormatReplayCommand returns the analyze-compatible replay command for a transaction span.
func FormatReplayCommand(txn model.Transaction, serverVersion string) string {
	return mysqlbinlogCmd(txn, serverVersion)
}

func replayBinlogBinary(serverVersion string) string {
	if strings.Contains(strings.ToLower(serverVersion), "mariadb") {
		return "mariadb-binlog"
	}
	return "mysqlbinlog"
}

func replayFileArg(path string) string {
	if path == "" {
		return ""
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

// FormatBinlogSpan returns the analyze-compatible display span for a transaction.
func FormatBinlogSpan(txn model.Transaction) string {
	return formatBinlogSpan(txn)
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
