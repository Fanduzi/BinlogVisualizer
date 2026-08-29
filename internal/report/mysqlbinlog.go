// Package report builds copy-paste mysqlbinlog commands from trusted full-transaction replay spans.
// input: analyzer-produced Transaction replay metadata plus per-transaction/report Format Description server-version evidence.
// output: a provenance-correct full-transaction mysqlbinlog or mariadb-binlog command, or empty when replay is untrusted/unusable, plus retained span formatting.
// pos: shared helper for text, JSON, and HTML Copy so #23 never invents --start-position.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"fmt"
	"path/filepath"
	"strings"

	"binlogviz/internal/model"
)

func txnRecordedSpanBytes(txn model.Transaction) int64 {
	if txn.BinlogBytes > 0 {
		return txn.BinlogBytes
	}
	if txn.PositionStart > 0 && txn.PositionEnd > txn.PositionStart {
		return txn.PositionEnd - txn.PositionStart
	}
	return 0
}

func txnSpanUsableForMysqlbinlog(txn model.Transaction) bool {
	return txn.FullReplayAvailable()
}

func txnReplayAvailable(txn model.Transaction) bool {
	return txnSpanUsableForMysqlbinlog(txn)
}

func mysqlbinlogCmd(txn model.Transaction, serverVersion string) string {
	if !txnSpanUsableForMysqlbinlog(txn) {
		return ""
	}
	span := txn.FullReplaySpan
	if txn.ServerVersion != "" {
		serverVersion = txn.ServerVersion
	}
	binary := replayBinlogBinary(serverVersion)
	const flags = "--base64-output=DECODE-ROWS -v"
	return fmt.Sprintf("%s %s --start-position=%d --stop-position=%d %s",
		binary, flags, span.PositionStart, span.PositionEnd, replayFileArg(span.BinlogPathStart))
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
