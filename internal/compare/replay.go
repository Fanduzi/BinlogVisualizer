// Package compare preserves replay-ready transaction evidence from analyze reports.
// input: compare InputTransaction values decoded from analyze JSON.
// output: bounded current transaction evidence with the analyze span and replay command.
// pos: shared compare/trend conversion seam before result and HTML rendering.
// note: if this file changes, keep internal/compare/README.md synchronized.
package compare

import (
	"strings"

	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// TransactionEvidenceFor converts analyze transaction fields into replay-ready evidence.
// A supplied command is preserved so MariaDB snapshots retain their binary choice.
func TransactionEvidenceFor(txn InputTransaction) *TransactionEvidence {
	modelTxn := model.Transaction{
		TxnKey:          txn.TxnKey,
		TotalRows:       txn.TotalRows,
		EventCount:      txn.EventCount,
		BinlogBytes:     txn.BinlogBytes,
		BinlogPathStart: txn.BinlogFileStart,
		BinlogPathEnd:   txn.BinlogFileEnd,
		PositionStart:   txn.PosStart,
		PositionEnd:     txn.PosEnd,
	}

	span := report.FormatBinlogSpan(modelTxn)
	generatedCommand := report.FormatReplayCommand(modelTxn, "")
	if txn.TxnKey == "" && span == "" && generatedCommand == "" {
		return nil
	}

	evidence := &TransactionEvidence{
		TxnKey:          txn.TxnKey,
		BinlogFileStart: txn.BinlogFileStart,
		BinlogFileEnd:   txn.BinlogFileEnd,
		PosStart:        txn.PosStart,
		PosEnd:          txn.PosEnd,
		BinlogSpan:      span,
	}
	if generatedCommand != "" {
		evidence.MysqlbinlogCmd = strings.TrimSpace(txn.MysqlbinlogCmd)
		if evidence.MysqlbinlogCmd == "" {
			evidence.MysqlbinlogCmd = generatedCommand
		}
	}
	return evidence
}
