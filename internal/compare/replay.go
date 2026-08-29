// Package compare preserves transaction evidence from analyze reports.
// input: compare InputTransaction values decoded from analyze JSON.
// output: bounded evidence with retained spans and only explicitly trusted full-transaction replay commands.
// pos: shared compare/trend conversion seam before result and HTML rendering.
// note: if this file changes, keep internal/compare/README.md synchronized.
package compare

import (
	"strings"

	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// TransactionEvidenceFor converts analyze transaction fields into report evidence.
// A supplied command is preserved only when report v3 says it was built from a
// trusted FullReplaySpan; retained or legacy spans are evidence, not replay input.
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
	if txn.TxnKey == "" && span == "" && txn.Completeness == "" && txn.ReplayAvailable == nil {
		return nil
	}

	evidence := &TransactionEvidence{
		TxnKey:          txn.TxnKey,
		BinlogFileStart: txn.BinlogFileStart,
		BinlogFileEnd:   txn.BinlogFileEnd,
		PosStart:        txn.PosStart,
		PosEnd:          txn.PosEnd,
		BinlogSpan:      span,
		Completeness:    txn.Completeness,
		ReplayAvailable: txn.ReplayAvailable,
		ReplayScope:     txn.ReplayScope,
	}
	if txn.ReplayAvailable != nil && *txn.ReplayAvailable && txn.ReplayScope == "full_transaction" {
		evidence.MysqlbinlogCmd = strings.TrimSpace(txn.MysqlbinlogCmd)
	}
	return evidence
}
