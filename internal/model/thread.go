// Package model defines session-level write rankings for analyze reports.
// input: per-transaction thread, server, actor, schema, and workload totals.
// output: ThreadStats ranked by rows, events, bytes, or transactions.
// pos: shared result-model layer between streaming aggregation and report renderers.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

// Ranking keys for Top Threads. Rows win when any session wrote rows.
const (
	ThreadRankRows         = "rows"
	ThreadRankEvents       = "events"
	ThreadRankBytes        = "bytes"
	ThreadRankTransactions = "transactions"
)

// ThreadStats is one session that wrote in the analyzed window.
// ThreadID is the stable MySQL thread id. ServerID, actor, and schema are set
// only when the binlog carried them. Share is the fraction of the ranking metric.
type ThreadStats struct {
	ThreadID    uint32
	ServerID    uint32
	ActorUser   string
	ActorHost   string
	Schema      string
	Schemas     []string
	TotalRows   int
	EventCount  int
	TxnCount    int
	BinlogBytes int64
	Share       float64
	ShareOfRows float64
}
