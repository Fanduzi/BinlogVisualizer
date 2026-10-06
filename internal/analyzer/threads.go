// Package analyzer ranks sessions while transactions stream through the report aggregator.
// input: retained transactions that carry thread id, server id, actor, schema, rows, events, and bytes.
// output: one ThreadStats per session, ordered by rows when any session wrote rows, otherwise by events, bytes, or transactions.
// pos: streaming session rollup used by ReportAggregator; it does not retain every transaction.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"sort"
	"strings"

	"binlogviz/internal/model"
)

type sessionKey struct {
	serverID uint32
	threadID uint32
	user     string
	host     string
}

type threadAccum struct {
	serverID  uint32
	threadID  uint32
	user      string
	host      string
	actorRows int
	schemas   map[string]int
	rows      int
	events    int
	txns      int
	bytes     int64
}

type threadAggregator struct {
	byKey map[sessionKey]*threadAccum
}

func (a *threadAggregator) add(txn model.Transaction) {
	key, ok := sessionKeyOf(txn)
	if !ok {
		return
	}
	if a.byKey == nil {
		a.byKey = make(map[sessionKey]*threadAccum)
	}
	acc := a.byKey[key]
	if acc == nil {
		acc = &threadAccum{serverID: txn.ServerID, threadID: txn.ThreadID}
		a.byKey[key] = acc
	}
	acc.rows += txn.TotalRows
	acc.events += txn.EventCount
	acc.txns++
	acc.bytes += txn.BinlogBytes
	acc.noteActor(txn)
	acc.noteSchemas(txn.Tables)
}

func sessionKeyOf(txn model.Transaction) (sessionKey, bool) {
	if txn.ThreadID != 0 {
		return sessionKey{serverID: txn.ServerID, threadID: txn.ThreadID}, true
	}
	if txn.ActorUser == "" && txn.ActorHost == "" {
		return sessionKey{}, false
	}
	return sessionKey{serverID: txn.ServerID, user: txn.ActorUser, host: txn.ActorHost}, true
}

func (a *threadAccum) noteActor(txn model.Transaction) {
	if txn.ActorUser == "" && txn.ActorHost == "" {
		return
	}
	if a.user == "" && a.host == "" {
		a.user = txn.ActorUser
		a.host = txn.ActorHost
		a.actorRows = txn.TotalRows
		return
	}
	if txn.ActorUser == a.user && txn.ActorHost == a.host {
		if txn.TotalRows > a.actorRows {
			a.actorRows = txn.TotalRows
		}
		return
	}
	if txn.TotalRows > a.actorRows {
		a.user = txn.ActorUser
		a.host = txn.ActorHost
		a.actorRows = txn.TotalRows
	}
}

func (a *threadAccum) noteSchemas(tables map[string]int) {
	if len(tables) == 0 {
		return
	}
	for key, rows := range tables {
		schema := schemaOfTableKey(key)
		if schema == "" {
			continue
		}
		if a.schemas == nil {
			a.schemas = make(map[string]int)
		}
		a.schemas[schema] += rows
	}
}

func schemaOfTableKey(key string) string {
	schema, _, ok := strings.Cut(key, ".")
	if !ok || schema == "" || schema == key {
		return ""
	}
	return schema
}

func (a threadAggregator) snapshot() ([]model.ThreadStats, string) {
	if len(a.byKey) == 0 {
		return nil, ""
	}
	out := make([]model.ThreadStats, 0, len(a.byKey))
	for _, acc := range a.byKey {
		out = append(out, acc.stats())
	}
	rankedBy := chooseThreadRank(out)
	sort.Slice(out, func(i, j int) bool {
		return threadLess(out[i], out[j], rankedBy)
	})
	var rankTotal int64
	var rowTotal int64
	for _, item := range out {
		rankTotal += threadRankValue(item, rankedBy)
		rowTotal += int64(item.TotalRows)
	}
	for i := range out {
		if rankTotal > 0 {
			out[i].Share = float64(threadRankValue(out[i], rankedBy)) / float64(rankTotal)
		}
		if rowTotal > 0 {
			out[i].ShareOfRows = float64(out[i].TotalRows) / float64(rowTotal)
		}
	}
	return out, rankedBy
}

func (a *threadAccum) stats() model.ThreadStats {
	schemas := schemaNames(a.schemas)
	schema := ""
	if len(schemas) > 0 {
		schema = schemas[0]
	}
	return model.ThreadStats{
		ThreadID:    a.threadID,
		ServerID:    a.serverID,
		ActorUser:   a.user,
		ActorHost:   a.host,
		Schema:      schema,
		Schemas:     schemas,
		TotalRows:   a.rows,
		EventCount:  a.events,
		TxnCount:    a.txns,
		BinlogBytes: a.bytes,
	}
}

func schemaNames(counts map[string]int) []string {
	if len(counts) == 0 {
		return nil
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if counts[names[i]] != counts[names[j]] {
			return counts[names[i]] > counts[names[j]]
		}
		return names[i] < names[j]
	})
	return names
}

func chooseThreadRank(items []model.ThreadStats) string {
	var rows, events, bytes, txns int64
	for _, item := range items {
		rows += int64(item.TotalRows)
		events += int64(item.EventCount)
		bytes += item.BinlogBytes
		txns += int64(item.TxnCount)
	}
	switch {
	case rows > 0:
		return model.ThreadRankRows
	case events > 0:
		return model.ThreadRankEvents
	case bytes > 0:
		return model.ThreadRankBytes
	default:
		return model.ThreadRankTransactions
	}
}

func threadRankValue(item model.ThreadStats, rankedBy string) int64 {
	switch rankedBy {
	case model.ThreadRankEvents:
		return int64(item.EventCount)
	case model.ThreadRankBytes:
		return item.BinlogBytes
	case model.ThreadRankTransactions:
		return int64(item.TxnCount)
	default:
		return int64(item.TotalRows)
	}
}

func threadLess(left, right model.ThreadStats, rankedBy string) bool {
	lv := threadRankValue(left, rankedBy)
	rv := threadRankValue(right, rankedBy)
	if lv != rv {
		return lv > rv
	}
	if left.TotalRows != right.TotalRows {
		return left.TotalRows > right.TotalRows
	}
	if left.EventCount != right.EventCount {
		return left.EventCount > right.EventCount
	}
	if left.BinlogBytes != right.BinlogBytes {
		return left.BinlogBytes > right.BinlogBytes
	}
	if left.TxnCount != right.TxnCount {
		return left.TxnCount > right.TxnCount
	}
	if left.ServerID != right.ServerID {
		return left.ServerID < right.ServerID
	}
	if left.ThreadID != right.ThreadID {
		return left.ThreadID < right.ThreadID
	}
	if left.ActorUser != right.ActorUser {
		return left.ActorUser < right.ActorUser
	}
	return left.ActorHost < right.ActorHost
}
