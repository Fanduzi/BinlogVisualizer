package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestAnalyzerRanksThreadsByRowsAndKeepsActorSchema(t *testing.T) {
	base := time.Date(2026, 10, 6, 11, 21, 18, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", ServerID: 1, ThreadID: 14, ActorUser: "app", ActorHost: "db.local"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", ServerID: 1, ThreadID: 14, Schema: "reviewdb", Table: "bulk", Operation: "INSERT", RowCount: 50001},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", ServerID: 1, ThreadID: 14},
		{Timestamp: base.Add(3 * time.Second), EventType: "BEGIN", ServerID: 1, ThreadID: 8},
		{Timestamp: base.Add(4 * time.Second), EventType: "ROWS", ServerID: 1, ThreadID: 8, Schema: "reviewdb", Table: "wide", Operation: "UPDATE", RowCount: 132591},
		{Timestamp: base.Add(5 * time.Second), EventType: "XID", ServerID: 1, ThreadID: 8},
		{Timestamp: base.Add(6 * time.Second), EventType: "BEGIN", ServerID: 1, ThreadID: 14, ActorUser: "app", ActorHost: "db.local"},
		{Timestamp: base.Add(7 * time.Second), EventType: "ROWS", ServerID: 1, ThreadID: 14, Schema: "other", Table: "t", Operation: "DELETE", RowCount: 10},
		{Timestamp: base.Add(8 * time.Second), EventType: "XID", ServerID: 1, ThreadID: 14},
		{Timestamp: base.Add(9 * time.Second), EventType: "BEGIN"},
		{Timestamp: base.Add(10 * time.Second), EventType: "ROWS", Schema: "reviewdb", Table: "anon", Operation: "INSERT", RowCount: 999999},
		{Timestamp: base.Add(11 * time.Second), EventType: "XID"},
		{Timestamp: base.Add(12 * time.Second), EventType: "BEGIN", ServerID: 2, ThreadID: 14, ActorUser: "other", ActorHost: "h"},
		{Timestamp: base.Add(13 * time.Second), EventType: "ROWS", ServerID: 2, ThreadID: 14, Schema: "reviewdb", Table: "bulk", Operation: "INSERT", RowCount: 4},
		{Timestamp: base.Add(14 * time.Second), EventType: "XID", ServerID: 2, ThreadID: 14},
		{Timestamp: base.Add(15 * time.Second), EventType: "BEGIN", ActorUser: "bob", ActorHost: "app.local"},
		{Timestamp: base.Add(16 * time.Second), EventType: "ROWS", ActorUser: "bob", ActorHost: "app.local", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 7},
		{Timestamp: base.Add(17 * time.Second), EventType: "XID", ActorUser: "bob", ActorHost: "app.local"},
	}

	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if result.ThreadsRankedBy != model.ThreadRankRows {
		t.Fatalf("ranked by %q, want rows", result.ThreadsRankedBy)
	}
	if len(result.Threads) != 4 {
		t.Fatalf("threads = %d (%+v), want 4 distinct sessions and no anonymous thread", len(result.Threads), result.Threads)
	}
	top := result.Threads[0]
	if top.ThreadID != 8 || top.TotalRows != 132591 || top.Schema != "reviewdb" || top.ServerID != 1 {
		t.Fatalf("top thread = %+v, want thread 8 / 132591 rows / reviewdb", top)
	}
	second := result.Threads[1]
	if second.ThreadID != 14 || second.ServerID != 1 || second.TotalRows != 50011 || second.TxnCount != 2 {
		t.Fatalf("second thread = %+v, want server 1 thread 14 summed to 50011 rows / 2 txns", second)
	}
	if second.ActorUser != "app" || second.ActorHost != "db.local" {
		t.Fatalf("second actor = %s@%s", second.ActorUser, second.ActorHost)
	}
	if second.Schema != "reviewdb" || len(second.Schemas) != 2 || second.Schemas[1] != "other" {
		t.Fatalf("second schemas = %q %+v", second.Schema, second.Schemas)
	}
	actorOnly := result.Threads[2]
	if actorOnly.ThreadID != 0 || actorOnly.ActorUser != "bob" || actorOnly.TotalRows != 7 || actorOnly.Schema != "shop" {
		t.Fatalf("actor-only session = %+v", actorOnly)
	}
	otherServer := result.Threads[3]
	if otherServer.ServerID != 2 || otherServer.ThreadID != 14 || otherServer.TotalRows != 4 {
		t.Fatalf("same thread id on another server was merged: %+v", otherServer)
	}
	if top.Share <= second.Share {
		t.Fatalf("share not ordered: top %v second %v", top.Share, second.Share)
	}
}

func TestThreadAggregatorRanksByEventsWhenRowsAreAbsent(t *testing.T) {
	agg := threadAggregator{}
	agg.add(model.Transaction{ThreadID: 3, EventCount: 2, Completeness: model.TransactionComplete})
	agg.add(model.Transaction{ThreadID: 9, EventCount: 8, BinlogBytes: 40, Completeness: model.TransactionComplete})
	threads, rankedBy := agg.snapshot()
	if rankedBy != model.ThreadRankEvents {
		t.Fatalf("ranked by %q, want events", rankedBy)
	}
	if len(threads) != 2 || threads[0].ThreadID != 9 || threads[0].EventCount != 8 {
		t.Fatalf("threads = %+v", threads)
	}
}
