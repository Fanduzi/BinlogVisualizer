// Package binlogviz benchmarks the real streaming command path over fixture-backed and synthetic workloads.
// input: real parser fixtures and synthetic RawEvent streams that feed normalize->Consume->Finalize with DuckDB temp stores.
// output: benchmark evidence for parser+normalize+streaming analyze throughput on Stage 5 workloads.
// pos: command-layer benchmark suite covering the production streaming and DuckDB path instead of slice-based wrappers.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"path/filepath"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
)

func BenchmarkStreamingRealFixtureEndToEnd(b *testing.B) {
	fixture := filepath.Join("..", "..", "internal", "binlog", "testdata", "minimal.binlog")
	parser := binlog.NewParser()
	benchmarkStreamingPipeline(b, []string{fixture}, parser, analyzer.DefaultOptions())
}

func BenchmarkStreamingSynthetic100k(b *testing.B) {
	base := time.Date(2026, 3, 17, 14, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 1000, 100)
	benchmarkStreamingPipeline(b, []string{"synthetic-100k"}, &mockParser{events: events}, analyzer.DefaultOptions())
}

func BenchmarkStreamingManyTransactions(b *testing.B) {
	base := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 10000, 3)
	benchmarkStreamingPipeline(b, []string{"many-transactions"}, &mockParser{events: events}, analyzer.DefaultOptions())
}

func BenchmarkStreamingSpikeHeavy(b *testing.B) {
	base := time.Date(2026, 3, 17, 16, 0, 0, 0, time.UTC)
	events := makeSpikeHeavyEvents(base, 180)
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true
	benchmarkStreamingPipeline(b, []string{"spike-heavy"}, &mockParser{events: events}, opts)
}

func benchmarkStreamingPipeline(b *testing.B, paths []string, parser binlog.Parser, opts analyzer.Options) {
	b.Helper()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store, cleanup, _, err := createDuckDBTempStore("")
		if err != nil {
			b.Fatalf("createDuckDBTempStore: %v", err)
		}

		a := analyzer.NewWithStore(opts, store)
		if err := parser.ParseFiles(paths, func(raw binlog.RawEvent) error {
			normalized, err := binlog.NormalizeRawEvent(raw)
			if err != nil {
				return err
			}
			if normalized == nil {
				return nil
			}
			return a.Consume(*normalized)
		}); err != nil {
			_ = cleanup()
			b.Fatalf("ParseFiles: %v", err)
		}

		if _, err := a.Finalize(); err != nil {
			_ = cleanup()
			b.Fatalf("Finalize: %v", err)
		}
		if err := cleanup(); err != nil {
			b.Fatalf("cleanup: %v", err)
		}
	}
}

func makeSyntheticTransactionEvents(base time.Time, txnCount, rowsPerTxn int) []binlog.RawEvent {
	events := make([]binlog.RawEvent, 0, txnCount*(rowsPerTxn+2))
	for txn := 0; txn < txnCount; txn++ {
		txnBase := base.Add(time.Duration(txn) * time.Second)
		events = append(events, binlog.RawEvent{
			Timestamp: txnBase,
			EventType: "QUERY_EVENT",
			Query:     "BEGIN",
		})
		for row := 0; row < rowsPerTxn; row++ {
			events = append(events, binlog.RawEvent{
				Timestamp: txnBase.Add(time.Duration(row+1) * time.Millisecond),
				EventType: "WRITE_ROWS_EVENT",
				Schema:    "bench",
				Table:     "orders",
				RowCount:  1,
			})
		}
		events = append(events, binlog.RawEvent{
			Timestamp: txnBase.Add(time.Duration(rowsPerTxn+1) * time.Millisecond),
			EventType: "XID_EVENT",
		})
	}
	return events
}

func makeSpikeHeavyEvents(base time.Time, minuteCount int) []binlog.RawEvent {
	events := make([]binlog.RawEvent, 0, minuteCount*20)
	for minute := 0; minute < minuteCount; minute++ {
		rowsPerMinute := 12
		if minute%30 == 0 || minute%31 == 0 {
			rowsPerMinute = 180
		}
		for row := 0; row < rowsPerMinute; row++ {
			events = append(events, binlog.RawEvent{
				Timestamp: base.Add(time.Duration(minute)*time.Minute + time.Duration(row)*time.Millisecond),
				EventType: "WRITE_ROWS_EVENT",
				Schema:    "bench",
				Table:     "orders",
				RowCount:  1,
			})
		}
	}
	return events
}
