// Package binlogviz benchmarks the real streaming command path over fixture-backed and synthetic workloads.
// input: real parser fixtures and synthetic RawEvent streams that feed normalize->Consume->Finalize with DuckDB temp stores.
// output: benchmark evidence for parser+normalize+streaming analyze throughput on Stage 5 workloads.
// pos: command-layer benchmark suite covering the production streaming and DuckDB path instead of slice-based wrappers.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

func BenchmarkStreamingRealFixtureEndToEnd(b *testing.B) {
	fixture := filepath.Join("..", "..", "internal", "binlog", "testdata", "minimal.binlog")
	parser := binlog.NewParser()
	benchmarkStreamingPipeline(b, []string{fixture}, parser, analyzer.DefaultOptions())
}

// --- Real parser benchmarks: isolated layers to identify per-stage throughput ---

func BenchmarkParserRealFixtureParseOnly(b *testing.B) {
	fixture := filepath.Join("..", "..", "internal", "binlog", "testdata", "minimal.binlog")
	parser := binlog.NewParser()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := parser.ParseFiles([]string{fixture}, func(raw binlog.RawEvent) error {
			count++
			return nil
		}); err != nil {
			b.Fatalf("ParseFiles: %v", err)
		}
		_ = count
	}
}

func BenchmarkParserRealFixtureParseAndNormalize(b *testing.B) {
	fixture := filepath.Join("..", "..", "internal", "binlog", "testdata", "minimal.binlog")
	parser := binlog.NewParser()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := parser.ParseFiles([]string{fixture}, func(raw binlog.RawEvent) error {
			var normalized model.NormalizedEvent
			ok, err := binlog.NormalizeRawEventInto(raw, &normalized)
			if err != nil {
				return err
			}
			if ok {
				count++
			}
			return nil
		}); err != nil {
			b.Fatalf("ParseFiles: %v", err)
		}
		_ = count
	}
}

func BenchmarkParserRealFixtureParseWithProgress(b *testing.B) {
	fixture := filepath.Join("..", "..", "internal", "binlog", "testdata", "minimal.binlog")
	progressParser := binlog.NewParser().(binlog.ProgressParser)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		progressCount := 0
		eventCount := 0
		if err := progressParser.ParseFilesWithProgress([]string{fixture}, func(progress binlog.ParseProgress) {
			progressCount++
		}, func(raw binlog.RawEvent) error {
			eventCount++
			return nil
		}); err != nil {
			b.Fatalf("ParseFilesWithProgress: %v", err)
		}
		_ = progressCount
		_ = eventCount
	}
}

func BenchmarkParserRealFixtureEndToEnd(b *testing.B) {
	fixture := filepath.Join("..", "..", "internal", "binlog", "testdata", "minimal.binlog")
	parser := binlog.NewParser()
	benchmarkStreamingPipeline(b, []string{fixture}, parser, analyzer.DefaultOptions())
}

func BenchmarkStreamingSynthetic100k(b *testing.B) {
	base := time.Date(2026, 3, 17, 14, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 1000, 100)
	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"synthetic-100k"}, &mockParser{events: events}, analyzer.DefaultOptions())
}

func BenchmarkStreamingManyTransactions(b *testing.B) {
	base := time.Date(2026, 3, 17, 15, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 10000, 3)
	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"many-transactions"}, &mockParser{events: events}, analyzer.DefaultOptions())
}

func BenchmarkStreamingSpikeHeavy(b *testing.B) {
	base := time.Date(2026, 3, 17, 16, 0, 0, 0, time.UTC)
	events := makeSpikeHeavyEvents(base, 180)
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true
	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"spike-heavy"}, &mockParser{events: events}, opts)
}

func BenchmarkAnalyzePlanNarrowWindowFewHits(b *testing.B) {
	base := time.Date(2026, 4, 16, 8, 0, 0, 0, time.UTC)
	probes := makeSyntheticFileProbes(base, 240, time.Hour)
	start := base.Add(91 * time.Hour)
	end := base.Add(93*time.Hour + 30*time.Minute)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan := buildAnalyzePlan(probes, start, end, defaultAnalyzeProbeWorkers(len(probes)))
		if len(plan.Paths) != 3 {
			b.Fatalf("expected 3 paths in narrow plan, got %d", len(plan.Paths))
		}
	}
}

func BenchmarkAnalyzePlanWindowSpanningManyFiles(b *testing.B) {
	base := time.Date(2026, 4, 16, 8, 0, 0, 0, time.UTC)
	probes := makeSyntheticFileProbes(base, 240, time.Hour)
	start := base.Add(40*time.Hour + 15*time.Minute)
	end := base.Add(160*time.Hour + 45*time.Minute)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan := buildAnalyzePlan(probes, start, end, defaultAnalyzeProbeWorkers(len(probes)))
		if len(plan.Paths) != 121 {
			b.Fatalf("expected 121 paths in spanning plan, got %d", len(plan.Paths))
		}
	}
}

func BenchmarkStreamingLargeTransactions(b *testing.B) {
	base := time.Date(2026, 3, 17, 17, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 250, 1200)
	opts := analyzer.DefaultOptions()
	opts.LargeTxnRows = 500
	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"large-transactions"}, &mockParser{events: events}, opts)
}

func BenchmarkStreamingDDLHeavy(b *testing.B) {
	base := time.Date(2026, 3, 17, 18, 0, 0, 0, time.UTC)
	events := makeDDLHeavyEvents(base, 5000)
	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"ddl-heavy"}, &mockParser{events: events}, analyzer.DefaultOptions())
}

func BenchmarkStreamingSyntheticLargeInputMix(b *testing.B) {
	base := time.Date(2026, 3, 17, 19, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 5000, 120)
	events = append(events, makeDDLHeavyEvents(base.Add(24*time.Hour), 1000)...)
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true
	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"synthetic-large-input-mix"}, &mockParser{events: events}, opts)
}

func BenchmarkAnalyzeRenderTextVsHTML(b *testing.B) {
	base := time.Date(2026, 4, 17, 10, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 5000, 120)
	result := benchmarkAnalysisResult(b, events)

	b.Run("text", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := report.RenderText(result); err != nil {
				b.Fatalf("RenderText: %v", err)
			}
		}
	})

	b.Run("html", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := report.RenderHTML(result); err != nil {
				b.Fatalf("RenderHTML: %v", err)
			}
		}
	})
}

func BenchmarkAnalyzeNearOneGBSyntheticMix(b *testing.B) {
	base := time.Date(2026, 4, 17, 11, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 20000, 120)
	events = append(events, makeDDLHeavyEvents(base.Add(24*time.Hour), 2000)...)
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true
	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"near-1gb-synthetic-mix"}, &mockParser{events: events}, opts)
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
			var normalized model.NormalizedEvent
			ok, err := binlog.NormalizeRawEventInto(raw, &normalized)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return a.Consume(normalized)
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

func benchmarkAnalysisResult(b *testing.B, raw []binlog.RawEvent) model.AnalysisResult {
	b.Helper()

	a := analyzer.New(analyzer.DefaultOptions())
	for _, ev := range raw {
		var normalized model.NormalizedEvent
		ok, err := binlog.NormalizeRawEventInto(ev, &normalized)
		if err != nil {
			b.Fatalf("NormalizeRawEventInto: %v", err)
		}
		if !ok {
			continue
		}
		if err := a.Consume(normalized); err != nil {
			b.Fatalf("Consume: %v", err)
		}
	}

	result, err := a.Finalize()
	if err != nil {
		b.Fatalf("Finalize: %v", err)
	}
	return *result
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

func makeSyntheticFileProbes(base time.Time, fileCount int, fileSpan time.Duration) []binlog.FileProbe {
	probes := make([]binlog.FileProbe, 0, fileCount)
	for i := 0; i < fileCount; i++ {
		first := base.Add(time.Duration(i) * fileSpan)
		probes = append(probes, binlog.FileProbe{
			BinlogPath:   filepath.Join("/synthetic", fmt.Sprintf("mysql-bin.%06d", i+1)),
			FirstEventAt: first,
			LastEventAt:  first.Add(fileSpan - time.Nanosecond),
		})
	}
	return probes
}

func BenchmarkAnalyzeExternalRealBinlog(b *testing.B) {
	path := os.Getenv("BINLOGVIZ_REAL_BINLOG")
	if path == "" {
		b.Skip("set BINLOGVIZ_REAL_BINLOG to run external real-binlog benchmark")
	}
	if _, err := os.Stat(path); err != nil {
		b.Fatalf("BINLOGVIZ_REAL_BINLOG file not accessible: %v", err)
	}
	parser := binlog.NewParser()
	benchmarkStreamingPipeline(b, []string{path}, parser, analyzer.DefaultOptions())
}

func makeDDLHeavyEvents(base time.Time, ddlCount int) []binlog.RawEvent {
	events := make([]binlog.RawEvent, 0, ddlCount)
	for i := 0; i < ddlCount; i++ {
		events = append(events, binlog.RawEvent{
			Timestamp: base.Add(time.Duration(i) * time.Second),
			EventType: "QUERY_EVENT",
			Schema:    "bench",
			Query:     "ALTER TABLE bench.orders ADD COLUMN marker INT",
		})
	}
	return events
}
