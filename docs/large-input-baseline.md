# Large-input baseline (production-readiness E2)

`TestLargeInputBaseline` is the check. It runs in CI's `verify` job because that job is plain `go test ./...`. The test generates its inputs. Nothing large is committed, and `BINLOGVIZ_REAL_BINLOG` is not involved.

## What is measured

The child writes two MySQL 8.0 binlogs into `t.TempDir()` from the hot-rows transaction template (`writeSyntheticBinlog`'s template: one-row `UPDATE` on `shop.counters`, a new GTID, primary key, and replica apply delay per transaction). A 2000-transaction file warms the process. The measured files are 38,437 transactions (13,107,174 bytes) and 307,500 transactions (104,857,657 bytes, above the 100MiB floor). The size ratio is 8.00. Generation is not part of the timed section.

On each file:

- **analyze** parses with the real parser, aggregates with the real analyzer, writes JSON through `runAnalysisStreamingWithDeps`, then `report.Write` renders text, JSON, markdown, and HTML from that result. Every arm of the format switch runs. Each report must carry the transaction count.
- **flashback generate** uses the same files, with `Flashback` on and `--conflict refuse`, and writes the SQL the command writes.

For each pair the test records, on the goroutine doing the work, after `runtime.GC`:

| Signal | Analyze gate | Flashback gate |
|---|---|---|
| Live heap (`/gc/heap/live:bytes`) | large peak ≤ small peak + 1MiB | extra bytes per extra transaction ≤ 100 |
| CPU time (`/cpu/classes/total:cpu-seconds`) | large/small ≤ 2 × the byte ratio | same |
| Bytes allocated per event (`TotalAlloc`) | large/small ≤ 1.5 | same |
| Temp file | apply-delay spill ≤ 64 bytes/transaction, and ≤ 4× the small run's per-transaction rate | row-spill bytes per input byte ≤ 2× the small run |
| Output size | each format non-empty and contains the count | SQL byte ratio within 0.5×–2× the input byte ratio |

Samples are every 20,000 events, plus both sides of `Finalize`. There is no background ticker.

## Why these metrics

E2 asks for a time and memory baseline on a ≥100MB binlog that fails CI on a regression. Absolute RSS and absolute wall clock do not survive a shared runner. `TestGeneratedReviewRSS` failed on a docs-only commit because it capped `VmHWM` at 64MB (#351). The first replacement sampled the heap from a second goroutine on a timer and was rewritten again so the read happens on the worker goroutine at fixed points (#355, #358, #361).

This test follows that shape:

- **Live heap after GC, same goroutine, large minus small.** A regression that keeps every event shows up as a gap that grows with the input. An absolute cap would move with `GOMAXPROCS` and with whatever else the runner has cached.
- **CPU-seconds ratio, not wall clock.** Wall clock on a shared runner includes time the OS gave to other jobs. CPU-seconds are this process only, and both runs share the machine, so a slow runner stretches both. The ratio is the time baseline. A uniform slowdown stays near the size ratio. An O(n²) step moves the ratio toward the square of the size ratio (64 at 8×).
- **Allocated bytes per event.** Quadratic allocation (copying a growing buffer every event) changes this ratio. Keeping a pointer does not, which is why the heap gap is a separate gate. Both runs pay the same per-event tax, so the ratio stays near 1 when the work is linear.
- **Temp-file size.** The apply-delay spill is 8 bytes per transaction and is an exact file size, not a timer. A spill of the row image is far above 64 bytes/transaction. Flashback's row spill is supposed to grow with the file; the check is that bytes of spill per byte of input stay in line between the two sizes.
- **SQL size ratio.** A flashback that stops emitting rows, or emits a quadratic script, moves this ratio off the input size ratio.

## How the thresholds were chosen

Measured on the same comparison the test runs (warmup, then small, then large), before the limits were fixed:

| | GOMAXPROCS 4 | GOMAXPROCS 1 |
|---|---|---|
| Analyze extra live heap | 45–54KB | 159KB |
| Analyze CPU ratio | 7.6–7.9 | 8.09 |
| Analyze alloc-per-event ratio | 0.97 | 0.97 |
| Apply-delay spill | 7.89 B/txn (small run 6.82) | same file sizes |
| Flashback extra live heap | 8.32MB, 30.9 B/txn | 8.31MB, 30.9 B/txn |
| Flashback CPU ratio | 7.57 | 7.58 |
| Flashback alloc ratio | 1.01 | 1.01 |
| Flashback spill | 0.58 B per input byte, both sizes | same |
| Flashback SQL ratio | 8.05 | 8.05 |

Analyze's heap gap is noise around a flat profile, so the cap is 1MiB: about 6× the higher measurement, and about 1000× smaller than the retain fault below. Flashback's heap is not flat. Each row adds a conflict key, about 31 bytes here. The cap is 100 bytes/transaction, the same line the generated-column review uses to reject keeping row images (those cost hundreds of bytes per row).

The CPU cap is twice the byte ratio (16 when the files are 8×). The measured ratios sit on the linear line (7.6–8.1). Twice linear still rejects an O(n²) step and a 1.5-power step (8^1.5 ≈ 23). It accepts a uniform 2× slowdown, which is not a scaling regression.

The alloc cap is 1.5 against a measurement of 0.97–1.01. The spill caps sit on exact file sizes.

## Staying non-flaky

- One process, back to back, after a warmup, so a one-time cache is not charged to the large run.
- The heap read is on the analyze goroutine (and, for flashback, on the parse goroutine, because the SQL writer type-asserts `*analyzer.Analyzer`). Nothing samples from a timer.
- The time gate is a ratio of CPU-seconds. It does not contain a millisecond budget.
- The two inputs differ by a fixed factor of eight, chosen so an O(n²) term is 64× the linear term, well clear of 2× slack. A smaller factor would let a quadratic hide inside the slack.
- Forced collections are every 20,000 events on both runs, so their cost scales with the input and does not by itself move the ratio.
- The test does not skip under `go test ./...`. It skips under `-race`, with a reason that names CI's verify job, and `TestEveryMemoryTestRunsInCIVerify` requires that skip text. Verify is not a race build. Parsing this file under the race detector is several times slower than the plain run.

## Faults

`BINLOGVIZ_LARGE_INPUT_FAULT` is unset in CI. Set on the test process it is inherited by the child:

- `retain` keeps a 1KB copy of every event. The heap gap was 1,412,537,792 bytes against the 1MiB cap. CPU ratio stayed 11.8, under the cap, which is why the heap gate is separate.
- `quadratic` does O(n) extra work per event (`i += 32`). CPU ratio was 43.69 against the cap of 16. The heap gap stayed 53KB.

## Cost

One plain run is about 24 seconds wall on a 4-core machine (analyze about 6 seconds, flashback generate the rest, including writing about 134MB of SQL for the large file). The added CI time is that run inside `go test ./...`.
