# Large-input baseline (production-readiness E2)

`TestLargeInputBaseline` is the check. It runs in CI's `verify` job because that job is plain `go test ./...`. The test generates its inputs. Nothing large is committed, and `BINLOGVIZ_REAL_BINLOG` is not involved.

## What is measured

The parent owns the temp directory and passes it to the child. The child writes two MySQL 8.0 binlogs from the hot-rows transaction template (`writeSyntheticBinlog`'s template: one-row `UPDATE` on `shop.counters`, a new GTID, primary key, and replica apply delay per transaction). A 2000-transaction file warms analyze, and the same file warms flashback, before the measured runs. The measured files are 38,437 transactions (13,107,174 bytes) and 307,500 transactions (104,857,657 bytes, above the 100MiB floor). The size ratio is 8.00. Generation is not part of the timed section.

On each file:

- **analyze** parses with the real parser, aggregates with the real analyzer, writes JSON through `runAnalysisStreamingWithDeps`, then `report.Write` renders text, JSON, markdown, and HTML from that result. Every arm of the format switch runs. Each report must carry the transaction count.
- **flashback generate** uses the same files, with `Flashback` on and `--conflict refuse`. The SQL is counted and scanned for `shop` as it is written. The ~140MB script is not held in memory.

For each pair the test records, on the goroutine doing the work, after `runtime.GC`:

| Signal | Analyze gate | Flashback gate |
|---|---|---|
| Live heap, large minus small | large peak ≤ small peak + 1MiB | extra bytes per extra transaction ≤ 100 |
| Live heap of the large run | ≤ 32MiB | ≤ 64MiB |
| Process CPU, large/small | ≤ 2 × the byte ratio | same |
| Process CPU of the large run | ≤ 20µs/event | ≤ 50µs/event |
| Bytes allocated per event, large/small | ≤ 1.5 | same |
| Bytes allocated per event, large run | ≤ 8000 | ≤ 20000 |
| Temp file | apply-delay spill ≤ 64 bytes/transaction, and ≤ 4× the small run | row-spill bytes per input byte ≤ 2× the small run |
| Output size | each format non-empty and contains the count | SQL byte ratio within 0.5×–2× the input byte ratio |

Process CPU is `syscall.Getrusage(RUSAGE_SELF)` user+sys. It is not `/cpu/classes/total:cpu-seconds`. Go defines that metric as `GOMAXPROCS` times wall time, so a pause during the large run moves the ratio with no code change.

Samples are every 20,000 events, plus both sides of `Finalize`. There is no background ticker.

The temp-file rows read `/proc/self/fd`. When that directory is not there (not Linux), those rows are skipped and the log says why. The heap and CPU gates still run. On Linux a missing spill still fails.

## Why these metrics

E2 asks for a time and memory baseline on a ≥100MB binlog that fails CI on a regression. Absolute RSS and a tight wall-clock budget do not survive a shared runner. `TestGeneratedReviewRSS` failed on a docs-only commit because it capped `VmHWM` at 64MB (#351). The first replacement sampled the heap from a second goroutine on a timer and was rewritten again so the read happens on the worker goroutine at fixed points (#355, #358, #361).

Two shapes are required:

- **A ratio between the two sizes** catches work that grows faster than the input. A uniform slowdown, or one buffer retained on both runs, stays inside the ratio.
- **An absolute cap on the large run** catches those. The caps sit several times above a clean run and far below the faults below, so runner noise has room and a real regression still fails.

Live heap is `/gc/heap/live:bytes` after `runtime.GC` on the analyze goroutine (and, for flashback, on the parse goroutine, because the SQL writer type-asserts `*analyzer.Analyzer`).

Process CPU is this process only. A sleep does not add to it. Wall clock on a shared runner does, and so does `GOMAXPROCS` times wall clock.

Allocated bytes per event catch a quadratic copy. A retained pointer does not move this number, which is why the heap cap is separate.

The apply-delay spill is 8 bytes per transaction and is an exact file size. Flashback's row spill should grow with the file; the check is that bytes of spill per byte of input stay in line.

## How the thresholds were chosen

Measured on the same comparison the test runs (warmup, then small, then large):

| | GOMAXPROCS 4 | GOMAXPROCS 1 |
|---|---|---|
| Analyze large live heap | 8.35–8.39MB | 8.27MB |
| Analyze extra live heap | 94–116KB | 51KB |
| Analyze process CPU | 4.30–4.71µs/event, ratio 7.69–7.90 | 3.84µs/event, ratio 7.00 |
| Analyze alloc per event | 1883, ratio 0.966 | 1882, ratio 0.966 |
| Apply-delay spill | 7.89 B/txn (small run 6.82) | same file sizes |
| Flashback large live heap | 15.93MB | 15.85MB |
| Flashback extra live heap | 8.32MB, 30.9 B/txn | 8.31MB, 30.9 B/txn |
| Flashback process CPU | 12.5–12.8µs/event, ratio 7.44–7.46 | 9.2µs/event, ratio 7.98 |
| Flashback alloc per event | 4309, ratio 1.002 | 4309, ratio 1.002 |
| Flashback spill | 0.58 B per input byte, both sizes | same |
| Flashback SQL ratio | 8.05 | 8.05 |

Analyze's heap gap is noise around a flat profile, so the ratio cap stays 1MiB. The absolute cap is 32MiB, about 4× the measured peak. Flashback's heap is not flat: each row adds a conflict key, about 31 bytes here. The ratio cap is 100 bytes/transaction. The absolute cap is 64MiB, about 4× the measured 16MB peak.

The CPU ratio cap is twice the byte ratio (16 when the files are 8×). That is 2.00× the linear extrapolation from the small run (`small CPU × byte ratio`). A uniform slowdown stays near the size ratio and does not trip it. Against the measured ratios above, the same limit is 2.01× to 2.28× the observed large-run cost (16 / measured ratio). The higher figure is analyze at `GOMAXPROCS=1` (measured ratio 7.00). The old `cpu-seconds` metric sat lower (about 6.87) and the same arithmetic gave about 2.33×; that metric is not the one this test uses.

The per-event CPU caps are 20µs (analyze) and 50µs (flashback): about 4× the higher measurement. An 11× per-event slowdown is past 20µs. A 2× stretch of the measured cost is not.

The alloc ratio cap is 1.5 against a measurement of 0.97–1.00. The absolute alloc caps are 8000 and 20000 bytes/event, about 4× the measurements (1883 and 4309).

## Staying non-flaky

- One process, back to back, after a warmup on each path, so a one-time cache is not charged to the large run.
- The heap read is on the worker goroutine. Nothing samples from a timer.
- The time gates use process user+sys. They do not contain a wall-clock budget, and a pause does not count.
- The two inputs differ by a fixed factor of eight, so an O(n²) term is 64× the linear term, well clear of 2× slack.
- Forced collections are every 20,000 events on both runs, so their cost scales with the input.
- The child is started with a context deadline a few seconds before `t.Deadline()`, and on Linux the kernel sends `SIGKILL` if the parent dies (`Pdeathsig`). The temp files live in the parent's directory, so a test timeout does not leave the child and its ~118MB behind.
- The test does not skip under `go test ./...`. It skips under `-race`, with a reason that names CI's verify job, and `TestEveryMemoryTestRunsInCIVerify` requires that skip text. Verify is not a race build.

## Faults

`BINLOGVIZ_LARGE_INPUT_FAULT` is unset in CI. Set on the test process it is inherited by the child:

- `slow` does a fixed amount of extra CPU per event. Analyze process CPU was 49255 ns/event against the 20000 cap (about 11× the clean ~4.3µs). The CPU ratio stayed 7.61, under 16, which is why the absolute cap is separate.
- `retain256` keeps one 256MiB buffer for the whole run. Large minus small was 55,536 bytes, under the 1MiB ratio cap. The large-run live heap was 277,257,288 bytes against the 32MiB cap.
- `pause` sleeps 7 seconds once during the large analyze. Process CPU stayed 4.7µs/event and the ratio stayed 7.85. The run passed. The same pause used to fail the old `cpu-seconds` ratio (18.42 against 16) because that metric counts wall time.
- `retain` keeps a 1KB copy of every event (about 1.3GB over the span). `quadratic` does O(n) extra work per event and moves the CPU ratio toward the square of the size ratio.

## Cost

One plain run is about 24 seconds wall on a 4-core machine. The added CI time is that run inside `go test ./...`.
