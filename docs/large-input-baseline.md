# Large-input baseline (production-readiness E2)

`TestLargeInputBaseline` is the check. It runs in CI's `verify` job because that job is plain `go test ./...`. The test generates its inputs. Nothing large is committed, and `BINLOGVIZ_REAL_BINLOG` is not involved.

## What is measured

The parent owns the temp directory and passes it to the child. The child writes two MySQL 8.0 binlogs from the hot-rows transaction template (`writeSyntheticBinlog`'s template: a one-row `UPDATE`, a new GTID, primary key, and replica apply delay per transaction), spread over a wider shape:

- **256 tables** (`shop.t_000000` to `shop.t_000255`, each with its own table id). Each minute writes 4 of them and the next minute moves on to the next 4, so both files touch every table.
- **1 second between commits.** The small file spans about 641 minutes (10.7 hours), the large one about 5126 minutes (3.6 days).

The table names have the fixture's name length, so event and file sizes do not change. A 2000-transaction file warms analyze, and the same file warms flashback, before the measured runs. The measured files are 38,437 transactions (13,107,174 bytes) and 307,500 transactions (104,857,657 bytes, above the 100MiB floor). The size ratio is 8.00. Generation is not part of the timed section.

Why 4 tables a minute and not every transaction on a new table: the analyzer keeps one activity point per table per minute (about 220 bytes) and a minute bucket per minute. Rotating every transaction through all 256 tables makes each transaction a new point, a per-transaction cost of a few hundred bytes, and a per-transaction leak of that size would hide inside it. With 4 a minute, the per-table and per-minute paths run on every event, and the legitimate growth is a fixed amount per minute that the gate budgets (below).

On each file:

- **analyze** parses with the real parser, aggregates with the real analyzer, writes JSON through `runAnalysisStreamingWithDeps`, then `report.Write` renders text, JSON, markdown, and HTML from that result. Every arm of the format switch runs. Each report must carry the transaction count. The reports are scanned as they are written and not kept, so the live heap is the analyzer's, not the test's copy of a 5MB JSON report.
- **flashback generate** uses the same files, with `Flashback` on and `--conflict refuse`. The SQL is counted and scanned for `shop` as it is written. The ~140MB script is not held in memory.

For each pair the test records, on the goroutine doing the work, after `runtime.GC`:

| Signal | Analyze gate | Flashback gate |
|---|---|---|
| Live heap, large minus small | ≤ 1MiB + 3584 B × extra minutes | ≤ 1MiB + 2048 B × extra minutes + 40 B × extra transactions |
| Live heap of the large run | ≤ 32MiB + 3584 B × minutes | ≤ 64MiB + 2048 B × minutes |
| Process CPU, large/small | ≤ 2 × the byte ratio | same |
| Process CPU of the large run | ≤ 20µs/event | ≤ 50µs/event |
| Bytes allocated per event, large/small | ≤ 1.5 | same |
| Bytes allocated per event, large run | ≤ 8000 | ≤ 20000 |
| Temp file | apply-delay spill ≤ 64 bytes/transaction, and ≤ 4× the small run | row-spill bytes per input byte ≤ 2× the small run |
| Output size | each format non-empty and contains the count | SQL byte ratio within 0.5×–2× the input byte ratio |

Live heap is modelled as a fixed part plus a part per minute of time span (plus, for flashback, its conflict keys per transaction). The two files span different minutes, so the per-minute part is budgeted rather than ignored, and growing it fails. The budgets are about 1.2× (analyze, measured 2925 B/minute) and 1.5× (flashback, about 1360 B/minute beside 31 B/transaction) the measurement, which is the same on every run. With 60 transactions a minute, a leak of more than about 15 bytes per transaction (3 per event) exceeds the analyze slack, and about 25 bytes per transaction (5 per event) the flashback slack.

### Retry on CPU only

The CPU gates are the only ones a noisy host moves. Heap, allocation, spill and output size did not move under any load QA tried. When every failure of a pair is a CPU failure (the ratio or the per-event cap), the child measures the whole pair again, small and large, and fails if the second pair fails any gate. Any other failure fails at once, with no retry. The single flake QA found (1 in 82 runs, 8 saturated cores, analyze ratio 17.23 against 16) came from one stall inflating one pair. A super-linear step fails every pair: the `quadratic` and `minutescan` faults fail both attempts. `TestLargeInputCPURetry` pins the rule: the measured quadratic shape (ratio about 37) fails on the second pair, a heap or allocation failure is never retried, and a second pair that fails on heap fails.

### How the parent judges the child

The child prints one record per path, `large-input verdict {json}`, after its last pair: the path, the number of attempts, pass or fail, every gated value with its cap, the last pair's failed checks (name, value, cap, message), and the first pair's failures when it was measured again. It also prints `large-input input {json}` with the file sizes. The parent judges from these records, not from counting measurement lines, so a retried pair, which prints its `scale=` lines twice, is judged like any other. #388 at b0f2ebff counted those lines, so every retry failed, even one that passed. A passing child must exit 0, print both verdicts in order with one or two attempts, have no failed check, have retried only after CPU-only failures, report no value over its cap, report a large file at or above 100MiB, and print go test's PASS line. `TestLargeInputParentVerdict` feeds the parent synthetic child output built from real `runGatedPair` verdicts: a clean run, a CPU retry that passes, a CPU retry that fails again, a heap failure (not retried), and records that lie or are missing.

### Seeing the numbers in CI

CI runs plain `go test ./...`. In package-list mode go test discards everything a passing package prints, `t.Log` and stderr alike, so the measurements never show in the job log. GitHub Actions gives every step `$GITHUB_STEP_SUMMARY`, a file whose markdown appears on the run's summary page (to signed-in viewers), and a test process inherits it. The parent writes it after judging the child: a heading with the parent's verdict (✅ pass, ⚠️ pass after a retry or near a cap, ❌ FAIL with the failing check, its value and its cap), the parent's error, and one line per path with value/cap and headroom for each gate, `over_cap=` for values past their cap, `near_cap=` for values within 1.5× of it (the deliberately tight heap-growth budget and ratio gates are never near_cap), and the retried first attempt's failures. A failed write to the summary file fails the test (`t.Errorf`). Outside Actions the same text goes to `t.Log` (`go test -v -run TestLargeInputBaseline ./cmd/binlogviz`), and to stderr when flagged.

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

Measured on the same comparison the test runs (warmup, then small, then large), three clean runs on an 8-vCPU box shared with other QA jobs (load average 10–36, so the CPU figures are high):

| | Measured |
|---|---|
| Analyze large live heap | 24.57–24.61MB (about 9.6MB fixed + 2925 B × 5126 minutes) |
| Analyze extra live heap | 13.10–13.14MB, 2921–2929 B per extra minute |
| Analyze process CPU | 6.8–7.3µs/event, ratio 7.74–8.00 |
| Analyze alloc per event | 1960–1966, ratio 0.95 |
| Apply-delay spill | 7.89 B/txn (small run 6.82) |
| Flashback large live heap | 24.58–24.59MB |
| Flashback extra live heap | 14.40–14.42MB (31 B × 269,063 transactions + about 1360 B × 4485 minutes) |
| Flashback process CPU | 17.4–19.6µs/event, ratio 7.60–7.97 |
| Flashback alloc per event | 4439, ratio 1.00 |
| Flashback spill | 0.58 B per input byte, both sizes |
| Flashback SQL ratio | 8.05 |

The live heap numbers were the same to within 40KB on every run, under any load. QA saw the same on #382 (heap and alloc did not move under 8 or 16 CPU hogs), which is why the heap-growth budgets can sit at 1.2× and 1.5× and are never retried. The fixed parts of the absolute caps (32MiB analyze, 64MiB flashback) stay about 3.5× the fixed part of the measurement.

The CPU ratio cap is twice the byte ratio (16 when the files are 8×). That is 2.00× the linear extrapolation from the small run (`small CPU × byte ratio`). A uniform slowdown stays near the size ratio and does not trip it. On #382's one-table template, QA's quiet worst case was 9.14 for analyze and 7.76 for flashback, 1.75× and 2.06× under the cap; a stall on a saturated host reached 17.23 once in 82 runs, which the retry now absorbs.

The per-event CPU caps are 20µs (analyze) and 50µs (flashback). QA's quiet worst case on #382 left 3.1× (analyze) and 2.6× (flashback) of headroom, not the 4× first estimated; on the loaded box above it was 2.7× and 2.55×. An 11× per-event slowdown is past 20µs. A 2× stretch of the measured cost is not, on either path.

**The per-event CPU and allocation caps hold for this synthetic template only.** They are not a statement about real binlogs. The template is 68 bytes per event, one-row `UPDATE`s. On QA's ≥100MB real files analyze measured 9–65µs/event and 2.9–14KB/event, and flashback up to about 400µs/event and 273KB/event, 3–14× over these caps, while per input byte the real files were cheaper than the template. The caps exist to catch a change in the code on a fixed input, not to bound real-world cost. The heap caps did hold on the real files (≤25MB).

The alloc ratio cap is 1.5 against a measurement of 0.95–1.00. The absolute alloc caps are 8000 and 20000 bytes/event, about 4× the measurements (1966 and 4439).

What the template still does not cover: wide rows, blobs, DDL, more than 32 threads, and many tables in the same minute (real busy servers write hundreds of tables a minute; the analyzer's per-table per-minute points then grow with tables × minutes).

## Staying non-flaky

- One process, back to back, after a warmup on each path, so a one-time cache is not charged to the large run.
- The heap read is on the worker goroutine. Nothing samples from a timer.
- A pair that fails only on CPU is measured once more and fails only if the second pair fails too; heap, allocation, spill and output failures are never retried.
- The time gates use process user+sys. They do not contain a wall-clock budget, and a pause does not count.
- The two inputs differ by a fixed factor of eight, so an O(n²) term is 64× the linear term, well clear of 2× slack.
- Forced collections are every 20,000 events on both runs, so their cost scales with the input.
- The child is started with a context deadline a few seconds before `t.Deadline()`, and on Linux the kernel sends `SIGKILL` if the parent dies (`Pdeathsig`). The temp files live in the parent's directory, so a test timeout does not leave the child and its ~118MB behind.
- The test does not skip under `go test ./...`. It skips under `-race`, with a reason that names CI's verify job, and `TestEveryMemoryTestRunsInCIVerify` requires that skip text. Verify is not a race build.

## Faults

`BINLOGVIZ_LARGE_INPUT_FAULT` is unset in CI. Set on the test process it is inherited by the child. Each one was run against this template (loaded 8-vCPU box):

| Fault | What it does | Result |
|---|---|---|
| `pertable` | analyze keeps 512KB per distinct table | FAIL: live heap 158.8MB against the 51.9MB cap (32MiB + 3584 B × 5126 minutes). With #382's one table this was +0.5MB on both runs and passed. |
| `minuteheap` | analyze keeps 16KB per minute bucket | FAIL: heap grew 86.9MB against a 17.1MB budget, and 108.9MB against the absolute cap. Passed on #382's 51-minute template. |
| `minutescan` | every analyze event walks every minute bucket seen so far | FAIL on both attempts: CPU ratio 20.71 and 20.50 against 16, and 29326 / 28862 ns/event against 20000. Invisible on #382's template. |
| `flashslow` | fixed extra CPU per flashback event (the `slow` loop on the parse goroutine) | FAIL on both attempts: 123862 / 121907 ns/event against 50000 (clean 17–20µs). |
| `flashleak` | flashback keeps 8 bytes per event (about 40 B/transaction) | FAIL: heap grew 25.9MB against a 21.0MB budget (1MiB + 2048 B × 4485 minutes + 40 B × 269,063 transactions). The same leak passed #382's 100 B/transaction cap. |
| `quadratic` | O(n) extra analyze work per event | FAIL on both attempts: ratio 34.18 and 34.51, 59514 / 58608 ns/event. The retry did not hide it. |
| `slow` | fixed extra analyze CPU per event | FAIL on both attempts: 56863 / 59355 ns/event against 20000; the ratio stayed 7.89–8.16, which is why the absolute cap is separate. |
| `retain256` | one 256MiB buffer for the whole run | FAIL: live heap 293.0MB against 51.9MB. Large minus small stayed 13.2MB, inside its budget. |
| `retain` | a 1KB copy of every analyze event | FAIL: heap grew 1.43GB (child RSS 1.7GB, no rlimit). |
| `pause` | 7 s sleep once in the large analyze | PASS, as designed: 6.4µs/event, ratio 7.21. Process CPU does not count a sleep. |
| `slowfirst` | the `slow` loop on the first measured analyze pair only, a host stall in miniature | PASS, as designed, with analyze attempts=2: the first pair failed only the per-event CPU cap (61707 ns/event), the retried pair measured 5650 ns/event and ratio 7.23, and the step summary reads ⚠️ pass with the retried attempt listed. Under #388 at b0f2ebff the same child passed and the parent failed. |

**Detection limits.** These pass today, measured by QA on #388 and #382, and are accepted rather than tightened:

- a uniform 2× analyze slowdown (14.8µs/event against 20µs) and a 2.1× flashback slowdown (41.6µs against 50µs); uniform slowdowns hide up to about 2.8× (analyze) and 2.7× (flashback);
- an n log n step at K=1000 (about 3× analyze cost; the ratio stays near 8.6 and the per-event cost under 20µs);
- 4KB of short-lived garbage per event (6062 against 8000 bytes/event);
- per-transaction leaks of about 12–13 bytes or less (14 B/transaction fails), about 100KB per table (128KB fails), and about 512 bytes per minute (1024 fails).

The per-event CPU caps leave about 3× over a quiet run on purpose. CI runners are shared, and QA's flake matrix on #388 saw the CPU ratio reach 13.69 against 16 (1.17× headroom) with two busy loops on two CPUs, and flashback per-event CPU left only 1.94× headroom; the absolute caps get no ratio to hide behind, so their slack has to cover a slow runner on its own. The retry absorbs a stall in one pair, not a runner that is slow throughout. Heap and allocation caps are deterministic and are where the gate is tight.

## Cost

QA measured #382 adding about 48 seconds to CI's `go test ./...` (cmd package 88s → 136s), not the 24 seconds first written here. This template costs about 15% more process CPU than #382's (40.6s user against 34.4s, back to back on the same loaded box), from the wider minute and table state; the file sizes and event counts are unchanged. A CPU retry, which only runs after a CPU-only failure, measures one path's pair again (about 10–30 s).
