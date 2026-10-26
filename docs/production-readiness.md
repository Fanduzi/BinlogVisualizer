# BinlogViz 生产级验收清单

基线：main `45f6a8b`（v0.34.1 加上 #369 版本矩阵 C2–C4、#370 四种格式共用重点事务 A3、#374 analyze 内存不随事务数增长、#376 GTID 过滤下 analyze 不再缓存整个事务、#379 GTID 过滤的匹配证据改成区间集合，E1 为 MET，#382 大文件基准 E2 为 MET，#383/#384 匹配事务计数与 MariaDB 选择器（#383 含 MariaDB 共享区间与区间选择器，#381 QA 的 P3）、#385 `TestLargeInputBaseline`（E2 为 MET）），2026-10-20 核对，E2 于 2026-10-21 复核。D5 在这次核对里改为 MET（JSON Schema 与 `--tz`，#255 第 3、7 项）。

状态口径：
- **MET**：有自动检查，今天每个 PR 或每次 push main 都在 CI 里跑并且通过。
- **PARTIAL**：只覆盖一部分，或只在 CI 之外（QA 本地）跑。
- **NOT MET**：没有可重跑的自动检查，或者已知缺陷还开着。

CI 现状（`ci.yml`，每个 PR 和 push main 都跑）：
- `verify`：`go test ./...`、`goreleaser check`、按 Linux 发布的方式（`-buildvcs=false` 加 commit ldflags）打包，再对包跑 `release_smoke.sh`，要求包报告本次 CI 的 commit。
- `vet and race`：`go vet ./...`、`go test -race -count=1 ./...`。
- `flashback e2e`：用 ubuntu-24.04 自带的 MySQL 8.0，跑 `go test -run 'TestFlashbackRoundTripMySQL80|TestNumericDecodeMySQL80|TestFlashbackFailedApplyDropsAndSignals'`，再跑 `internal/guard` 的 `OnServer` 测试；有任何 SKIP 或缺 PASS 就判失败。

需要 MySQL 的测试在 `verify` 里都会 Skip（`go test` 不加 -v，日志里看不到）。

## A. 分析正确性

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| A1 | analyze 在固定语料上的统计和报告输出稳定；遇到 STATEMENT/MIXED 格式时报警并以非 0 退出 | `golden_test.go`、`analyze_corpus_rich_test.go`、`TestStatementBinlogIsUnsupported`，用 `internal/binlog/testdata` 下的 MySQL 8.0.46 夹具（都小于 10KB） | MET | 注意：QA 用的 37 个真实 binlog（含 105MB 的 rt325）只在 QA 机器上，不在仓库也不在 CI |
| A2 | 解析结果和独立实现（mysqlbinlog）交叉校验 | `dml_rows_test.go`、`replica_apply_test.go`、`drop_table_gtid_test.go`，对照仓库里提交的 `.mysqlbinlog.txt` 解码 | MET | — |
| A3 | text、Markdown、JSON、HTML 选出的「重点事务」是同一批 | `TestAnalyzeFormatsShareKeyTransactions`（`go test ./...`，verify）比较四种格式的**有序**事务序列（顺序不同也算失败），并检查 HTML 证据不越出这一批。夹具是 `internal/binlog/testdata` 里的 `mysql-8.0.46-busiest-minute.binlog`、`mysql-8.0.46-committed-duration.binlog`、`mysql-8.0.46-dml-full.binlog`，另加一例 `busiest-minute --start-position 384 --top-transactions 0`：txn-1 被窗口截成 `partial_start`，排在完整事务之后，测试要求列表里确有不完整证据的事务。变异检查：某一格式倒序、丢掉不完整事务、只列冠军、截断、多列、丢首行，都会让测试失败 | MET | — |

## B. Flashback 安全（绝不写错数据）

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| B1 | 选中的行在之后被改过时，默认拒绝（exit 7，不输出 SQL）；`--conflict guard` 只回滚 after image 能对上的行 | `TestFlashbackConflictExitCode`；`TestFlashbackRoundTripMySQL80GuardSkipMarker`、`NoPKGuardRerun`（flashback e2e） | MET | #270（主键被复用时撞 1062，整笔回滚，不会写错数据） |
| B2 | 无法精确还原的列类型、镜像不全、DDL、schema 对不上时，exit 6 拒绝，一行 SQL 都不输出 | `TestFlashbackRefusesIncompleteMetadataImageAndDDL`、`TestFlashbackRefusalJSONReasons`、`TestFlashbackSchemaFileRefusesMismatch`、`TestNumericDecodeMySQL80` | MET | — |
| B3 | 回滚往返后数据和原状态逐行一致；任何一条按主键回滚的语句影响的不是恰好 1 行，整笔回滚，不留部分提交 | `TestFlashbackRoundTripMySQL80`、`…Integrity`、`…PartialCommit`、`…PadCharModes`、`…BinaryPad`、`…PreXAGrantForce`（flashback e2e，8.0） | MET | — |
| B4 | 被改过的脚本、旧格式的脚本，`flashback verify` 一律拒绝 | `TestVerifyRefusesTamperedScripts`、`TestVerify*IsOldFormat`、`TestWriteDigestsTheExecutedString` | MET | — |
| B5 | apply 失败后不留下过程，失败信号可靠；guard SQL 在真实服务器上行为正确 | `TestFlashbackFailedApplyDropsAndSignals`（flashback e2e 的 `-run` 里，并要求 `--- PASS`）；`TestGuardMessageOnServer`、`TestGuardMatchOnServer`、`TestGuardMessageCutSweepOnServer`（flashback e2e 的「Guard SQL on MySQL 8.0」一步，`BINLOGVIZ_GUARD_MYSQL` 指向同一台 MySQL 8.0，有 SKIP 或缺 PASS 就失败） | MET | — |

## C. MySQL 版本矩阵

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| C1 | MySQL 8.0 上 analyze 和 flashback 端到端正确 | `flashback e2e` job。注意：用的是 runner 镜像里预装的 8.0.x，小版本跟着镜像变，没有固定 | MET | — |
| C2 | MySQL 5.7：analyze 正确；flashback 明确拒绝（exit 6），不产出脚本 | `TestVersionMatrixAnalyzeMatchesDecode`、`TestVersionMatrixFlashbackBoundary`（`go test ./...`）。夹具 `mysql-5.7.44-wide-full`、`wide-minimal`、`ddl`，对照提交的 `mysqlbinlog -vv` 解码。行和 DDL 与解码一致；列名是位置序号，文本报告写提交时间戳不可用。flashback 以退出码 6 拒绝（没有列名），stdout 为空。`TestFlashbackMySQL57DeprecatedMode` 仍要活的 5.7，测的是在 5.7 上执行脚本时的 3090 警告，不属于本项 | MET | — |
| C3 | MySQL 8.4 LTS（以及 9.x）支持或明确拒绝 | 同上，夹具 `mysql-8.4.11`、`mysql-9.7.2` 的 wide-full、wide-minimal、ddl。FULL 镜像加 `--schema-file` 生成脚本：前镜像（无符号、DECIMAL、DATETIME(6)、TIMESTAMP(6)、TEXT、BLOB、JSON、ENUM、SET）在 SQL 里，后镜像不在，生成列不写入。MINIMAL 镜像和范围内的 DDL 以退出码 6 拒绝，不产出 SQL。在 8.4 / 9.x 上执行脚本的往返不在本项，和 C1 的 8.0 e2e 分开 | MET | 要在 8.4 / 9.x 上做 apply 往返，需要改 workflow |
| C4 | MariaDB：analyze 正确；flashback 的支持边界写清楚并且有测试 | 同上，夹具 `mariadb-10.11.19`、`mariadb-11.8.9`，对照提交的 `mariadb-binlog -vv` 解码。analyze 一致，文本报告写提交时间戳不可用。10.11 的 FULL 镜像加 schema 文件（JSON 列在 binlog 里是 longtext）生成脚本。MINIMAL 和 DDL 拒绝。11.8 即使 `binlog_row_image=FULL` 也不把生成列写入行镜像，整文件以退出码 6 拒绝；`--include-table shop.heap` 只撤销没有生成列的表。MariaDB 上不能执行脚本：脚本开头检查 `@@version`，在 MariaDB 上以 ERROR 1231 明确停下、不改数据（`--force` 也一样），生成时给出 `mariadb_apply_unsupported` 警告；已在 10.11.19 / 11.8.9 上实测。MariaDB 的 apply 路径不在本项 | MET | MariaDB 的 apply 方言（不用 `GET STACKED DIAGNOSTICS` 的检查行和收尾过程）是后续工作 |

## D. Agent 使用契约

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| D1 | 每个 JSON 输出都带 `schema_version`，遇到不认识的版本就拒绝 | `TestEveryJSONOutputIsVersioned`、`internal/jsonout` 单测、`TestWorkflowRefusesUnknownArtifactSchemaVersion` | MET | — |
| D2 | 退出码按种类结构化，没有不带类型的错误 | `TestExitTaxonomy277`、`TestNoUntypedErrors`、`exit_kinds*_test.go`；`docs/reference/exit-codes.md` | MET | — |
| D3 | 每个子命令的 `--help` 都能自解释，并带 Example | `TestEveryCommandHasExample` 走命令树（不写死名单），缺 Example、Example 里没有该命令的可执行行，或 `--help` 没印出来，就失败。`TestCommandExamplesRun` 在仓库夹具上执行每条 Example | MET | — |
| D4 | 没有交互提示；默认安全；有 dry-run | `TestCommandsDoNotPrompt`：每个命令在非 TTY、stdin 阻塞时限时返回，输出里没有确认提示；用法里把 `-` 写成 stdin 时，stdin 已关闭则立刻报没有数据，不提问。`TestNoInteractivePromptAPI` 禁止提示库、`fmt.Scan` 和 `ReadPassword`（读 stdin 只允许 `analyze_stdin.go`）。`TestFlashbackPlanRefusalPrintsReasons`：exit 6 时 `--plan` 的 stdout 仍是带 `plan_version` 的 JSON，里面有 `would_refuse` 和 `refusal`；`--error-format text` 与 `json` 的 stderr 写同一条原因。`install.sh --dry-run`；`workflow clean` 默认只预览 | MET | — |
| D5 | 有机器可读的输出 schema（JSON Schema 或 describe 命令），显示时区可选（`--tz`） | `TestJSONOutputMatchesSchema` 用 `schemaOutputs()`（与 `TestEveryJSONOutputIsVersioned` 同一份名单）跑每条 JSON 命令，拿成功时的 stdout 对 `docs/schema/<name>.json` 做校验；名单里多一个名字、少一个 runner 或少一个文件就失败。`TestJSONFailureOutputMatchesSchema` 用同一份名单跑每条命令的一次失败：失败时写到 stdout 的 JSON 文档必须通过该命令的 schema（`workflow describe` 与 `workflow validate` 打印 `{schema_version, valid:false, error}`；describe 的 schema 是成功形状与这个失败形状的 `oneOf`，成功文档仍要求 `output_dir`、`snapshot_save`、`workflow_name`）。失败时 stdout 为空的命令，用例要求 stdout 为空。名单里少一条失败用例就失败。`TestPublishedSchemaMatchesGenerator` 要求文件、生成器和 `binlogviz schema <name>` 的字节一致。`TestSchemaRejectsDroppedProperty` 从 analyze schema 删掉 `summary` 后真实输出必须校验失败。`TestSchemaRejectsUnknownField` 给输出加一个 schema 里没有的字段必须失败。`TestDisplayTimezone` 用 `Asia/Kolkata`（+05:30）、`America/New_York`（夏令时）和 `Etc/UTC` 跑 analyze 的 JSON/文本/Markdown/HTML，时刻不变、偏移正确，并检查 snapshot list/show、workflow describe、compare/trend HTML；非法或空的 `--tz` 退出码 3、kind `usage`。`TestDefaultOutputByteIdentical` 在不传 `--tz` 时比较 `TZ=UTC` 与 `TZ=America/New_York` 的 analyze JSON/文本/Markdown，必须逐字节相同且仍是 `Z` / ` UTC`。`TestStartEndTimezone` 证明不带 `--tz` 时 `YYYY-MM-DD HH:MM:SS` 仍按机器本地时区；设置了 `--tz` 时无时区的值按 `--tz` 解释，包括 `--tz UTC`（与 `Etc/UTC` 同一时刻）；`±HH:MM` 和结尾 ` UTC` 是该时刻。`TestZonelessWindowAcrossHosts` 在 Atlantic/Azores、Asia/Shanghai、America/New_York 上核对 analyze 快照窗口和 flashback undo。`TestDSTGapAndOverlapRefused` 与 `TestInvalidUTCOffsetRefused` 要求空档、重叠和非法偏移退出码 3。`TestSnapshotSaveStoresUTC` 要求 `snapshot save` 把 `+14:00` 报告写成 UTC 并能 compare、trend。`TestSnapshotListWindowFollowsTimezone` 核对 list 的 window start。`TestCompareHTMLMinuteFollowsTimezone` 核对 compare HTML 的 Minute 单元格。`TestDraft202012Schemas` 用外部 Draft 2020-12 校验器检查 `docs/schema`。`TestSnapshotFileStaysUTC` 证明快照文件始终是 UTC。都在 `go test ./...`（verify）里跑 | MET | — |

## E. 大输入与资源上界

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| E1 | analyze、flashback、verify 的内存有上界，大输入走 spill，不会整份读进内存 | analyze：`TestAnalyzeRetainedHeap`（先跑 2000 个事务预热，再分别用 2.5 万和 10 万个事务的合成 MySQL 8.0 副本 binlog 走完整 analyze 流程，每 2000 个事件和 Finalize 前后在 analyze 的 goroutine 上 GC 后测 live heap；10 万比 2.5 万多出的峰值堆必须小于 256KiB，实测约 -5KB 到 67KB，每个事务不到 1 字节）。#364 把这三处按事务增长的状态改成常数大小，输出和 v0.34.0 逐字节相同：按表、按分钟的事务 key 集合换成「计数 + 上一个 key」（事务 key 按 binlog 顺序单调分配，同一事务的行事件是连续的）；热点行 map 每淘汰 `limit` 次重建一次，清掉 Go map 删除留下的墓碑；apply-delay 的精确 P95（nearest-rank）在内存里最多留 8192 个值，超过后写到已 unlink 的临时文件，再用 4 趟 16 位基数选择取第 k 小（每个事务约 8 字节临时盘，5000 万个事务约 400MB；建不了临时文件时退回内存）。(表, 分钟) 系列（每张表的活动点和每分钟的 table_rows）以前约每个点 220 字节，500 张表 × 7 天约 1GB（#389）。超过 8192 个点后写到已 unlink 的临时文件 `binlogviz-activity-*`（每个点 68 字节）。每个已落盘的分钟占一块连续区域，放不下就按 2 倍容量搬到文件末尾，所以读回是一次 pread，一段连续脏单元格是一次 pwrite。废弃区域进空闲表，必要时压缩，文件不超过点数 × 68 的 2 倍。驻留量按单元格字节封顶（800KiB），不是按 4 个分钟，交错的多个渠道仍留在内存里。spill 所在文件系统剩余空间不足以再留 32MiB 时不再扩大文件，其余留在内存；statfs 本身失败则允许写入，避免占满 TMPDIR 拖垮 flashback SQL 或 DuckDB 临时文件。报告按一分钟或一张正在展示的表读回。JSON 在第一个 stdout 字节之前读完 spill，再走内联的同一条编码路径；分析收尾（含 DuckDB 明细）或渲染时读失败则 stdout 为空，退出码 11，kind 为 spill，不是 error 或 output（ADR 0017 第 8 条、ADR 0019），字节与内联时相同。SIGTERM 的截断窗口是这次编码，与 main 相同。8192 个点以内在 Finalize 复制回分钟和表，小报告走原来的编码路径。建不了临时文件或写失败时退回内存（ADR 0019）。`TestWideActivityHeap` 用同样 24000 个事务比较每分钟 4 张表和每分钟 60 张表，多出的 live heap 必须 ≤ 2MiB：未 spill 时 analyze 多 7421024 字节、flashback 多 5410568 字节，spill 之后 run 40421515796 的 `wide-heap` 行（spill 代码与 952ae8fb 相同）analyze 多 829288 字节、flashback 多 1946800 字节。仍随输入增长、但不随事务数增长的：时间跨度（分钟桶，约 1–2KB/分钟）和 spill 后的每分钟索引、不同的表/线程/语句模式数；告警（大事务、不完整事务）按告警数增长，因为文本报告要逐条列出。flashback generate：`TestFlashbackSpillLimitOnFixture`、`TestGeneratedReviewRSS`；verify：`TestFlashbackVerifyMemoryCeiling`、`TestVerifyDoesNotBufferTheScript`。都在 CI `verify` 的 `go test ./...` 里跑；`TestGeneratedReviewRSS`、`TestVerifyDoesNotBufferTheScript` 在 -race 下用 `t.Skip` 跳过并写明原因，`TestEveryMemoryTestRunsInCIVerify` 保证 `verify` 一直是非 -race 的 `go test ./...`，这些测试都在而且没有 race 构建标签。GTID 过滤：`TestAnalyzeGTIDFilterRetainedHeap`（同样的测法：`--include-gtids` 选同样 2000 个事务时 10 万比 2.5 万个事务；一个 30 万个 UPDATE_ROWS 事件的大事务比 7.5 万个，分别在 include 和 include+exclude 下；多出的峰值堆都必须小于 256KiB，实测不到 12KB）。#376 之前 GTID 过滤会把当前事务的每个事件缓存到事务结束（每个事件约 4.3KB），30 万事件的事务会 OOM、exit 2；现在 include/exclude 在事务开头的 GTID 事件就能定下来，事件直接流过，每个事务只留常数大小的状态，输出不变（`docs/gtid-filter-streaming.md`）。匹配证据：`TestAnalyzeMatchedGTIDSetHeap`（合成 MySQL 8.0 binlog，26.2 万比 104.9 万个事务，带行事件的事务固定 256 个；分别用 `--exclude-gtids` 单个 GTID，和 `--include-gtids` 全覆盖加排除 3 个 GTID；匹配数涨 4 倍、区间数不变，多出的峰值堆必须小于 256KiB，实测 13–20KB，GOMAXPROCS=1 和 8 都通过）。#379 之前 `selection.matched_gtids` 逐个保存匹配到的 GTID，同一测试里每个匹配约 117 字节、多出约 92MB，QA 在 300 万事务的 binlog 上 `--exclude-gtids` 单个 GTID 会 OOM、exit 2；现在改成 `selection.matched_gtid_set`（按 UUID 合并的区间，如 `uuid:1-4:6-3000000`）加 `matched_transactions` 计数，`report_version` 升到 4，内存随区间数而不是匹配数增长；区间超过 10 万个时 exit 10，`details.reason` 为 `matched_gtid_intervals`（`TestAnalyzeMatchedGTIDIntervalCeilingExitsTen`）。这 8 个测试（加上 `TestWideActivityHeap`）都列在 `TestEveryMemoryTestRunsInCIVerify` 里 | MET | — |
| E2 | 大文件（≥100MB）的耗时和内存有基准，回退会让 CI 失败 | `TestLargeInputBaseline`（`docs/large-input-baseline.md`）在父进程的临时目录用 hot-rows 事务模板写出确定性的 MySQL 8.0 binlog，不提交大文件，也不读 `BINLOGVIZ_REAL_BINLOG`。模板分到 256 张表（每分钟写其中 4 张，下一分钟换下 4 张），事务间隔 1 秒：小文件 38437 个事务（13107174 字节，约 641 分钟），大文件 307500 个事务（104857657 字节，≥100MiB，约 5126 分钟、3.6 天）。analyze 与 flashback 各自先预热，再在同一进程里跑小、大两个文件；analyze 走真实解析和聚合，写成 json、text、markdown、html（报告边写边扫描，不留在内存里）。时间是本进程 user+sys（`syscall.Getrusage(RUSAGE_SELF)`）。live heap 按「固定部分 + 每分钟」建模：analyze 大减小 ≤ 1MiB + 3584 B × 多出的分钟（#389 之后实测 1238；此前 2925，那时每分钟 4 个 (表, 分钟) 点还在堆上），绝对 ≤ 32MiB + 3584 B × 分钟（实测 16.4MB，此前 24.6MB）；flashback 大减小 ≤ 1MiB + 2048 B × 分钟 + 40 B × 事务（实测约 322 B/分钟 + 31 B/事务，此前约 1360 B/分钟 + 31 B/事务），绝对 ≤ 64MiB + 2048 B × 分钟。大文件约 2 万个 (表, 分钟) 点，超过 8192 后系列在临时文件里，所以这里的每分钟项是分钟桶，不再含那 4 个点。同一分钟里很多表由 `TestWideActivityHeap` 覆盖（E1）。上限没有收紧，仍是原来的上界。进程 CPU 比 ≤ 字节数比的 2 倍；每事件 CPU analyze ≤ 20µs、flashback ≤ 50µs；每事件分配比 ≤ 1.5，绝对 4000 / 20000 字节；apply-delay 临时文件每事务 ≤ 64 字节；flashback 行 spill 每输入字节 ≤ 小文件 2 倍，SQL 字节比 0.5–2 倍。只有 CPU 门失败时整对重测一次，两次都失败才算失败（QA 在 8 核全满时 82 次里出现过 1 次 17.23 > 16）；堆、分配、spill、输出失败不重试，`TestLargeInputCPURetry` 钉住这条规则（比值约 37 的 quadratic 两次都失败）。每事件 CPU 和分配上限只对这个合成模板成立，真实大文件 analyze 是 9–65µs/事件，不是对真实成本的承诺。注入的故障都会失败：`pertable`（每表 512KB）、`minuteheap`（每分钟 16KB）、`minutescan`（每个事件扫一遍分钟桶，两次都失败）、`flashslow`、`flashleak`（flashback 每事件 8 字节）、`quadratic`、`slow`、`retain`、`retain256`；`pause` 睡 7 秒照常通过；`slowfirst`（只拖慢第一对 analyze）重测一次后通过，attempts=2。父进程只看子进程每条路径一行的 verdict 记录（次数、实测值、失败项），不再数 scale= 行，所以重测通过不会被误判。检测下限（目前会放过，记录在案，不收紧）：analyze 均匀变慢 2 倍、flashback 变慢 2.1 倍、K=1000 的 n log n、每事务约 12–13 字节以下的泄漏。analyze 每事件分配上限是 4000（#389 这次实测约 2175，此前 1966 且负载下不动；4000 仍约 1.8 倍，4KB 垃圾会过不去）。flashback 每事件 CPU 仍是 50µs：负载下最差 25.8µs，收到 35µs 只剩 1.36 倍，而同一次负载把 CPU 比值顶到上限的 1.17 倍；重测只能吸收一次停顿，不能吸收整体慢的 runner。CI 的 `go test ./...` 不显示通过包的输出，所以测试把每条路径一行的「实测/上限」写进 `$GITHUB_STEP_SUMMARY`（运行摘要页），判定之后再写：通过 ✅，重测或离上限 1.5 倍以内 ⚠️，失败 ❌，标题是子进程的失败原因，低于 1 倍余量的项（含紧门）标 over_cap=；写摘要失败会让测试失败。在 CI `verify` 的 `go test ./...` 里跑，约多 48 秒；-race 下跳过并写明原因。列在 `TestEveryMemoryTestRunsInCIVerify` | MET | — |

## F. 安装与发布完整性

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| F1 | 4 个平台的包带 checksums；`install.sh` 校验 sha256，没有校验工具时直接失败；Homebrew cask 同步 | `release.yml`；`TestInstallScriptInstallsVerifiedArchive`、`…RefusesBadDownloads`、`…FailsClosedWithoutShaTool`；`release_smoke.sh`（CI verify） | MET | — |
| F2 | 发布出来的二进制报告正确的 commit | `release.yml` 4 个构建 job 都用 `-X binlogviz/internal/version.Commit=${GITHUB_SHA}` 和 `CommitTime` 构建（Linux 在 manylinux 容器里仍是 `-buildvcs=false`），并以 `BINLOGVIZ_SMOKE_EXPECT_COMMIT=${GITHUB_SHA}`、`BINLOGVIZ_SMOKE_REQUIRE_COMMIT=1` 跑 `release_smoke.sh`，commit 不对或为空就挡住发布。CI `verify` 每个 PR 用同样的 `-buildvcs=false` 加 ldflags 构建，再以 `BINLOGVIZ_SMOKE_EXPECT_COMMIT=${{ github.sha }}` 跑包的 smoke | MET | — |
| F3 | 打 tag 前，这个 commit 已经过了 flashback e2e | `release.yml` 的 `ci` job 以 `uses: ./.github/workflows/ci.yml` 在 tag 所在 commit 上跑完整 CI（`verify` 和 MySQL 8.0 的 `flashback e2e`），4 个 build job 都 `needs: [test, ci]`，没过就不打包、不发布。`TestReleaseWaitsForFlashbackE2E`（`go test ./...`，每个 PR）锁住这条依赖：`ci.yml` 可被调用且保留 flashback-e2e，`release.yml` 调用它且 build 依赖它 | MET | — |
| F4 | CI 里跑 `go vet` 和 `-race` | `ci.yml` 的 `vet and race` job：`go vet ./...` 和 `go test -race -count=1 ./...`，每个 PR 和 push main 都跑；E1 的内存测试在 -race 下按 `TestEveryMemoryTestRunsInCIVerify` 的约定 Skip，仍由 `verify` 的普通 `go test ./...` 覆盖 | MET | — |

## G. 文档

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| G1 | 发布包里带 README 和 agent 使用指南，文档里的链接在包内包外都能打开 | `TestReleaseArchiveListsBinarySampleAndWorkflowPlan`、`TestReleaseArchiveReadmeLinksStayInside`、`install_docs_test.go` | MET | — |

## 计数

共 24 项：**MET 24 / PARTIAL 0 / NOT MET 0**，还剩 0 项。

清单上的项都已 MET。

A3 已由 #370 覆盖（四种格式比较有序的重点事务序列，含不完整证据的一例）。C2–C4 已由 `go test ./...` 里的版本矩阵夹具覆盖；8.4 / 9.x 上执行脚本的往返仍要改 workflow，不算清单项；MariaDB 上脚本明确拒绝执行。
