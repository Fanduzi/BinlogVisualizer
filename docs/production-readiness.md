# BinlogViz 生产级验收清单

基线：main `14bd41b`（#358、#361 已合入，#351 已关闭），即 v0.34.0 发布的代码（2026-10-20）。2026-10-20 核对。

状态口径：
- **MET**：有自动检查，今天每个 PR 或每次 push main 都在 CI 里跑并且通过。
- **PARTIAL**：只覆盖一部分，或只在 CI 之外（QA 本地）跑。
- **NOT MET**：没有可重跑的自动检查，或者已知缺陷还开着。

CI 现状（`ci.yml`，每个 PR 和 push main 都跑）：
- `verify`：`go test ./...`、`goreleaser check`、打包，再对包跑 `release_smoke.sh`。
- `flashback e2e`：用 ubuntu-24.04 自带的 MySQL 8.0，跑 `go test -run 'TestFlashbackRoundTripMySQL80|TestNumericDecodeMySQL80'`；有任何 SKIP 或缺 PASS 就判失败。

需要 MySQL 的测试在 `verify` 里都会 Skip（`go test` 不加 -v，日志里看不到）。

## A. 分析正确性

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| A1 | analyze 在固定语料上的统计和报告输出稳定；遇到 STATEMENT/MIXED 格式时报警并以非 0 退出 | `golden_test.go`、`analyze_corpus_rich_test.go`、`TestStatementBinlogIsUnsupported`，用 `internal/binlog/testdata` 下的 MySQL 8.0.46 夹具（都小于 10KB） | MET | 注意：QA 用的 37 个真实 binlog（含 105MB 的 rt325）只在 QA 机器上，不在仓库也不在 CI |
| A2 | 解析结果和独立实现（mysqlbinlog）交叉校验 | `dml_rows_test.go`、`replica_apply_test.go`、`drop_table_gtid_test.go`，对照仓库里提交的 `.mysqlbinlog.txt` 解码 | MET | — |
| A3 | text、Markdown、JSON、HTML 选出的「重点事务」是同一批 | 没有。要加一个四种格式取同一批事务的断言 | NOT MET | #352；#357（compare/trend 的格式分支还有 default 分支） |

## B. Flashback 安全（绝不写错数据）

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| B1 | 选中的行在之后被改过时，默认拒绝（exit 7，不输出 SQL）；`--conflict guard` 只回滚 after image 能对上的行 | `TestFlashbackConflictExitCode`；`TestFlashbackRoundTripMySQL80GuardSkipMarker`、`NoPKGuardRerun`（flashback e2e） | MET | #270（主键被复用时撞 1062，整笔回滚，不会写错数据） |
| B2 | 无法精确还原的列类型、镜像不全、DDL、schema 对不上时，exit 6 拒绝，一行 SQL 都不输出 | `TestFlashbackRefusesIncompleteMetadataImageAndDDL`、`TestFlashbackRefusalJSONReasons`、`TestFlashbackSchemaFileRefusesMismatch`、`TestNumericDecodeMySQL80` | MET | — |
| B3 | 回滚往返后数据和原状态逐行一致；任何一条按主键回滚的语句影响的不是恰好 1 行，整笔回滚，不留部分提交 | `TestFlashbackRoundTripMySQL80`、`…Integrity`、`…PartialCommit`、`…PadCharModes`、`…BinaryPad`、`…PreXAGrantForce`（flashback e2e，8.0） | MET | — |
| B4 | 被改过的脚本、旧格式的脚本，`flashback verify` 一律拒绝 | `TestVerifyRefusesTamperedScripts`、`TestVerify*IsOldFormat`、`TestWriteDigestsTheExecutedString` | MET | — |
| B5 | apply 失败后不留下过程，失败信号可靠；guard SQL 在真实服务器上行为正确 | 测试写了，但 CI 不跑：`TestFlashbackFailedApplyDropsAndSignals` 需要 `BINLOGVIZ_FLASHBACK_E2E`，可名字不在 CI 的 `-run` 正则里；`TestGuardMessageOnServer`、`TestGuardMatchOnServer`、`TestGuardMessageCutSweepOnServer` 需要 `BINLOGVIZ_GUARD_MYSQL`，CI 不设 | PARTIAL | 要改 `ci.yml`（需要 workflow token） |

## C. MySQL 版本矩阵

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| C1 | MySQL 8.0 上 analyze 和 flashback 端到端正确 | `flashback e2e` job。注意：用的是 runner 镜像里预装的 8.0.x，小版本跟着镜像变，没有固定 | MET | — |
| C2 | MySQL 5.7：analyze 正确；flashback 明确拒绝（exit 6），不产出脚本 | 只有 QA 手工在 5.7.44 上测过；`TestFlashbackMySQL57DeprecatedMode` 要 5.7 服务器，CI 不跑；仓库里没有 5.7 的 binlog 夹具 | PARTIAL | — |
| C3 | MySQL 8.4 LTS（以及 9.x）支持或明确拒绝 | 没有夹具，也没有 CI | NOT MET | — |
| C4 | MariaDB：analyze 正确；flashback 的支持边界写清楚并且有测试 | 只有 `mariadb-10.11.14-dml.binlog` 的解析测试，flashback 和 apply 都没测 | PARTIAL | — |

## D. Agent 使用契约

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| D1 | 每个 JSON 输出都带 `schema_version`，遇到不认识的版本就拒绝 | `TestEveryJSONOutputIsVersioned`、`internal/jsonout` 单测、`TestWorkflowRefusesUnknownArtifactSchemaVersion` | MET | — |
| D2 | 退出码按种类结构化，没有不带类型的错误 | `TestExitTaxonomy277`、`TestNoUntypedErrors`、`exit_kinds*_test.go`；`docs/reference/exit-codes.md` | MET | — |
| D3 | 每个子命令的 `--help` 都能自解释，并带 Example | 有措辞测试（`TestFlashbackHelpWording`、`TestSubcommandHelpUsageMatchesCommandPath`），但 22 个命令里只有 6 个顶层命令有 Example，也没有测试强制每个命令都有 | PARTIAL | #255 |
| D4 | 没有交互提示；默认安全；有 dry-run | flashback 不连 MySQL，有 `--plan`；`install.sh --dry-run`；有非 TTY 输出的测试。缺：一个确保所有命令都不读 stdin、不等确认的守卫测试；`--plan` 在 exit 6 时什么都不打印 | PARTIAL | #270 |
| D5 | 有机器可读的输出 schema（JSON Schema 或 describe 命令），显示时区可选（`--tz`） | 没有 | NOT MET | #255（第 3、7 项） |

## E. 大输入与资源上界

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| E1 | analyze、flashback、verify 的内存有上界，大输入走 spill，不会整份读进内存 | analyze：`TestAnalyzeRetainedHeap`（25k 和 100k 个事务的合成 MySQL 8.0 binlog 走完整 analyze 流程，在固定点测 live heap，每多一个事务多留的堆要小于 95 字节；现在约 76，主要是每张表按事务去重的集合）；flashback generate：`TestFlashbackSpillLimitOnFixture`、`TestGeneratedReviewRSS`；verify：`TestFlashbackVerifyMemoryCeiling`、`TestVerifyDoesNotBufferTheScript`。都在 CI `verify` 的 `go test ./...` 里跑。`TestGeneratedReviewRSS`、`TestVerifyDoesNotBufferTheScript` 在 -race 下用 `t.Skip` 跳过并写明原因；`TestEveryMemoryTestRunsInCIVerify` 保证 `verify` 一直是非 -race 的 `go test ./...`，这些测试也不会被 build tag 静默排除 | MET | — |
| E2 | 大文件（≥100MB）的耗时和内存有基准，回退会让 CI 失败 | `benchmark_test.go` 要 `BINLOGVIZ_REAL_BINLOG` 才跑，CI 不设；大语料只在 QA 机器上 | NOT MET | — |

## F. 安装与发布完整性

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| F1 | 4 个平台的包带 checksums；`install.sh` 校验 sha256，没有校验工具时直接失败；Homebrew cask 同步 | `release.yml`；`TestInstallScriptInstallsVerifiedArchive`、`…RefusesBadDownloads`、`…FailsClosedWithoutShaTool`；`release_smoke.sh`（CI verify） | MET | — |
| F2 | 发布出来的二进制报告正确的 commit | v0.33.1 的 linux_amd64 我实测，`version --format json` 里 `"commit": ""`。`release_smoke.sh` 支持 `BINLOGVIZ_SMOKE_EXPECT_COMMIT`，但 `release.yml` 没设 | NOT MET | #303（要改 workflow，需要有 workflow 权限的 token） |
| F3 | 打 tag 前，这个 commit 已经过了 flashback e2e | `release.yml` 的 test job 只跑 `go test ./...`，MySQL 测试全部 Skip，也不检查这个 commit 的 CI 结果 | NOT MET | 要改 workflow |
| F4 | CI 里跑 `go vet` 和 `-race` | 都不跑；只有 QA 在本地跑 | NOT MET | 要改 workflow |

## G. 文档

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| G1 | 发布包里带 README 和 agent 使用指南，文档里的链接在包内包外都能打开 | `TestReleaseArchiveListsBinarySampleAndWorkflowPlan`、`TestReleaseArchiveReadmeLinksStayInside`、`install_docs_test.go` | MET | — |

## 计数

共 24 项：**MET 12 / PARTIAL 5 / NOT MET 7**。

## 最卡生产级的 5 项

1. **C2–C4，版本矩阵**：「flashback 绝不写错数据」只在 MySQL 8.0 上有 CI 证据。5.7 的拒绝路径、8.4 LTS、MariaDB 都没有自动检查。
2. **B5，失败路径的测试 CI 不跑**：apply 失败后的清理（`TestFlashbackFailedApplyDropsAndSignals`）和服务器端的 guard 测试，写好了但 CI 从来不跑。
3. **F2，二进制 commit 为空**：#303，要你用有 workflow 权限的 token 改 `release.yml`。
4. **F3、F4，发布不看 e2e，CI 不跑 -race/vet**：同样卡在 workflow token。
5. **E2，大文件基准不在 CI**：≥100MB 的耗时和内存没有基准，大语料只在 QA 机器上。（E1 的偶发失败 #351 已由 #358、#361 修复，analyze 的内存测试由 #364 补上。）
