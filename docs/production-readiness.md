# BinlogViz 生产级验收清单

基线：main `013a436`（v0.34.1 加上 #369 版本矩阵，C2–C4）加上 #370（四种格式共用重点事务，A3），2026-10-20 核对。

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
| A3 | text、Markdown、JSON、HTML 选出的「重点事务」是同一批 | `TestAnalyzeFormatsShareKeyTransactions`（`go test ./...`，verify）比较四种格式的**有序**事务序列（顺序不同也算失败），并检查 HTML 证据不越出这一批。夹具是 `internal/binlog/testdata` 里的 `mysql-8.0.46-busiest-minute.binlog`、`mysql-8.0.46-committed-duration.binlog`、`mysql-8.0.46-dml-full.binlog`，另加一例 `busiest-minute --start-position 384 --top-transactions 0`：txn-1 被窗口截成 `partial_start`，排在完整事务之后，测试要求列表里确有不完整证据的事务。变异检查：某一格式倒序、丢掉不完整事务、只列冠军、截断、多列、丢首行，都会让测试失败 | MET | — |

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
| D5 | 有机器可读的输出 schema（JSON Schema 或 describe 命令），显示时区可选（`--tz`） | 没有 | NOT MET | #255（第 3、7 项） |

## E. 大输入与资源上界

| # | 要求 | 验证 | 状态 | 阻塞 |
|---|---|---|---|---|
| E1 | analyze、flashback、verify 的内存有上界，大输入走 spill，不会整份读进内存 | `TestFlashbackSpillLimitOnFixture`、`TestFlashbackVerifyMemoryCeiling`、`TestVerifyDoesNotBufferTheScript`、`TestGeneratedReviewRSS`。后两个原来按固定 RSS 上限判、会偶发失败（#351）；#358、#361 改为在固定点测 live heap，`GOMAXPROCS=1` 下也稳定。这些只覆盖 flashback 生成和 verify；analyze 的内存上界还没有测试 | PARTIAL | #364（analyze 内存上界，#367） |
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

共 24 项：**MET 17 / PARTIAL 2 / NOT MET 5**。

## 最卡生产级的 5 项

1. **B5，失败路径的测试 CI 不跑**：apply 失败后的清理（`TestFlashbackFailedApplyDropsAndSignals`）和服务器端的 guard 测试，写好了但 CI 从来不跑。
2. **F2，二进制 commit 为空**：#303，要你用有 workflow 权限的 token 改 `release.yml`。
3. **F3、F4，发布不看 e2e，CI 不跑 -race/vet**：同样卡在 workflow token。
4. **E1、E2，内存与大文件**：analyze 的内存上界还没有测试（#364）；≥100MB 的耗时和内存没有基准，大语料只在 QA 机器上。（E1 的偶发失败 #351 已由 #358、#361 修复。）
5. **D5，机器可读的输出 schema 和 `--tz`**：没有 JSON Schema 或 describe 命令，显示时区不可选（#255 第 3、7 项）。

A3 已由 #370 覆盖（四种格式比较有序的重点事务序列，含不完整证据的一例）。C2–C4 已由 `go test ./...` 里的版本矩阵夹具覆盖；8.4 / 9.x 上执行脚本的往返仍要改 workflow，不在这三项里；MariaDB 上脚本明确拒绝执行。
