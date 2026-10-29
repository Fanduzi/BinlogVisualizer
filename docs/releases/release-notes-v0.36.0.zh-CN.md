# BinlogViz v0.36.0 发布说明

发布日期：2026-10-28

## 概览

v0.36.0 是质量版本：关闭 17 个 P3 issue，修复生成列校验与 DDL 报告中的多个真实正确性问题，新增一个输入发现参数，并实证加固了发布门。`report_version` 仍为 4，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。

## 新增

- **`analyze --from-dir DIR --index-file NAME`(#256)。** 目录发现现在可以直接读服务器的 `binlog.index` 清单，不再只有前缀匹配。清单条目相对 `--from-dir` 解析（也接受绝对路径），按清单顺序读取；条目缺失或已被轮换会显式失败，而不是静默漏掉一段历史。`--index-file` 与 `--prefix` 互斥；`--from-dir` 必须搭配且只搭配其中一个选择器。
- **`landing-overflow` CI job(#451)。** landing 页在无头 Chromium 里按 320/768/1280px 视口加载，`documentElement.scrollWidth` 超过视口即失败 —— 这是只看 markup 的守卫看不到的布局溢出。
- **macOS CI。** 完整测试套件在 `macos-14` 上以 `darwin verify` 运行，Linux-only 的测试假设不会再悄悄破坏 darwin 用户。
- **difftest 夜间工作流**(`difftest-nightly.yml`),MySQL 镜像钉到 `8.0.46`/`8.4.13`，不再用浮动的 `8.0`/`8.4` tag。
- **CI 统一 `go test -count=1`**(`verify`、`darwin verify`、`vet and race`),缓存结果不能再冒充新跑的结果。

## 修复 —— 正确性

- **生成列校验中的 UNSIGNED 算术(#450)。** 列的 `unsigned` 标志在到达表达式检查器之前就丢了，无符号列上的 `a-b` 可能按 MySQL 会拒绝的负数结果通过校验（ERROR 1690)。现在 `+`、`-`、`*`、`DIV`、`%` 和一元负号在两个求值域都按无符号界 `[0, 2^64-1]` 执行；越界的操作数或结果一律拒绝验证。
- **`/` 除法结果按 `div_precision_increment` 标记为 DECIMAL(#450)。** `CONCAT(7/7)` 现在按 `'1.0000'` 校验、`7/2` 按 `'3.5000'`，与 MySQL 一致。
- **再补四种赋值类型(#450)。** YEAR 接受 `0`/`0000`;DATETIME 字面量赋值按 MySQL 半进位规则舍入小数秒；定长 BINARY 用 `0x00` 补齐；生成表达式里的 `_latin1` 类 introducer 字面量不再求值 —— 这些字节在 `SHOW CREATE TABLE` 输出里本来就有损。dump 安全的 UTF 家族(`utf8`、`utf8mb3`、`utf8mb4`、`ascii`)introducer 仍然求值。
- **`CREATE/ALTER/DROP/RENAME USER` 的扇出(#421)。** USER DDL 里点名的每个账户都会带上正确的表归属上报，包括多账户语句和多对 `RENAME TABLE`。
- **`describe` 失败输出钉死 `valid:false`**,不再带出过期默认值。
- **`--start`/`--end` 偏移量变更的错误信息**本地化，不再漏出英文模板。
- **HTML 分钟图标签**在分析区间跨午夜时带日期前缀。
- **形似时间戳的 `workload_id`** 在 `snapshot save` 后保持原样，不再被折叠。
- **GTID 上限消息**在 per-server 上限不是约束项时不再说 "can need fewer"。
- **拒绝摘录**折叠空白；单行语句保持单行。
- **报告排序(#255)。** text 和 Markdown 里 Top Transactions 提到 Hot Rows 之前 —— 尖峰排查时事务排名是首要答案。HTML 保持面向 DBA 的导航分区不变。

## 修复 —— 发布流程与门

- **发布门无法被静默(#420),经真实 GitHub 运行验证：** 在 tag 上注入 e2e 失败后，4 个 build 与 release job 全部跳过（run 40718489351);e2e job 上加 PR-only `if:` 或 `continue-on-error:` 都会在 e2e 运行前被 `TestFlashbackE2EJobCannotBeSilenced` 抓住（run 40718744800、40722108847)；单独重跑被跳过的 build 仍保持 skipped,`rerun --failed` 会先重跑 `ci`。
- **`TestVetRaceAndGuardWiringPresent`** 钉住 vet-race job 与 guard-SQL 接线；**`TestBuildVerifiesCheckoutMatchesTag`** 钉住每次发布构建前的 `git rev-parse HEAD` 和 `git status --porcelain` 干净树检查。
- **`release-prep` 自检**在 `pipefail` 下运行（管道中途失败过去会漏过），版本号扫描与 Go landing 守卫共享同一份白名单(`scripts/landing-allowed-numbers.txt`）和同一套令牌形状规则（#451)。
- **README 守卫改为白名单(#447)** 而非禁词列表 —— QA 的 122 个探针里曾有 38 个漏过。归档内离线链接检查覆盖所有随包 Markdown 页面，12 个指向未打包文件的相对链接改为 GitHub 绝对链接。
- **`codehealth-compare`** 在失败路径上不再顺手更新基线；lint 行号归属正确的文件；尾部赋值标识符归一化（`mark` → `pos`)；已提交的 submodule 或嵌套仓库现在计为 finding 而不是静默漏掉（#454)。
- **codehealth 账本**按内容推导身份、逐项记录上限，文件移动记 lineage，不再把移动当成新的超限文件。

## 架构

- **`internal/detailstore`(ADR 0012 第 6 步，#257)。** DuckDB 适配器移出 `internal/analyzer`;analyzer 不再 import `go-duckdb`,cmd 层注入 `*detailstore.DuckDBStore` 即可；`CGO_ENABLED=0` 仍可通过 stub 编译。
- 巨型文件继续拆分：`analyze.go` 拆为四个领域文件，`workflow.go` 拆为输出与执行两半，`planner.go` 拆为渲染与拒绝两半，`expr.go` 拆为 JSON 与 DECIMAL 两个域 —— 全部回到行数上限内且行为不变。

## 测试与 CI

- 新增回归:index 清单解析及全部非法选择器组合、无符号算术边界、四种赋值类型、USER DDL 账户扇出、报告段落顺序、e2e 门 mutation、checkout 校验。
- mutant pin:lexer 引号跳过、数字开头的词、版本注释数字边界、GTID 上限 reason、`analyze` 与 `workflow describe` 的第二失败形态(#402、#403、#418)。
- `docs/production-readiness.md`:24 MET / 0 PARTIAL / 0 NOT MET。

## 验证

- **CI**:PR #455 的 run 40725552689(ci:verify、vet and race、darwin verify、flashback e2e 73 过、difftest、landing overflow、release-prep self-test）与 run 40725552716(lint、codehealth）全绿；合入 main 是该树的 fast-forward。
- **真实 GitHub 门验证(#420):** 带注入 e2e 失败的 tag push 未产生 Release 和任何 build;dispatch 运行确认两种静默 mutation 都会被静态锁抓住。
- **本地：** `go test ./...` 全绿、`go vet` 干净、`make codehealth-compare` 过基线、release-prep 自测 68 例全过。

## 兼容性

- `report_version` 4、`plan_version` 2、`schema_version` 1 均不变；JSON 字段无增删。
- text 和 Markdown 报告里 Top Transactions 排在 Hot Rows 之前；JSON 与 HTML 内容不变。
- `analyze` 新增 `--index-file`（增量）;`--from-dir`/`--prefix` 配对报错替换为同时点名两个选择器的报错。
- 产物名与支持平台不变。

## 已知问题

- **#256(P3,roadmap)仍开着：** 远程复制读取、forward/redo SQL、CSV/TSV 统计、按表/事务拆分输出、行级过滤、语句指纹 —— 各项均为功能级改动，需要各自的设计。其中两项已在本版本落地：`--index-file` 发现与 `--tz`。
