# BinlogViz v0.35.1 发布说明

发布日期：2026-10-26

## 概述

v0.35.1 是补丁版本。DDL 里的名字现在由同一个标识符读取器按服务器自己的规则读取（#412），所以服务器生成的多表 `DROP TABLE` 不再报出一个不存在的表（#410），带引号、`ANSI_QUOTES` 引号、数字开头和写在版本注释里的名字都按服务器的读法读取。`workflow describe --format json` 失败时打印的文档能通过它发布的 schema（#407）。发布流水线现在先在打 tag 的 commit 上跑 vet、`-race`、apply 失败与 guard SQL 测试和完整的 flashback e2e，通过后才构建，每个发布包都报告它构建自哪个 commit（#419）。至此 `docs/production-readiness.md` 24 项全部 MET。`report_version` 仍为 4，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。

## 修复

- **多表 `DROP TABLE`（#410，关闭 #375）。** 服务器生成的多表 `DROP TABLE` 以前按点号切分，`analyze` 和不带过滤的 `flashback` 会报出 ``z`,`db.t1`` 这样不存在的表。现在按标识符列表读取。`analyze` 在 JSON、text、Markdown 和 HTML 里每个表一个 DDL 目标。flashback 仍以退出码 6 拒绝、不输出 SQL，消息和 `details.table` 列出每个表。
- **所有 DDL 名字共用一个标识符读取器（#412，关闭 #411）。** `mysqlexpr.Reader` 是唯一的 DDL 名字读取器；`CREATE`、`ALTER`、`RENAME`、`TRUNCATE`、`DROP`，单个名字或多个名字，都用它。
  - 双写的反引号在每种语句里都按同样方式还原，`` `we``ird` `` 是一个表；带引号的名字不会被当成关键字（`` DROP TABLE `if`,`t2` `` 可以读）。
  - `ANSI_QUOTES` 取自 query 事件的 `sql_mode`，服务器把 `"db"."t"` 当名字时这里也当名字。
  - 版本注释按服务器规则处理：只有服务器版本已知、且不低于注释版本时，`/*!NNNNN */` 的内容才算数。MySQL 读 5 位数字，8.1 起第 6 位后面是空白时读 6 位；MariaDB 的 `/*!` 和 `/*M!` 读 5 位，第 6 个字符是数字时读 6 位。多出的数字留在注释内容里，所以 `/*!800369_t ... */` 不会再藏住服务器实际执行的 rename。已知不是 MariaDB 的服务器上，`/*M! */` 是普通注释；flavor 未知时读不了。
  - 不带引号的名字可以以数字开头（`2fa_codes`）；纯数字或指数（`1e5`）不是名字。
  - `CREATE`、`ALTER`、`DROP USER` 保留账号的原样写法，包括引号。
  - 不带库名的名字取 query 的默认库；query 没有记录库名时取事件的库名。
  - flashback 的拒绝消息只列前 5 个名字，后面写「and N more」，并转义控制字符（原始 TAB 写成 `\t`，U+202E 和 U+0085 也会转义）。`details.table` 仍列出全部名字。
  - text、Markdown、HTML、JSON `ddl_event_series`、`compare` 和 `trend` 里的 DDL 计数按语句计。一条语句有多个目标时写成 `4 targets in 2 statements`，跟随报告语言。时间线仍每个目标一行。
  - 带过滤的 flashback 不再因为只涉及其他表的带版本注释的 `TEMPORARY` drop 或 `ANSI_QUOTES` drop 而拒绝，因为这些语句现在读得出来。版本未知或没有 `ANSI_QUOTES` 位时仍读不了，仍会拒绝。
- **`workflow describe` 失败 JSON 符合 schema（#407，关闭 #404）。** plan 加载失败时，`workflow describe --format json` 和 `workflow validate --format json` 打印 `{schema_version, valid: false, error}`。describe 的 schema 现在是成功对象与这个失败对象的 `oneOf`，文档能通过校验。stdout 字节不变。`TestJSONFailureOutputMatchesSchema` 对每个 JSON 命令跑一次失败，要求打印的文档通过校验，或 stdout 为空。

## 发布流水线与 CI（#417，经 #419 合入）

- **B5**：flashback e2e 在 MySQL 8.0 上跑 `TestFlashbackFailedApplyDropsAndSignals` 和 guard SQL 测试（`TestGuardMessageOnServer`、`TestGuardMatchOnServer`、`TestGuardMessageCutSweepOnServer`），有 SKIP 或缺 PASS 就失败。
- **F4**：`vet and race` job 在每个 PR 和每次 push main 时跑 `go vet ./...` 和 `go test -race -count=1 ./...`。
- **F2（#303）**：4 个发布构建都用 ldflags 写入 tag 所在 commit 及其时间，`binlogviz version --format json` 报告的不是这个 commit 时，发布 smoke 失败。Linux 发布包不再报告空的 `commit`。CI `verify` 在每个 PR 上用同样方式构建。
- **F3**：`release.yml` 在 tag 所在 commit 上跑完整 CI（包括 MySQL 8.0 的 flashback e2e），通过前不开始构建。`TestReleaseWaitsForFlashbackE2E` 锁住这条依赖。
- `docs/production-readiness.md`：B5、F2、F3、F4 为 MET；24 MET / 0 PARTIAL / 0 NOT MET。

## 验证

BinlogQA 的 PR QA 通过了 #407、#410、#412 和 #417，main `bc807826` 上的 tip QA 通过，0 个 P0、0 个 P1、0 个 P2（新提一个 P3，#421，v0.35.0 已存在）。本发布 commit 在 `bc807826` 之上只加了这份说明和 CHANGELOG。

- **构建**：go1.26.10，`go mod verify` 通过，`go vet` 无问题，`go test -count=1 ./...` 通过；按发布参数构建的二进制报告 `bc8078264ac2`、`modified: false`。`pack_release_archive.sh` 和 `release_smoke.sh` 通过，19 个失败用例的退出码和 stderr 与 v0.35.0 相同。
- **语料**（680 个文件、3914 个任务）：每个任务的退出码与 v0.35.0 相同。81 个文件上的 313 个任务输出不同，都来自上面的变更：多表 `DROP` 拆分（#410）、带引号和 ANSI 引号的名字（#412）、像数字的限定名改为失败即拒绝（#418），以及拒绝消息现在写出正确的表。
- **Schema**：13 个发布的 schema 与 `docs/schema` 一致；868 份文档（433 份 analyze v4 报告、422 份 flashback plan）用 ajv（Draft 2020-12，strict）和 python-jsonschema 4.26.0 校验通过。
- **在线 guard 攻击**：MySQL 8.0.36、5.7.44、8.4.11 与 MariaDB 11.8.9、10.11.19 上 0 个漏放。8.0 和 8.4 上各有 16 个带过滤的 flashback 运行由 v0.35.0 的拒绝变为退出码 0，都是 #412 有意的精度提升（DDL 只涉及其他表）。多表 `DROP` 得到 4 个正确事件，而不是 1 个错乱的事件。数字开头的名字在 5 个服务器上 0 个漏放。
- **8.0.36 上的 flashback**：202 行往返，校验和一致；`undo.sql` 与 v0.35.0 逐字节相同。
- **CI**：`bc807826` 的 PR 和 push 运行全绿，包括 vet、`-race`（0 失败、0 竞争）和 flashback e2e（73 通过、0 失败、0 跳过，含 apply 失败与 guard SQL 测试）。每个新门禁（B5、F4、F2、F3）各做一个变异，CI 都变红。

## 兼容性

- `report_version` 仍为 4，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。JSON 没有增删字段。
- 含多表 `DROP TABLE`、带引号或 ANSI 引号的名字、或带版本注释 DDL 的 binlog，`analyze` 输出会变：DDL 目标更多、表名正确，一条语句有多个目标时按语句计数。
- 带过滤的 flashback 拒绝得更少：读得出来、只涉及过滤范围外的表的 DDL 不再拒绝。读不全的仍拒绝（退出码 6）。
- 像数字的带库名的名字（`db.1e5`、`db.0x1f`、`db.123`）读成 `table: null`，所以对无关表做带过滤的 flashback 时退出码是 6 而不是 7。这是失败即拒绝（#418）。
- 产物名称和支持的平台不变。Linux 发布包现在在 `binlogviz version` 中报告 commit。

## 已知问题

- **#418（P3）**：#412 QA 提出的 DDL 读取器后续：像数字的限定名读成 `table: null`（失败即拒绝）；`compare` 的新增/删除列表每条语句只写第一个目标；analyze 把多对 `RENAME` 只记到第一对上（flashback 仍拒绝）；部分变异没有测试覆盖。
- **#420（P3）**：#417 QA 提出的 CI 清单后续：F3 的锁定测试只检查 workflow 文本，B5/F4 的接线没有被锁住，发布 smoke 不检查 `modified`。
- **#421（P3，v0.35.0 已存在）**：`DROP USER a, b` 只记第一个账号，`RENAME USER` 不记账号，账号在 Top Tables 里被当成会话库下的表。
- **#399**、**#400**、**#403**（P3）延续到下个版本。`compare` 和 `trend` 的格式分支仍有 default 分支（#357）。`flashback verify` 不接受 `-` 作为 stdin（退出码 4）。**#255**、**#256**、**#257**（P3）延续到下个版本。

参见 `docs/adr/0015-flashback-ddl-under-a-table-filter.md`、`docs/production-readiness.md` 和 `docs/concept/limitations.md`。
