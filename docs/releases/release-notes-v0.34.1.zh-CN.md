# BinlogViz v0.34.1 发布说明

发布日期：2026-10-20

## 概述

v0.34.1 是补丁版本，修复带过滤的 flashback 中的一个 P1。**如果你用 `--include-table`、`--exclude-table`、`--include-schema` 或 `--exclude-schema` 运行 `flashback`，请升级。** v0.34.0 及更早版本的对象过滤只按 DDL 写在最前面的表判断范围内的 DDL，所以把选中表写在后面的 `RENAME TABLE` 或 `DROP TABLE` 不会被拒绝，撤销可能把行写进错的表（#368，ADR 0015）。本版本还给每个命令的 `--help` 加上可执行的示例，并用测试保证没有命令会提问或等待 stdin（#365）。analyze 输出、不带过滤的 flashback、`schema_version` 1、`plan_version` 2、`report_version` 3 和退出码与 v0.34.0 相同。

## 修复

- **P1：带过滤的 flashback 现在会拒绝在任何位置写到选中表的 DDL（#368，ADR 0015）。** 范围内有 DDL 时 flashback 会拒绝（退出码 6，`ddl_in_range`），因为它不撤销表结构变更。v0.34.0 在有表或库过滤时只按 DDL 的第一个名字决定保留还是丢弃，被丢弃的 DDL 根本到不了拒绝检查。在 MySQL 8.4.11 上：

  ```sql
  DELETE FROM rn.t1 WHERE id=1;
  RENAME TABLE rn.t2 TO rn.tmp, rn.t1 TO rn.t2, rn.tmp TO rn.t1;
  DELETE FROM rn.t1 WHERE id=5;   -- 这一行来自原来的 rn.t2
  ```

  `flashback --include-table rn.t1` 退出码为 0，`verify | mysql` 打印 `binlogviz: undo committed, 2 statements`，原 rn.t2 的行被插进 rn.t1，rn.t2 仍然缺这一行。`DROP TABLE rn.z, rn.t1` 之后再 `RENAME TABLE rn.t2 TO rn.t1`，以及对这次互换用 `--exclude-table rn.t2`，结果相同。不带过滤，或选中表正好是 DDL 的第一个名字时，原本就会拒绝。

  现在范围内的每条 DDL 都会交给 flashback planner。有过滤时，它读出 DDL 作为源或目标写到的每张表（`RENAME TABLE` 列表的每一对、多表 `DROP TABLE` 的每个名字、`ALTER TABLE ... RENAME TO`、`REFERENCES`、`CREATE TABLE ... LIKE`、`CREATE`/`DROP INDEX ... ON`、`TRUNCATE`），以及 `CREATE`、`ALTER`、`DROP DATABASE` 涉及的每个库，只要其中任何一个可能包含选中的行就拒绝。不带库名的表名按会话的默认库解析，include 列表匹配时忽略大小写。读不全的语句一律拒绝，并在消息里说明。只写到被过滤掉的表的 DDL 仍然放行。不带表或库过滤、以及只用 `--dml` 时，拒绝规则和消息不变。

## 新增

- **每个命令的 `--help` 都有可执行的示例（#365，属于 #255）。** `TestEveryCommandHasExample` 遍历命令树，包括 `help`、`completion`、`snapshot`、`workflow` 和 `flashback verify` 的子命令，命令没有示例或 `--help` 没有打印示例就失败。`TestCommandExamplesRun` 在仓库夹具上执行每条示例。`flashback` 的示例不再带 `--include-gtids <gtid>` 占位符，改为在上方注释里说明用 `--include-gtids` 传入 GTID。workflow 示例改用相对目录 `incident-out`，不再用 `/tmp/incident`。
- **没有命令会提问（#365）。** `TestCommandsDoNotPrompt` 在没有 TTY、stdin 阻塞的情况下运行每个命令，卡住或要求确认就失败。`TestNoInteractivePromptAPI` 禁止提示库、`fmt.Scan` 和 `ReadPassword`，只允许 `analyze -` 读 stdin。`TestFlashbackPlanRefusalPrintsReasons` 检查 `flashback --plan` 在退出码 6 时仍然打印带 `would_refuse` 和 `refusal` 的计划 JSON。这些是测试，命令行为不变。

## 验证

BinlogQA 的 PR QA 在 `29b01043` 上通过了 #365 和 #368。该提交是发布提交的父提交，也是 main 的最新提交，因此同时算作 tip QA。0 个 P0、0 个 P1、0 个 P2。

- **P1**：在在线的 MySQL 8.4.11 和 8.0.36 上，表互换、多表 `DROP` 加 `RENAME`、`ALTER TABLE ... RENAME` 三个 binlog 都以退出码 6 `ddl_in_range` 拒绝，不输出 SQL。在线采集了约 60 种 DDL 形态，所有危险形态都拒绝：后面的和跨库的 `RENAME` 对、多表 `DROP`、`DROP`/`CREATE`/`ALTER DATABASE`、`TRUNCATE`、`CREATE ... LIKE`、`REFERENCES`、视图、触发器、索引、反引号转义、`ANSI_QUOTES` 和 `/*! */` 注释。无关表上的 DDL 放行，撤销执行正确。没有任何形态产生错误的 SQL。
- **其他行为不变**：260 次不带过滤和只用 `--dml` 的 flashback 与 v0.34.0 逐字节一致。analyze 输出 1914/1914 一致，mf、mi、pc、n316 语料的 flashback 样本逐字节一致。
- **变异测试**：只查第一个名字、忽略 rename 目标、关闭库级检查、读不全的语句放行，每个变异都会让测试失败（14/14 被杀）。#365 中，示例带错误参数和加入 `ReadPassword` 提示都能被测试发现。
- `go test ./...` 在默认 `GOMAXPROCS` 和 `GOMAXPROCS=1` 下都通过。CI 全绿。

## 兼容性

- 对不带过滤的 flashback 和 analyze 没有不兼容变更。`report_version` 仍为 3，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。退出码、快照、产物名称和支持的平台不变。
- 带过滤的 flashback 会拒绝更多情况：上面列出的范围，以及范围内有读不全的 DDL 的情况，即使这条 DDL 作用在别的表上。要对这样的范围做 flashback，请用 `--start-position`、`--stop-position` 或 `--exclude-gtids` 缩小范围，把这条 DDL 排除在外。
- `--help` 给原来没有示例的命令加了示例，`flashback` 和 `workflow` 的示例措辞有调整。
- v0.33.0 到 v0.34.0 生成的 flashback 脚本可以直接 verify 和执行。如果你用 v0.34.0 或更早版本，从包含 `RENAME TABLE` 或多表 `DROP TABLE` 的范围生成过带过滤的 flashback 脚本，请在执行前用 v0.34.1 重新生成。

## 已知问题

- **过滤下拒绝过于保守（P3）**：无关表上读不全的 DDL，例如 `ADD PARTITION ... VALUES LESS THAN`、`EXCHANGE PARTITION`、`CREATE TABLE ... SELECT`、`CHECK (...)`、`FOREIGN KEY`，或 `NO_BACKSLASH_ESCAPES` 下带反斜杠的注释，会让选中表的 flashback 被拒绝。这是 ADR 0015 的失败即拒绝规则。
- **退出码是 2 而不是 6（P3）**：带过滤的范围里有作用于选中表的 DDL、但没有它的行事件时，flashback 以退出码 2（`schema/table filter matched no events`）退出，而不是 6 `ddl_in_range`。不会输出 SQL，但消息里没有提到这条 DDL。
- 整条语句写在可执行注释里（`/*!80000 RENAME ... */`）时以退出码 1 `Unclassified QUERY` 退出，与 v0.34.0 相同。
- **#303（P2）**：Linux 发布包里的二进制在 `binlogviz version` 中 `commit` 和 `commit_time` 为空。请用 `binlogviz_0.34.1_checksums.txt` 校验 Linux 二进制。
- **#352**：text 和 Markdown 选出的重点事务不同。`compare` 和 `trend` 的格式分支仍有 default 分支（#357）。
- `flashback verify` 不接受 `-` 作为 stdin（退出码 4）。
- **#270**、**#255**、**#256**、**#257**（P3）延续到下个版本。

flashback 的保证及其限制见 `docs/concept/limitations.md`、`docs/adr/0014-flashback-no-partial-commit.md` 和 `docs/adr/0015-flashback-ddl-under-a-table-filter.md`。
