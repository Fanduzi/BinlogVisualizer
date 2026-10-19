# BinlogViz v0.32.0 发布说明

发布日期：2026-10-20

## 概述

v0.32.0 实现了 ADR 0014：flashback 执行失败时，永远不会提交撤销的一部分。撤销在一个 XA 事务里执行，脚本里或脚本之后的 `BEGIN`、`START TRANSACTION`、DDL 都无法把它提交；只有每条语句都成功，收尾才会提交（#328、#329、#332）。执行所需的权限写进了文档并降到最小，每次执行使用独立的收尾存储过程，`XA START` 之前的失败在 `mysql --force` 下也会一直记着（#333、#334、#335、#337）。

**执行现在需要新的权限，v0.31.0 生成的脚本必须重新生成。** 在目标库上给执行账号授权：

```sql
GRANT CREATE, CREATE ROUTINE, ALTER ROUTINE ON `binlogviz`.* TO 'account'@'host';
-- 仅当 automatic_sp_privileges=0 时：
GRANT EXECUTE ON `binlogviz`.* TO 'account'@'host';
```

执行方式不变：`binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4`。

## 不兼容变更

- **v0.31.0 生成的 flashback SQL 无法通过 verify，请用 v0.32.0 重新生成（ADR 0014）。**
  - 执行时如果 `binlogviz` 库不存在会先创建，并在其中创建收尾存储过程。执行账号需要 `binlogviz.*` 上的 `CREATE`、`CREATE ROUTINE`、`ALTER ROUTINE`；`automatic_sp_privileges=0` 时还需要 `binlogviz.*` 上的 `EXECUTE`。撤销改动的表仍需要 `INSERT`、`UPDATE`、`DELETE`。
  - 8.0 上不需要 `XA_RECOVER_ADMIN`，5.7 上不需要 `SUPER`。执行不需要该库的 `DROP` 权限。
  - 必须使用官方 `mysql` 客户端（`DELIMITER`）。
  - `schema_version` 仍为 1，`plan_version` 仍为 2。

## 修复

- **执行失败时永远不会提交撤销的前缀（#328、#329、#332、ADR 0014）。**
  - 目标缺表或缺列时，`verify | mysql --force` 不再提交部分撤销（#328）。
  - 截断的文件后面再跟另一个脚本、`BEGIN`、`START TRANSACTION` 或 DDL，以及 `--force` 下完整文件后面跟这些，都不再提交撤销（#329）。在打开的 XA 事务里执行 `BEGIN` 报 ERROR 1399。
  - 撤销是一个 XA 事务。只有每条语句都成功，收尾才提交，否则回滚。执行中途 kill 会话，`XA RECOVER` 为空。
- **执行权限写进文档并降到最小（#333）。** `DROP PROCEDURE IF EXISTS` 即使过程不存在也需要 `ALTER ROUTINE`（ERROR 1370）。`binlogviz.*` 上的 `CREATE` 覆盖 `CREATE DATABASE IF NOT EXISTS binlogviz`。不使用 `XA RECOVER`。同一套授权在 MySQL 5.7 和 8.0 上都适用。agent 指南（中英文）列出了 `ALTER ROUTINE`。
- **`XA START` 之前的失败在 `mysql --force` 下也会一直记着。** 以前缺 `ALTER ROUTINE` 时 `DROP` 报 1370，紧接着的 `CREATE PROCEDURE` 成功把错误清掉，撤销就提交了。现在存储过程段里的每条语句都会把失败记到一个标志里，之后的成功语句无法清除，收尾不会提交。
- **每次执行使用独立的收尾存储过程（#334）。** `verify` 把它命名为 `binlogviz.binlogviz_fb_end_<token>_<nonce>`，并在结束时删掉。遗留的过程，包括 `SYSTEM_USER` 账号拥有的过程（MySQL 8.0 上 ERROR 1227），不会挡住下一次执行。新脚本以 `SQL SECURITY INVOKER` 创建该过程。
- `binlogviz flashback verify --help` 打印的是该命令自己的用法 `flashback verify <file>`（#335）。

## 验证

BinlogQA 对 #332 的 PR QA（`68c32d52`，MySQL 8.0.36）和对 main `68c32d52` 的 tip QA（MySQL 5.7.44），以及对 #337 的 PR QA（`3338ed61`；此前在 `7f1d1212` 判 FAIL 并发现 `--force` 漏洞，8.0.36 和 5.7.44），都通过，没有 P0 或 P1。main 为 `3338ed61`。全部在 3 GiB 地址空间限制下运行。

- **没有部分提交（#328、#329）**：缺表或缺列 4/4，加不加 `--force` 兄弟表都不变（ERROR 1146 / 1054）；#329 的各种形态在 #337 上 14/14（在 `68c32d52` 的 5.7 上 20/20）表都不变。在 v0.31.0 的 main `f1a62e23` 上，这些会提交前缀或整段撤销。
- **权限（#333）**：按文档授权，8.0 和 5.7 都恢复到事故前状态，不留存储过程；`automatic_sp_privileges=0` 加 `EXECUTE` 也可以。缺任何一项权限，加不加 `--force`，数据都不变，`XA RECOVER` 为空。同一轮 A/B 对比中，缺 `ALTER ROUTINE` 加 `--force`，在 `7f1d1212` 上会提交，在 `3338ed61` 上表不变，两种服务器都一样。
- **遗留过程（#334）**：root 遗留的过程不会挡住执行；20 次执行得到 20 个不同的 nonce。
- **回滚往返**：n316 恢复；带生成列的 rt325 在 8.0 和 5.7 上都恢复到校验和 3356976996。
- **不变的部分**：篡改用例仍是退出码 6；637 MB 脚本上 `verify` RSS 为 34.3 / 34.1 MB（SQL / JSON）；`BINLOGVIZ_MEMORY_BYTES=1` 退出码 10；analyze 与 `68c32d52` 逐字节一致；66 例矩阵退出码和 kind 一致；`plan_version` 2。
- `go test` / `go vet` / `-race` / `CGO_ENABLED=0` 构建通过。CI 绿。

## 兼容性

- **升级步骤：**
  1. 给执行账号授予 `binlogviz.*` 上的 `CREATE`、`CREATE ROUTINE`、`ALTER ROUTINE`；如果 `automatic_sp_privileges=0`，再授予 `binlogviz.*` 上的 `EXECUTE`。
  2. 重新生成 v0.31.0 及更早版本写出的 flashback 脚本，`verify` 会拒绝旧脚本。
  3. 如有遗留的收尾存储过程，用能删除它们的账号清理（8.0 上定义者带 `SYSTEM_USER` 时，清理账号也需要 `SYSTEM_USER`；`DROP DATABASE` 需要 `DROP` 权限）：`DROP DATABASE IF EXISTS binlogviz;`。下一次执行会重新建库，库里只有这些辅助对象。
- `report_version` 仍为 3，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。analyze 输出不变。
- `verify` 输出的 SQL 与文件不逐字节相同：每次执行的收尾过程名和结尾 hex 都不同。
- `flashback` 和 `flashback verify` 都从不连接 MySQL。每个会话只执行一个脚本，在主库上、`sql_log_bin=1` 执行。
- 手工修改 `verify` 输出的 SQL 不在保证范围内（ADR 0014「Not guaranteed」）。

## 已知问题

- **#338（P2）**：部分缺权限的路径会留下本次执行的收尾过程 `binlogviz.binlogviz_fb_end_<token>_<nonce>`（缺 `ALTER ROUTINE` 加 `--force`、缺 `INSERT`/`UPDATE`/`DELETE` 不加 `--force`、`automatic_sp_privileges=0` 时缺 `EXECUTE`）。数据不变，也不会挡住之后的执行。用 `DROP DATABASE IF EXISTS binlogviz` 清理。
- **#303（P2）**：Linux 发布版二进制在 `binlogviz version` 中报告的 `commit` 和 `commit_time` 为空。修复在 `.github/workflows/release.yml`，需要有 workflow 权限的维护者处理。请用 `binlogviz_0.32.0_checksums.txt` 而不是 commit 来核对 Linux 二进制。
- `flashback verify` 不接受 `-` 作为 stdin（退出码 4），#326 遗留。
- **#312（P3）**、**#270（P3）** 继续沿用，#255、#256、#257、`.github/workflows/README.md` 的归档清单和 #285 的 `install.sh` P3 也一样。

完整的保证表及其限制见 `docs/concept/limitations.zh-CN.md` 和 `docs/adr/0014-flashback-no-partial-commit.md`。
