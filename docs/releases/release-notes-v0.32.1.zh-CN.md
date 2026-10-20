# BinlogViz v0.32.1 发布说明

发布日期：2026-10-20

## 概述

v0.32.1 在 ADR 0014 的基础上修复 flashback 执行（#346；修复 #338、#341、#342、#343、#344、#345、#347、#348）。撤销仍在一个 XA 事务里执行，执行失败仍然什么都不提交。变化在于撤销如何匹配行，以及操作者如何得知结果：

- `CREATE PROCEDURE` 之前去掉 `PAD_CHAR_TO_FULL_LENGTH` 和 `NO_BACKSLASH_ESCAPES`（#347）。
- 按键匹配的 `UPDATE` 或 `DELETE` 撤销必须恰好影响一行，否则整次执行回滚（#347）。
- `BINARY(n)` 的值用 `0x00` 补齐到 n 字节（#348）。
- 成功以一行锚定的标记为准，而不是退出码 0。执行失败会打印带真实错误的回滚行，并删掉本次的收尾存储过程（#338、#343）。

**v0.32.0 及更早版本写出的 flashback 脚本都要重新生成。** `flashback verify` 会以退出码 6、`details.reason` `old_format` 拒绝它们。在目标库上给执行账号授权：

```sql
GRANT CREATE, CREATE ROUTINE, ALTER ROUTINE ON `binlogviz`.* TO 'account'@'host';
GRANT SELECT, INSERT, UPDATE, DELETE ON `schema`.* TO 'account'@'host';
-- 仅当 automatic_sp_privileges=0 时：
GRANT EXECUTE ON `binlogviz`.* TO 'account'@'host';
```

执行，并按标记行判断成功：

```sh
set -o pipefail
binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4 \
  | tee apply.log
grep -E '^binlogviz: undo committed, [0-9]+ statements$' apply.log
```

## 不兼容变更

- **v0.32.0 及更早版本生成的 flashback SQL 无法通过 verify，请用 v0.32.1 重新生成。** `flashback verify` 把完整的 v0.31 或 v0.32.0 脚本报告为退出码 6、`details.reason` `old_format`，并提示重新生成，而不再报 `truncated`（#342）。修改发布后缀里的收尾 mode 行仍是 `truncated`，因为这一行不在摘要范围内。
- **退出码 0 不代表成功。** 成功是 stdout 上的以下某一行：
  - 没有跳过时：`^binlogviz: undo committed, [0-9]+ statements$`。
  - `--conflict guard` 跳过了行时：`^binlogviz: undo committed with skips, [0-9]+ applied, [0-9]+ skipped$`。
  
  自动化脚本使用 `set -o pipefail` 并用 `grep -E` 匹配锚定的模式。标记由 `CONCAT` 片段拼成，SQL 里没有这段字面量；`mysql --force -v` 会回显语句，对回显做普通 grep 不算提交。
- `schema_version` 仍为 1，`plan_version` 仍为 2，`report_version` 仍为 3。

## 修复

- **执行权限包含 `SELECT`（#341）。** 撤销改动的表上的数据权限是 `SELECT, INSERT, UPDATE, DELETE`。`UPDATE` 和 `DELETE ... WHERE` 需要 `SELECT`（ERROR 1143）。v0.32.0 的说明已加更正。
- **执行失败会明确说出来并清理（#338、#343）。** 收尾回滚，客户端删掉本次的收尾存储过程，输出 `binlogviz: undo rolled back, no rows changed`，捕获到语句错误时后面跟 `(ERROR N: message)`。不加 `--force` 时 mysql 退出码为 1。stderr 是 ERROR 1231，其文本包含真实的错误号和消息，包括 1370、1044 这样的客户端侧错误。存储过程不能删除自己（ERROR 1357），所以删除在收尾返回之后执行。在收尾 `CALL` 之前被 kill、`CALL` 根本没有进入，或者账号删不掉 `SYSTEM_USER` 定义者的过程时，仍可能留下过程；用 `DROP DATABASE IF EXISTS binlogviz` 清理（需要 `binlogviz.*` 上的 `DROP`）。
- **MySQL 8.0 上报告真实错误号。** 收尾读取 `GET STACKED DIAGNOSTICS`，1143、1146、1054 不再被报成 1243。
- **行值无法伪造提交行。** 服务器消息里的控制字符（C0 和 DEL）在写入回滚行和 stderr 副本之前替换为空格。重复键的值里含换行和 `binlogviz: undo committed, 9 statements` 时，`mysql --force` 加 `-r`、`-t` 或 `-vvv` 也不会再打印一行独立的标记。
- **`sql_mode` 不能改变撤销匹配的行（#347）。** 在 `PAD_CHAR_TO_FULL_LENGTH` 下，NO PAD 排序规则（`utf8mb4_0900_ai_ci`、`utf8mb4_0900_bin`）的 `CHAR` 键上的按键撤销匹配 0 行，`INSERT` 撤销照常执行，执行以退出码 0 提交了错误的数据。现在第一条语句保存 `@@SESSION.sql_mode`，并在 `CREATE PROCEDURE` 之前去掉 `PAD_CHAR_TO_FULL_LENGTH` 和 `NO_BACKSLASH_ESCAPES`；存储过程以这个 mode 执行。执行成功或失败都会恢复保存的 mode，`--force` 下也一样。MySQL 5.7 的警告 3090（`MYSQL40`、`POSTGRESQL` 这类已弃用的组合 mode）不算失败。
- **按键撤销必须恰好影响一行（#347）。** 按键的 `UPDATE` 或 `DELETE`（包括无主键时的 `LIMIT 1` 匹配）改动 0 行或多于 1 行时，整次执行回滚，并提示 `keyed undo affected N rows, expected 1`。标记里的 `N` 统计恰好改动一行的撤销语句数。
- **`BINARY(n)` 的值会补齐（#348）。** 在 `WHERE`、`SET`、`INSERT` 中，值都用 `0x00` 补齐到 n 字节写出。v0.32.0 写的是短值（存储为 `6B3100000000` 却写成 `X'6B31'`），按键撤销匹配 0 行，v0.32.0 照样提交了其余部分。以 `0x00` 结尾的满长值（例如 `BINARY(16)` UUID）也已覆盖。`VARBINARY`、`BLOB`、`CHAR` 不补齐。
- **`--conflict guard` 如实计数。** 改动 1 行算已应用，0 行算跳过，其他行数一律回滚。只有至少改动了一行且没有跳过时才打印干净的提交标记；只跳过行的执行打印 `binlogviz: undo skipped, N rows`，不打印提交标记。无主键的 `INSERT` 撤销只在该行镜像不存在时插入，所以第二次执行会跳过，而不是重复插入。
- 不经 `verify` 直接执行发布的文件，会在不提交的结尾处报错，不再打印 `ERROR 0`。与头部断开的块会把会话设为只读，不调用收尾过程。
- **生成阶段的拒绝可被机器读取（#344）。** 退出码 6 的拒绝会设置 `details.reason`（`row_metadata`、`row_image`、`column_type`、`row_capture`、`ddl_in_range`、`generated_unknown`、`generated_mismatch`、`generated_row`、`schema_mismatch`、`sql_context_off`），点名单张表时还会设置 `details.table`。`flashback --plan` 带相同字段；`--plan --sql-context off` 会打印 plan，`would_refuse` 为 true，退出码 6。MySQL 5.7 的 binlog 无法撤销，`--schema-file` 也不能提供行元数据；消息会提示从备份或 PITR 恢复，下次使用 MySQL 8.0.14+ 并设置 `binlog_row_metadata=FULL`。
- **README 和安装脚本（#345）。** README 的退出码与 CLI 一致（输入损坏为退出码 5，flashback 拒绝为退出码 6）。`trend` 默认保持参数顺序，除非设置 `--order time`。`install.sh --help` 会打印用法。

## 验证

BinlogQA 对 #346 做了四轮 PR QA（r2、r3 判 FAIL，r4 在 `41853984` 上 PASS），并在 MySQL 8.0.36 和 5.7.44 上对 main `41853984` 做了 tip QA，没有 P0、P1、P2，也没有新的 P3。全部在 3 GiB 地址空间限制下运行。

- **最小权限流程**：按文档授权，使用官方 `mysql` 客户端、`set -o pipefail` 和锚定的 grep。n316 恢复；rt325 恢复到校验和 3356976996；8 行中有 2 行已漂移的 guard 执行打印 `with skips, 6 applied, 2 skipped`，干净标记的 grep 不匹配；缺表时回滚行给出真实的 ERROR 1146，没有标记，不留存储过程，`XA RECOVER` 为空。
- **`BINARY(n)`（#348）**：refuse 和 guard 都能恢复；`BINARY(6)` 键以 0x00 补齐写出。
- **`sql_mode`（#347）**：8.0 上 17 种、5.7 上 16 种 mode 都能恢复，并恢复保存的 mode。
- **标记伪造**：PR QA 中跨分隔符、客户端参数和字符集，每种服务器锚定匹配的误判为 0/612；tip QA 中为 0/12。
- **失败矩阵**：每种服务器 22 个用例都给出真实的错误号和消息；篡改用例仍是退出码 6；v0.32.0 的脚本报 `old_format`。
- **不变的部分**：analyze 在 5 个 binlog × 4 种格式上与 v0.32.0 逐字节一致（唯一差异是 HTML 的 `Generated` 时间戳）；`trend` 输出一致。
- `go test` / `go vet` / `-race` / `CGO_ENABLED=0` 构建通过。CI 绿。

## 兼容性

- **升级步骤：**
  1. 确认执行账号在撤销改动的表上有 `SELECT, INSERT, UPDATE, DELETE`，在 `binlogviz.*` 上有 `CREATE`、`CREATE ROUTINE`、`ALTER ROUTINE`（`automatic_sp_privileges=0` 时还要 `EXECUTE`）。
  2. 重新生成 v0.32.0 及更早版本写出的 flashback 脚本，`verify` 会以 `old_format` 拒绝旧脚本。
  3. 把自动化改为检查锚定的标记行，而不是 mysql 的退出码。
  4. 如有遗留的收尾存储过程，用 `DROP DATABASE IF EXISTS binlogviz;` 清理。
- `report_version` 仍为 3，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。analyze 输出不变。
- 在 `COMMIT` 之后断开的流会让撤销已提交但没有标记。
- `flashback` 和 `flashback verify` 都从不连接 MySQL。每个会话只执行一个脚本，在主库上、`sql_log_bin=1` 执行。

## 已知问题

- **#303（P2）**：Linux 发布版二进制在 `binlogviz version` 中报告的 `commit` 和 `commit_time` 可能为空。修复在 `.github/workflows/release.yml`，需要有 workflow 权限的维护者处理。请用 `binlogviz_0.32.1_checksums.txt` 而不是 commit 来核对 Linux 二进制。
- `flashback verify` 不接受 `-` 作为 stdin（退出码 4），#326 遗留。
- **#312（P3）**、**#270（P3）** 继续沿用，#255、#256、#257 也一样。

完整的保证表及其限制见 `docs/concept/limitations.zh-CN.md` 和 `docs/adr/0014-flashback-no-partial-commit.md`。
