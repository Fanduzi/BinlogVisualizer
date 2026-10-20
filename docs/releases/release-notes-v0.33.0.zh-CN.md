# BinlogViz v0.33.0 发布说明

发布日期：2026-10-20

## 概述

v0.33.0 修复了 v0.32.0 的 flashback 执行问题。v0.32.0 文档里的权限漏了 `SELECT`，`UPDATE` 和 `DELETE ... WHERE` 撤销会报 ERROR 1143（#341）。在 `PAD_CHAR_TO_FULL_LENGTH` 下，或者键是 `BINARY(n)` 时，按键撤销可能匹配 0 行却照样提交（#347、#348）。`mysql --force` 下执行失败会静默回滚，还可能留下收尾存储过程（#338、#343）。现在每次执行最后都会输出一行，说明发生了什么，自动化脚本检查这一行即可。

**v0.32.0 生成的 flashback 脚本必须重新生成。** `flashback verify` 会以退出码 6 拒绝它们，`details.reason` 为 `old_format`（#342）。因此这次发布的是次版本号，而不是补丁版本。

在目标库上给执行账号授权：

```sql
GRANT CREATE, CREATE ROUTINE, ALTER ROUTINE ON `binlogviz`.* TO 'account'@'host';
GRANT SELECT, INSERT, UPDATE, DELETE ON `schema`.* TO 'account'@'host';
-- 仅当 automatic_sp_privileges=0 时：
GRANT EXECUTE ON `binlogviz`.* TO 'account'@'host';
```

执行并检查结果：

```bash
set -o pipefail
binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4 \
  | grep -E '^binlogviz: undo committed, [0-9]+ statements$'
```

## 不兼容变更

- **v0.32.0 或 v0.31.0 生成的 flashback SQL 无法通过 verify，请用 v0.33.0 重新生成。** `flashback verify` 会把完整的旧脚本报为 `details.reason` `old_format` 并提示重新生成，而不再说它被截断（#342）。
- **`mysql` 退出码 0 不代表成功。** 成功以 stdout 上的一行为准：
  - 没有跳过任何行时为 `binlogviz: undo committed, N statements`（`^binlogviz: undo committed, [0-9]+ statements$`）。
  - `--conflict guard` 跳过了部分行时为 `binlogviz: undo committed with skips, N applied, M skipped`（`^binlogviz: undo committed with skips, [0-9]+ applied, [0-9]+ skipped$`）。
  - 执行失败时为 `binlogviz: undo rolled back, no rows changed`，stderr 会给出真实的错误码和错误信息。
  - 请使用 `set -o pipefail` 加带锚点的 `grep -E`。`mysql --force -v` 会回显语句，对回显做普通 grep 不算提交。

## 修复

- **执行权限包含 `SELECT`（#341）。** 撤销改动的表上的数据权限是 `SELECT, INSERT, UPDATE, DELETE`。v0.32.0 的发布说明已加更正。最小权限 e2e 授予的正是文档列出的权限。
- **按键撤销没有恰好改动一行时，整次执行回滚（#347）。** `CREATE PROCEDURE` 之前会去掉 `PAD_CHAR_TO_FULL_LENGTH`，`CHAR` 键可以正确匹配。默认 `--conflict refuse` 下，按键的 `UPDATE` 或 `DELETE` 撤销影响 0 行或多于 1 行（包括无主键表的 `LIMIT 1` 匹配）时，整次执行回滚。执行结束后会恢复原来的 `sql_mode`，失败时也会恢复。
- **`BINARY(n)` 的值用 `0x00` 补齐到 n 字节（#348）**，`WHERE`、`SET`、`INSERT` 中都如此。v0.32.0 写的是未补齐的值，导致按键撤销匹配 0 行。`VARBINARY`、`BLOB`、`CHAR` 不补齐。
- **执行失败会删除收尾存储过程并明确报告（#338、#343）。** 输出 `binlogviz: undo rolled back, no rows changed`，stderr 给出真实的错误码和信息。如果在收尾 `CALL` 之前被 kill，或 `CALL` 根本没有进入，仍可能留下存储过程。`DROP DATABASE IF EXISTS binlogviz` 需要 `binlogviz.*` 上的 `DROP` 权限。
- **行数据无法伪造提交行。** 标记由 `CONCAT` 拼成，SQL 里没有这段字面量；服务端错误信息里的控制字符在写入回滚行和 stderr 之前会被替换。
- **`--conflict guard` 如实计数。** 改动 1 行计为已执行，0 行计为跳过，其他行数一律回滚。`--conflict guard` 下无主键表的 `INSERT` 撤销只在该行镜像不存在时插入，所以第二次执行会跳过而不会重复插入。
- MySQL 5.7 的 3090 警告（`MYSQL40`、`POSTGRESQL` 等已弃用的 `sql_mode` 组合）不再导致回滚。
- 不经过 `verify` 直接执行发布文件时，不再报 `ERROR 0`；未提交的结尾本身就是错误。
- `flashback --plan --sql-context off` 报告 `would_refuse` 为 true、原因为 `sql_context_off`，并以退出码 6 退出。
- **生成时的拒绝会说明原因（#344）。** 退出码 6 的拒绝会设置 `details.reason`（`row_metadata`、`row_image`、`column_type`、`row_capture`、`ddl_in_range`、`generated_unknown`、`generated_mismatch`、`generated_row`、`schema_mismatch`、`sql_context_off`），涉及单张表时还会设置 `details.table`；`--plan` 输出同样的字段。MySQL 5.7 的 binlog 无法撤销，提示信息会建议从备份或 PITR 恢复。
- README 中的退出码与 CLI 一致（输入损坏为 5，flashback 拒绝为 6）。除非指定 `--order time`，`trend` 保持参数顺序。`install.sh --help` 会打印用法（#345）。

## 验证

BinlogQA 对 #346 做了四轮 PR QA（MySQL 8.0.36 和 5.7.44，所有生成都在 3 GiB 地址空间限制下运行）。前三轮先后发现一个 P1 和若干 P2/P3，均已修复；第四轮在 `41853984` 上通过，0 P0、0 P1、0 P2、0 个新 P3。PR 以快进方式合并，main 即 `41853984`，main 上的 tip QA 也已通过。

- **8.0.36 和 5.7.44 上的最小权限流程**：只授予文档列出的权限，n316 和 rt325 往返都能恢复数据（rt325 的 `CHECKSUM TABLE` 为 3356976996），输出干净的提交行，不留存储过程，`XA RECOVER` 为空。guard 部分跳过时输出 "with skips" 行，带锚点的干净 grep 不会匹配它。缺表时回滚行给出真实的 ERROR 1146，数据不变。
- **#347 / #348**：17 种 `sql_mode` 全部恢复成功并还原原来的模式；`BINARY(6)` 键以 `0x00` 补齐写出并能恢复。
- **标记伪造**：在各种分隔符（LF、CR、NEL、U+2028、U+2029 等）、客户端参数和字符集组合下，每台服务器 612 个用例中带锚点的误判成功为 0。
- **篡改**：截断、追加、修改过的脚本以退出码 6 退出，原因正确，输出 0 字节；v0.32.0 的脚本以退出码 6 `old_format` 退出。
- **大小与速度**：1 GB binlog 生成的脚本为 397 MiB；rt325 执行耗时 16.9 秒，v0.32.0 为 16.2 秒。`verify` 仍为流式处理（416 MB 脚本 VmHWM 50 到 66 MB）。
- **未变化**：`analyze` 的 text、JSON、Markdown、HTML 输出与 v0.32.0 逐字节一致（HTML 的 `Generated` 时间除外）；`trend` 输出一致。
- `go test`、`go vet`、`-race` 和 `CGO_ENABLED=0` 构建均通过，CI 全绿。

## 兼容性

- **升级步骤：**
  1. 在撤销改动的表上为执行账号补充 `SELECT` 权限。
  2. 重新生成 v0.32.0 及更早版本写出的 flashback 脚本，`verify` 会拒绝旧脚本。
  3. 自动化改为检查带锚点的提交行，而不是 `mysql` 的退出码。
  4. 如有遗留的收尾存储过程，执行 `DROP DATABASE IF EXISTS binlogviz;` 清理（需要 `binlogviz.*` 上的 `DROP`）。
- `report_version` 仍为 3，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。`flashback --plan` 的拒绝新增 `details.reason` 和 `details.table`。analyze 输出不变。
- `flashback` 和 `flashback verify` 从不连接 MySQL。每个会话只执行一个脚本，在主库上执行，并保持 `sql_log_bin=1`。
- 默认 `--conflict refuse` 下，无主键表的 `INSERT` 撤销不是幂等的：同一脚本执行两次会插入两次。可能重跑时请使用 `--conflict guard`。

## 已知问题

- **#303（P2）**：Linux 发布包里的二进制在 `binlogviz version` 中 `commit` 和 `commit_time` 为空。修复在 `.github/workflows/release.yml`，需要有 workflow 权限的维护者处理。请用 `binlogviz_0.33.0_checksums.txt` 校验 Linux 二进制。
- `flashback verify` 不接受 `-` 作为 stdin（退出码 4）。
- **#312**、**#270**、**#255**、**#256**、**#257**（P3）延续到下个版本。

完整的保证表及其限制见 `docs/concept/limitations.md` 和 `docs/adr/0014-flashback-no-partial-commit.md`。
