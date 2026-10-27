# 找到并撤销误改的行

本页收录原先写在项目 README 里的行镜像、DDL 时间线和 flashback 细节。已知限制见 [限制与边界](../concept/limitations.zh-CN.md)，参数见 [CLI 参考](../reference/cli.zh-CN.md)。

flashback 是实验功能：执行生成的 SQL 之前必须先审阅。

## 找到误删并看到被删的行

```bash
binlogviz analyze mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  --start "2026-10-06 14:00:00" \
  --end "2026-10-06 14:10:00" \
  --show-rows
```

`--dml` 接受 `insert`、`update`、`delete`，用逗号组合。它和 `--include-table` / `--exclude-table`、`--include-schema`、`--start` / `--end`、位点、GTID 过滤一起生效。Summary、Top Tables、Top Transactions、Top Threads 和告警只统计保留下来的类型，报告里会写明这个过滤。类型过滤没有匹配时退出 2，`Error: dml filter matched no events`。

`--show-rows` 默认关闭。打开后，列出的每个事务会打印 DELETE 的前镜像、UPDATE 里发生变化的列（`before -> after`），以及 INSERT 的后镜像。MySQL 8 且 `binlog_row_metadata=FULL` 时显示列名；否则列是 `@1`..`@N`，报告会说明为什么没有列名。值有上限（每个事务 32 行；字符串和二进制值最多 64 字节；整数、小数和 `BIT` 完整打印），截断会标明，并给出省略的行数。`--sql-context off` 会一并省略这些值，并在报告里说明。事务上的 `mysqlbinlog_cmd` 仍然在，用来对照。报告本身不打印撤销 SQL；同样的选择条件交给 `binlogviz flashback` 才会打印。`TIMESTAMP` 列是存储时刻的 UTC 墙钟（含小数秒），不跟随本机时区。`DATETIME` 仍是 binlog 里写下的墙钟。

## 找到误操作的 DROP

```bash
binlogviz analyze mysql-bin.000123
```

DDL 时间线列出 `DROP TABLE`、`TRUNCATE` 和 `ALTER`，并带上该事务的 GTID，以及事务起点的文件字节。复制 `BinlogServer stop_gtid=<gtid>` 或 `mysqlbinlog --stop-position=<N> <file>`。这个停止点回放更早的事件，并且不包含这条 DDL。binlog 里没有 GTID 时，中文界面写「GTID 不可用」，英文界面写 `GTID unavailable`，位置提示仍然在。时间线不生成撤销 SQL。选定范围内有 DDL 时，`binlogviz flashback` 会拒绝。

## 撤销误改的行

先用 `analyze`、DDL 时间线或 `--show-rows` 找到那个事务，再打印只撤销这些行变更的 SQL。先审脚本，再自己执行。flashback 不连接数据库。

```bash
binlogviz analyze mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  --show-rows

binlogviz flashback mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  > flashback.sql

binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4
```

`--dml`、`--include-table`、`--include-schema`、`--start` / `--end`、`--start-position` / `--stop-position`、`--include-gtids` / `--exclude-gtids` 与 `analyze` 是同一套选择条件。`--schema-file` 不是过滤器：它提供表定义，flashback 仍然离线。binlog 里的名字全是小写、文件里只有大小写不同的同名表时（`lower_case_table_names=0` 的转储），flashback 采用这张表，并在警告里写出文件中的表名。DELETE 变成前镜像的 `INSERT`。INSERT 变成 `DELETE`。UPDATE 把行设回前镜像，`WHERE` 用后镜像的主键，这样改过主键的行仍能对上当前行。语句按 binlog 逆序：最后一个事务在前，事务内最后一行在前。整个撤销是一个 XA 事务，文件本身不会 `COMMIT`。用 `binlogviz flashback verify` 来应用。注释里有原 GTID（binlog 没有 GTID 时写 `GTID unavailable`）和 `file:start-position`，即文件名和该事务起点字节，用来和 `mysqlbinlog` 对照。

binlog 必须是 `binlog_row_metadata=FULL` 且 `binlog_row_image=FULL`。flashback 需要 `binlog_row_metadata=FULL`（MySQL 8.0.14+）。MySQL 5.7 的 binlog 无法撤销，`--schema-file` 也不提供行元数据。MySQL 8.4 LTS 和 9.x 与 8.0 相同：FULL 镜像可以撤销，MINIMAL 镜像或选中范围内的 DDL 会被拒绝。MariaDB 10.11 在这两项都是 FULL 时，生成的撤销语句是准确的（它的 JSON 列在 binlog 里记成 `LONGTEXT`），但脚本不能在 MariaDB 上执行：flashback 会给出警告（`mariadb_apply_unsupported`），在 MariaDB 服务器上脚本开头几条语句就以 ERROR 1231 `binlogviz: flashback apply does not support MariaDB; no rows were changed; see docs/concept/limitations.md` 停下，不改任何数据，`mysql --force` 也一样。MariaDB 11.8 即使 `binlog_row_image=FULL` 也不把生成列写入行镜像，这张表会按镜像不全拒绝；`--include-table` 可以把它排除。脚本会设置 `utf8mb4`、`time_zone='+00:00'`（`TIMESTAMP` 字面量是 UTC 墙钟），并在创建收尾过程之前去掉 `PAD_CHAR_TO_FULL_LENGTH` 和 `NO_BACKSLASH_ESCAPES`，成功执行后再把保存的模式设回去。没有主键的表按每一列匹配并加 `LIMIT 1`，语句上方的注释会说明。被删行的 `INSERT` 不用 `LIMIT 1`。JSON 按二进制文档重建，包括 `binlog_transaction_compression=ON` 记下的事务。非 `utf8mb4` 字符串是字符集引导符加十六进制字节。`ENUM` 写成成员序号，`SET` 写成位掩码，因此 `latin1` 或 `gbk` 的值在严格和非严格 `sql_mode` 下都精确。`ENUM` 序号 0 是非严格插入留下的错误成员，严格模式会报 `ERROR 1265`。脚本先保存 `@@SESSION.sql_mode`，只在这一条语句去掉 `STRICT_TRANS_TABLES`、`STRICT_ALL_TABLES` 和 `TRADITIONAL`，然后把保存的模式设回去。`TRADITIONAL` 也要去掉，因为 MySQL 会把它展开回那两个严格模式。解析到的 `CREATE` 或 `ALTER`，或者 `--schema-file`（`mysqldump --no-data`，或 `SHOW CREATE TABLE`）点名的生成列，只有定义和 binlog 对得上时才不写入 `INSERT` 和 `UPDATE`。MySQL 8 的行元数据不标记生成列，`FULL` 镜像同时包含虚拟列和存储列的值，因此会拿记下的值和表达式核对。schema 文件必须是事故当时的表结构。flashback 核对列数、列名、顺序，以及 binlog 里有的类型，不一致就不打印 SQL。生成列在每一行的记录值都等于表达式时省略。能核对的是整数和 `DECIMAL` 的 `+`、`-`、`*`、`/`、`DIV`、`%`、`MOD`，以及 `UPPER`、`LOWER`、`CONCAT`、`CONCAT_WS`、`LENGTH`、`CHAR_LENGTH` 和简单的 JSON 提取。`/` 赋给整数时远离零的一半进位，所以 `5 / 2` 是 `3`，`-5 / 2` 是 `-3`。`/` 赋给 `DECIMAL(p,s)` 时按 MySQL 默认的除法宽度（整数操作数保留 9 位小数），再按列的标度远离零的一半进位，所以 `DECIMAL(40,4)` 里的 `5 / 2` 是 `2.5000`，`DECIMAL(40,9)` 里的 `1 / 7` 是 `0.142857142`。记下的值与表达式矛盾时以退出码 6 拒绝，不打印 SQL，包括 `div_precision_increment = 0` 时写下的商。结果放不进列类型、且记下的值是非严格模式的截断端点时，省略该列并给出警告。表达式无法核对时省略该列，stderr 给出警告，三条 `SET` 之后的守卫会在目标上这一列不是生成列时让执行失败。遇错继续的客户端会被改成只读会话，后面的写入都会失败。记下的值恰好等于能核对的表达式时，要等这个守卫跑起来才分得开真实列和生成列。没有主键的 `WHERE` 仍比较生成列。普通的 `mysqldump --no-data db` 不必带 `USE`：库名来自转储头的 `Database:`（多份转储拼在一起时，每个头管它后面那份）、`--schema-file-db`，或 binlog 里该表唯一的库。多份 `SHOW CREATE TABLE` 可以放在同一个文件里，中间没有 `;`。库名有歧义时只警告，不用这份定义。选中的表没有定义时，脚本仍列出每一列，stderr 警告不能排除生成列，并且执行可能在 `ERROR 3105` 停下，撤销会回滚。

`ON DELETE` / `ON UPDATE CASCADE` 的子表行不会被还原。撤销时触发器会执行。选中的行如果在扫描的 binlog 中之后又被修改过，就是冲突：flashback 以退出码 7 拒绝，不输出脚本。`--conflict guard` 仍输出脚本，每条撤销语句都匹配日志里的后镜像（之后被改过的行保持不动，旧主键已被后来的行占用时，改回主键的撤销也不动）；`--plan` 输出 JSON 摘要而不是 SQL。扫描范围之外的修改看不到。表过滤或 `--dml` 只撤销事务的一部分时，stderr 打出警告。先审阅并测试脚本，再在主库的同一个会话里执行，并保持 `sql_log_bin=1`。某条语句失败时，撤销会回滚，`mysql --force` 也一样（缺表或缺列不会把已经成功的语句提交掉）。撤销是一个 XA 事务：它还开着的时候，`BEGIN`、`START TRANSACTION` 和 DDL 会以 ERROR 1399 失败，不会把它提交（#328、#329、ADR 0014）。直接 `mysql < flashback.sql` 不会提交，完整的文件也一样：结束过程会回滚 XA 事务，然后结尾报错。`binlogviz flashback verify` 拒绝被改过、截断、重排或追加的文件（退出码 6），并且只有它会允许结束过程提交，而且仅当每一条语句都成功。执行需要在 `binlogviz.*` 上具备 `CREATE`、`CREATE ROUTINE` 和 `ALTER ROUTINE`，在撤销改动的表上具备 `SELECT, INSERT, UPDATE, DELETE`，以及官方 `mysql` 客户端（脚本使用 `DELIMITER`）。`automatic_sp_privileges=0` 时，还要授予 `binlogviz.*` 上的 `EXECUTE`。`UPDATE` 和 `DELETE ... WHERE` 需要 `SELECT`（没有时是 ERROR 1143）。`verify` 每次执行都会新建一个结束过程，回滚之后也会删掉它，所以残留过程不会挡住下一次执行。回滚的执行会打印 `binlogviz: undo rolled back, no rows changed`。不加 `--force` 时 mysql 退出码为 1。什么都没跳过时，成功是匹配 `^binlogviz: undo committed, [0-9]+ statements$` 的一行。有跳过时，成功是匹配 `^binlogviz: undo committed with skips, [0-9]+ applied, [0-9]+ skipped$` 的一行。标记由 CONCAT 拼出，字面量不在 SQL 里。`mysql --force -v` 会回显语句，对回显做普通 grep 不能当成提交。自动化用 `set -o pipefail` 和 `grep -E`。退出码 0 但没有匹配的那一行不算成功。N 是恰好改了一行的撤销语句数。带键的 `UPDATE` 或 `DELETE`，包括无主键的 `LIMIT 1`，必须恰好影响一行，否则撤销回滚。`--conflict guard` 把影响 1 行算作已应用，0 行算作跳过，其它行数算作回滚。只有至少改了一行且没有跳过时，才打印不带 skips 的成功标记。`BINARY(n)` 的值会补 0x00 到 n 字节；`VARBINARY`、`BLOB` 和 `CHAR` 不补。`COMMIT` 之后流被截断时，撤销已经提交，但没有这一行。管理员用 `DROP DATABASE binlogviz` 清掉残留。生成失败时不会写出脚本字节。拷贝到 shell 重定向的中途失败可能留下一个前缀；这个前缀没有提交结尾，后面的 `BEGIN` 或 DDL 也不会把它提交。已知限制见 [docs/concept/limitations.zh-CN.md](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/concept/limitations.zh-CN.md)：schema 文件把真实列标成生成列时，若记下的值恰好等于能核对的表达式，这一列仍会被省略并以退出码 0 结束（[#166](https://github.com/Fanduzi/BinlogVisualizer/issues/166)）。矛盾的值会拒绝。仍然以退出码 0 结束的错误转储，会在任何事务之前被执行守卫拦住。遇错继续的客户端会被改成只读会话，后面的写入都会失败；断开连接，改好 schema 文件，在新会话里重新执行。事故之后如果把这一列改成了生成列，原来的值写不回去。非严格 `sql_mode` 下写入的行（零日期，或生成列 `a/b` 且 `b` 为 0）在严格会话里也能还原：只有那一条语句去掉会拒绝存下的值的那个标志（[#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180)）。不传 `--schema-file` 仍会给真正的生成列赋值，执行可能停在 `ERROR 3105`。还原 `ENUM` 序号 0 时，这一条语句会去掉 `TRADITIONAL` 和严格模式，然后再把保存的 `sql_mode` 设回去，包括 `sql_mode=TRADITIONAL`（[#168](https://github.com/Fanduzi/BinlogVisualizer/issues/168)）。执行失败后不要从失败的 `-- gtid:` 块接着跑。撤销是一个事务，失败会回滚；`mysql < flashback.sql` 不会提交。`TIMESTAMP` 字面量是 UTC，脚本会设置 `time_zone='+00:00'`。细节写在那份文档里。

列名缺失、前镜像或后镜像不完整、某一列无法精确写成字面量（`FLOAT`、`DOUBLE`、`GEOMETRY`、`VECTOR`、不完整或无法精确表示的 JSON、未知字符集，或缺少有无符号、字符集或 ENUM/SET 成员）、表定义出现过但读不出来、`--schema-file` 和 binlog 的列不一致、生成列的值与表达式矛盾，或者选定范围内有 DDL 时（使用表或库过滤时，DDL 点名的任何一张表可能是被选中的表就拒绝，包括 `RENAME TABLE` 列表里靠后的一对和多表 `DROP TABLE` 里靠后的表名；语句读不完整时也拒绝，见 ADR 0015），flashback 以退出码 6（kind refused）拒绝，只打一行 `Error:`，不打印任何 SQL。`--sql-context off` 同样拒绝：脚本就是行值。什么都没选中时退出 2，`Error:` 与 `analyze` 相同（`schema/table filter matched no events`、`dml filter matched no events` 或 `window matched 0 events`），stdout 为空。账号 DDL 的 `<secret>` 打码仍作用于 `analyze` 里的语句文本。flashback 不打印这些语句；没有表或库过滤时，选定范围内的账号 DDL 会按 DDL 拒绝。单元格的值不打码。不运行 `flashback` 时，`analyze` 的文本、Markdown、JSON 和 HTML 不变。
