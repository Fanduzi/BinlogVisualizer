# BinlogViz v0.23.24 发布说明

发布日期：2026-10-09

## 概述

v0.23.24 堵上了 `binlogviz flashback` 脚本在执行守卫没通过之后仍可能写入错误行的最后两条路（#197、#191）。守卫现在绑定到它所属的脚本，每一条撤销语句也会检查它。不含守卫的脚本和 `analyze` 的输出与 v0.23.23 相同。

## 变更

- **守卫绑定到脚本（#197）**：守卫只有在本会话里完成检查、所有列都匹配、服务器受支持、并且会话原本可写时，才把 `@binlogviz_ok` 设成由脚本自身内容算出的令牌。每个事务块只有持有这个令牌才会解锁。同一个会话里别的 binlogviz 脚本留下的解锁语句，不再能让没有脚本头的块解锁。
- **每条撤销语句都检查令牌（#197）**：`INSERT` 撤销写成 `INSERT ... SELECT ... FROM DUAL WHERE @binlogviz_ok <=> '<令牌>'`，`UPDATE` / `DELETE` 在 `WHERE` 里加上 `AND @binlogviz_ok <=> '<令牌>'`。在没有令牌的会话里执行的语句不改任何行：交互式客户端在块中间重连之后是这样，在加锁本身报 `ERROR 1399` 的 `XA START` 事务里也是这样。
- **只读会话里给出明确提示（#191）**：如果会话在执行脚本前已经是只读的，例如前一份脚本的守卫没通过，这份脚本会停在自己的守卫，提示 `binlogviz: this session is already read-only so the script cannot write. Disconnect and apply the script again in a new session`，而不是每条写入都只报 `ERROR 1792`。不匹配时，守卫的提示行会加上 `This session is now read-only: disconnect, fix the schema file, and apply the script again in a new session.`

## Bug 修复

- #197：在已经执行过匹配的 binlogviz 脚本的会话里粘贴没有脚本头的块，这个块会自己解锁并写入错误的行，退出码 0。现在报 `ERROR 1792`，不改任何行。
- #197：交互式 `mysql` / `mariadb` 客户端或 `mysqlsh --interactive` 在块中间重连时，这个块剩下的语句会在新的、可写的会话里提交。现在这些语句不改任何行，之后的每个块都报 `ERROR 1792`。
- #191：自动重连会丢掉会话锁（块之间的情况已在 v0.23.23 修复，块中间的情况在本版修复），XA 事务可能提交错误的行，之后在已锁定会话里执行的正确脚本只报 `ERROR 1792`。

## 验证

- CI 任务 `flashback e2e`（MySQL 8.0）：`--- PASS: TestFlashbackRoundTripMySQL80`（新增 `STALE_TOKEN` 和 `RECONNECT_DML` 两个场景）和 `--- PASS: TestNumericDecodeMySQL80`，没有跳过。
- 实机矩阵：MySQL 8.0.46、5.7.44、5.7.19 和 MariaDB 10.6.28、10.11.19、11.4.13，对比 v0.23.23 与本版。在 v0.23.23 上，正确脚本之后再执行没有脚本头的错误目标块、伪造的残留解锁、在客户端提示符里于块中间 `KILL` 连接、`XA START`，都会写入错误的行，退出码 0。在本版上，这些场景全部不改行，GTID 集合不变。正确的 schema 文件在 `mysql`、`mysql --force`（autocommit 开和关）、`source`、粘贴到会话、以及遇错继续的执行器（autocommit 开和关）里都还原出相同的行。
- #183 绕过矩阵（MySQL 8.0.46）：62 次错误转储执行（除 `mysqlsh` 外的所有客户端模式）全部不改行，GTID 集合不变。

## 破坏性变更

正确执行时没有。含守卫的脚本里，`INSERT` 撤销从 `INSERT ... VALUES` 改为 `INSERT ... SELECT ... FROM DUAL WHERE @binlogviz_ok <=> '<令牌>'`，`UPDATE` / `DELETE` 多一个 `WHERE` 条件。只执行这类脚本里的 DML 行、不带脚本头时，现在不改任何行。在已经是只读的会话里执行含守卫的脚本，现在会停在守卫。

## 兼容性说明

- 执行支持 MySQL 5.7 及以上和 MariaDB 10.2 及以上。MySQL 5.6 和 MariaDB 10.1 仍然停在守卫并保持只读。
- `flashback` 从不连接 MySQL。先审阅并测试脚本，再带着脚本头在主库的一个新会话里执行，并保持 `sql_log_bin=1`。某条语句失败时，脚本里更早的事务已经提交。
- JSON `report_version` 仍是 `3`。快照、工作流、产物命名和支持平台与 v0.23.23 一致。

## 已知问题

完整说明和变通办法见 `docs/concept/limitations.zh-CN.md`。

- 目标正确时，块中间重连会让这个块没有还原：断开前的语句回滚，剩下的语句什么都不改。从这个块开始，带上脚本头，在新会话里续跑。
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167)：解析到的 binlog 里的 `ALTER` 会再应用到已经包含它的转储上。用这次 `ALTER` 之前的转储，或者不要传入包含它的 binlog。拼接的多库转储只使用第一个 `Database:` 头。
- [#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180)：非严格会话写入的行在严格会话下可能以 `ERROR 1292` 或 `ERROR 1365` 失败，此时更早的事务已经提交。用原会话的 `sql_mode` 执行。
- [#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187)、[#192](https://github.com/Fanduzi/BinlogVisualizer/issues/192)、[#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193)：库名带空格的 `Database:` 头和 `lower_case_table_names` 下的大小写混用名字；守卫报错里的非 ASCII 列名；`div_precision_increment` 不是默认值时的 `DECIMAL` 操作数。
- 执行失败后接着跑：保留脚本头（第一个 `-- gtid:` 之前的所有行），只删掉失败那一块上面的事务块，在新会话里执行「脚本头 + 失败的那一块及其下面的全部」。
