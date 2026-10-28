# Analyze 行为细节

本页收录原先写在项目 README 里的 `analyze` 细节规则。README 只保留命令；精确规则写在这里。参数见 [CLI 参考](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/cli.zh-CN.md)，报告字段见 [输出格式参考](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/output-format.zh-CN.md)。

## 样例 binlog 与发布包内容

同一份 1500 字节样本在仓库的 `cmd/binlogviz/testdata/minimal.binlog`。GitHub Release 的每个 tar.gz 也会带上这份文件（`testdata/minimal.binlog`）、discovery 布局副本 `testdata/sample-binlog/mysql-bin.000001`、`from_dir` 指向该目录的 `incident.yaml`、`README.md`、`README_ZH.md`，`docs/agents/USING-BINLOGVIZ.md`（和中文指南），以及 README 链接到的文档页：`docs/reference/analyze-behavior`、`docs/recipe/flashback`、`docs/recipe/compare-trend-workflow` 和 `docs/concept/architecture`，每页都带 `.zh-CN.md` 版本。解压后执行 `./binlogviz analyze testdata/minimal.binlog` 和 `./binlogviz workflow run incident.yaml`，不需要再 clone 仓库。给代理的用法从 `docs/agents/USING-BINLOGVIZ.md` 开始。

## 从 stdin 读取 binlog

`analyze -` 从管道读取一份二进制 binlog。解析需要可 seek 的文件，所以会把 stdin 复制到临时文件，命令结束时删除，包括 SIGHUP（退出码 129）、Ctrl-C（退出码 130）、SIGQUIT（退出码 131）和 SIGTERM（退出码 143）。终端会在解析前失败。`/dev/null` 和空管道报 `stdin 没有数据`。这种输入的回放提示会说明来自 stdin，不会编造文件路径。`mysqlbinlog` 的文本输出不是 binlog。

## 热点线程

默认文本报告包含热点线程，有行变更时按行数排序（否则按事件数、字节或事务数）。binlog 里有的 `thread_id`、`server_id`、`user@host` 和 schema 会写出来，回答「谁写最多」不必再 `jq`。`--top` 限制这一节；`--top-threads 0` 保留全部会话。JSON 的同一排名在 `threads`。

## 热点行

同一份报告还有热点行：被 UPDATE 和 DELETE 行镜像碰到次数最多的主键。每一条给出次数、碰到它的事务数、第一次和最后一次的事件时间，以及最早和最晚那个事务的 GTID 和 file:byte，可以直接拿去跑 `mysqlbinlog` 或 BinlogServer。主键只来自 MySQL 8 `binlog_row_metadata=FULL`。binlog 没有记下主键时，报告会说明这张表无法追踪热点行，不会拿第 1 列 `@1` 来猜。没有主键的表不进这个排名。`--top` 限制这一节；`--top-rows` 覆盖它（`0` 保留已追踪的全部键）。`--sql-context off` 隐藏键值，次数仍在。JSON 字段是 `hot_rows`。追踪最多保留 8192 个主键。超出后，新键替换被碰到次数最少的键；报告会说明达到了上限，继承了被丢掉键的计数的行标成近似。一直留在表里的键计数是精确的。

## 退出码

`analyze` 在计入至少 1 个事件时退出 **0**；无法分析（损坏、截断、没有 Format Description）时退出码 5（kind corrupt）；完整 binlog 解析成功但计入 0 个事件（空的 `--start`/`--end` 窗口，或仅 Format Description / rotate）时退出 **2**。schema 或 table 过滤没有匹配到事件同样是 exit 2，`Error:` 会写明过滤没有匹配。`--dml` 没有匹配到事件也是 exit 2，`Error: dml filter matched no events`。过滤匹配到 view、event、function、procedure 或 trigger 时退出 0，即使没有行变更也会打印这条 DDL。exit 2 不写 `stdout`，只在 `stderr` 打一行 `Error:`。如果进度条还停在当前 stderr 行，打印 `Error:` 之前会先清掉那一行。

## 没有时区的时间窗口

`--start`/`--end` 也接受 `YYYY-MM-DD HH:MM:SS`。没有时区的 --start 或 --end，在设置了 --tz 时按 --tz 读取，包括 --tz UTC；没有设置 --tz 时，按运行 binlogviz 的机器的本地时区读取。跨机器请优先使用带显式偏移的 RFC3339。

## 表过滤的名字

`--include-table` / `--exclude-table` 接受 `TABLE` 或 `SCHEMA.TABLE`，view、event、function、procedure、trigger 也是同样的写法。`CREATE TRIGGER` 和 `DROP TRIGGER` 都用触发器名字，过滤时传这个名字。

## 为什么副本在延迟？

```bash
binlogviz analyze mysql-bin.000123
```

「无主键」一节列出收到了 UPDATE 或 DELETE、并且没有主键的表，按这些行数排序。副本应用这些行时可能全表扫描。`主键是否存在未知（binlog_row_metadata 不是 FULL）` 表示 binlog 没有记下主键信息，不是在说某张表没有主键。没有主键但只有 INSERT 的表会点名，但不会被排成这种风险。

MySQL 8.0 及以上、并且打开了 `log_replica_updates` 的副本 binlog，在最忙分钟之后有「副本应用延迟」。延迟是本机提交时间减去原始提交时间，并假设源库和副本的时钟一致。这一节给出最大延迟、p95、延迟达到峰值的分钟，以及最慢的事务：GTID、事务起点 file:byte、两个提交时间、延迟，以及该事务里的表。`--top` 限制列出几条。两个时间戳相同的源库 binlog 只说一行，没有表。MySQL 5.7 和 MariaDB 打印 `提交时间戳不可用`。

## Analyze 性能门槛

面向故障排查时，单个 1 GB binlog 的 `analyze` 目标耗时是目标 DBA 环境上的 10 秒。超过 15 秒应视为性能失败，并用 `pprof` 做剖析。热点行追踪不会为文件里的每一行建一张表：最多保留 8192 个主键。超出后，新键替换被碰到次数最少的键，报告把继承来的计数标成近似。

默认 `--detail-store none` 与 `--detail-store duckdb` 生成的 JSON 等价，同时峰值 RSS 降低约 38%（在 988 MB MySQL 8.0 ROW binlog 上测量）。总耗时仍主要受 parser/流式聚合限制。

建议的人工检查命令：

```bash
time binlogviz analyze /path/to/mysql-bin.000044 --format text > /tmp/binlogviz-text.txt
time binlogviz analyze /path/to/mysql-bin.000044 --format html --output /tmp/binlogviz.html
```

文本输出应保持在快速诊断路径上；HTML 输出则构建完整的可视化证据报告。

## Release artifact

权威 release artifact 由 GitHub Actions release workflow 产出。macOS 产物在原生 runner 上构建，Linux 产物则在 manylinux2014 用户态中构建，以保持对 CentOS 7 / glibc 2.17 的兼容基线。本地 `goreleaser` 更适合做配置校验和当前宿主机的可选验证，不是主要发布路径。
