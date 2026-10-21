# BinlogViz v0.35.0 发布说明

发布日期：2026-10-21

## 概述

v0.35.0 是次版本。**对 `analyze --format json` 的使用方有不兼容变更：`report_version` 升到 4（#379）。** 匹配到的 GTID 证据改成区间集合，不再每个匹配事务记一条，所以带 `--include-gtids` 或 `--exclude-gtids` 的 `analyze` 和 `flashback` 在数百万个匹配时不再耗尽内存。本版本还新增 `binlogviz schema` 和 `--tz`（ADR 0017，#255）；`analyze` 的内存不再随事务数增长（#364、#376）；四种格式列出同一批重点事务（#352）；修复时间戳倒退时同一分钟被拆成两条、最忙分钟被低估的问题（#377）；flashback 脚本在 MariaDB 上以明确的错误停下；并用 Go 1.26.10 和 `mapstructure` v2.4.0 构建以修复安全问题。`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。

## 不兼容变更

- **`report_version` 为 4（#379）。** 每个匹配事务一条的 `selection.matched_gtids` 换成：
  - `selection.matched_gtid_set`，区间集合：MySQL 按 server UUID 写成 `uuid:1-4:6-3000000`；MariaDB 按连续的序列号写成 `domain-server-seq` 或 `domain-server-first..last`，按 server 分开，或由写同一个 domain 的多个 server 共用。
  - `selection.matched_transactions`，在时间和位置窗口内至少有一个事件的匹配事务组的个数，每读到一次算一次。同一个 GTID 读到两次算两次，匹配的 DDL 组、以及在第一行之前被 `--stop-position` 截断的组也算，所以它可能大于 `summary.total_transactions`。

  不带 GTID 选择器的报告只有 `report_version` 变了。`compare` 和 `trend` 能读 v3 和 v4 报告，v3 和 v4 快照可以在 `compare`、`trend`、`snapshot list` 和 `snapshot show` 里混用。要求 `report_version` 为 3 的使用方需要接受 4。
- **GTID 选择器下新增退出码 10（#379）。** 带 `--include-gtids`/`--exclude-gtids` 的 `analyze` 和 `flashback`，当匹配到的 GTID 需要超过 100,000 个独立区间时，以退出码 10 `resource`、`details.reason` `matched_gtid_intervals` 退出，不再无限增长。消息已本地化，并给出需要的区间数。MariaDB 取按 server 和共用两种形式中较小的一个，并在存下让运行停止的那个事务之后计数（ADR 0018），所以序列号之间有空档的多个 writer 报告的是真实需要的区间数，而不是共用形式的代价。

## 新增

- **`binlogviz schema` 和 `--tz`（ADR 0017，#255）。** `binlogviz schema` 列出并打印 13 种带版本的 JSON 输出各自的 JSON Schema（Draft 2020-12），同样的文件在 `docs/schema/*.json`，并打进发布包。未知名称退出码 3。`--tz` 选择渲染时间戳用的 IANA 时区。不传 `--tz` 或传 `--tz UTC` 时，输出仍写 ` UTC` / `Z`，与之前逐字节相同。`Local`、空值和未知时区退出码 3。
  - 不带时区的 `--start`、`--end`：设置了 `--tz`（包括 `--tz UTC`）时按 `--tz` 解释，没有设置时按运行 binlogviz 的机器本地时区解释。带偏移或结尾为 ` UTC` 的值就是该时刻。
  - 落在夏令时空档或重叠里的时间，以及偏移超过 14:00、或分钟不是 00、30、45 的偏移，退出码 3。v0.34.1 会静默接受落在机器时区夏令时空档里的无时区时间。
  - 快照文件始终是 UTC，`snapshot save` 也一样。`snapshot list`/`show` 和 compare HTML 的 Minute 单元格跟随 `--tz`；compare 和 trend 的文本输出不跟随。
- **版本矩阵夹具（#366）。** MySQL 5.7.44、8.4.11、9.7.2 和 MariaDB 10.11.19、11.8.9：`analyze` 与各自服务器的 `mysqlbinlog -vv` 解码对照，flashback 的边界（FULL 可撤销；MINIMAL、DDL、缺少行元数据、11.8 生成列拒绝）在 CI 中测试。
- `flashback` 遇到 MariaDB binlog 时给出 `mariadb_apply_unsupported` 警告。

## 变更

- **四种 analyze 格式列出同一批重点事务（#352，ADR 0012）。** 文本、Markdown、JSON、HTML 都列出 `--top-transactions` 排名。文本不再换成另一组最大/最长/最宽事务，HTML 证据也不再提到列表之外的事务。列表里同时是被截断的冠军的事务保留标签（`Largest transaction`、`Longest transaction`、`Widest transaction`）。JSON 的 `transactions`、`transactions_listed` 和 `transactions_omitted` 不变。很多 binlog 的文本输出里，重点事务的行会重新排序、增加或减少。
- **`analyze` 内存有上界（#364、#376）。** 按表、按分钟的事务计数不再保留 key 集合；热点行 map 会重建，delete/insert 反复也不会让它变大；apply-delay 的 P95 在内存里最多留 8192 个值，其余写到已 unlink 的临时文件（每个事务约 8 字节临时盘，建不了临时文件时退回内存）。溢写的值读不回来时，`analyze` 以 `read spilled apply-delay values` 失败。带 `--include-gtids` 或 `--exclude-gtids` 时，在事务开头的 GTID 事件就做出取舍，事件直接流过，30 万个行事件的单个事务不再以退出码 2 崩溃。输出逐字节不变。`TestAnalyzeRetainedHeap`、`TestAnalyzeGTIDFilterRetainedHeap`、`TestAnalyzeMatchedGTIDSetHeap` 和 `TestLargeInputBaseline`（生成的 100MB binlog，按每事件堆和 CPU 比值把关）在 CI 中运行。
- **MariaDB。** flashback 脚本在 MariaDB 服务器上执行时，开头就以 ERROR 1231 `binlogviz: flashback apply does not support MariaDB; no rows were changed; ...` 停下，不改任何数据，`mysql --force` 下也一样（ADR 0016）。以前会停在无关的 `ERROR 1064`，或在 `--force` 下以 `ERROR 1758` 回滚。缺少行元数据的拒绝消息改为提示 MariaDB 10.5+ 的 `binlog_row_metadata=FULL`。`--include-gtids` 和 `--exclude-gtids` 接受 MariaDB 的 `d-s-first..last` 区间，所以 `matched_gtid_set` 可以直接传回。`compare` 在范围检查前按 domain 和 server 合并 MariaDB GTID 选择器，`0-7-1,0-7-2` 与 `0-7-1..2` 不再报 `incompatible_scope`。`..` 两侧有空白一律拒绝。
- `docs/production-readiness.md`：E1、E2、D5 为 MET；MET 20 / PARTIAL 1 / NOT MET 3，还剩 4 项。

## 修复

- **时间戳倒退时同一分钟不再列两次（#377）。** 跨分钟边界仍未结束的事务，其事件带的是事务开始的时间，而之后的提交已经进入下一分钟。v0.34.1 这时会给已经有桶的分钟再开一个桶，把该分钟的行数和事务数拆开。最忙分钟因此被低估，尖峰检测可能漏掉它，也可能因为被拆开的桶误报。现在乱序的事件合并进已有的分钟。行数和事务总数不变，但这类 binlog 在所有格式里的按分钟时间线、最忙分钟和尖峰告警会变化：在 BinlogQA 的 663 个文件的语料里，11 个文件去掉了 36 个重复分钟，MariaDB `mdev6020` 的尖峰告警从 10 条变为 1 条。

## 安全

- 用 Go 1.26.10 构建。govulncheck 报告 binlogviz 可达的 24 个 Go 1.26.1 标准库漏洞，包括 GO-2026-6809（`crypto/x509`）、GO-2026-6599/6600（`html/template`）、GO-2026-6603/6611/6612/6613/6617（`net/http`）和 GO-2026-6604（`os`）。从源码构建需要 Go 1.26.10 及以上。
- `github.com/go-viper/mapstructure/v2` v2.2.1 -> v2.4.0（间接依赖，经由 `go-duckdb` v1.8.5），修复 GO-2025-3787 和 GHSA-2464-8j7c-4cjm：解码错误消息不再回显原始输入值。

## 验证

BinlogQA 的 PR QA 通过了范围内的每个变更，main `478e6993` 上的 tip QA 通过，0 个 P0、0 个 P1、0 个 P2（新提一个 P3，#404）。

- **按发布方式构建**：go1.26.10、`mapstructure` v2.4.0、`-trimpath`、CGO。`pack_release_archive.sh` 和 `release_smoke.sh` 通过；发布包新增 `docs/schema/*.json`，`binlogviz schema` 与 `docs/schema` 逐字节相同。
- **Schema**：394 份 analyze 文档，以及所有 snapshot、compare、trend、flashback plan 和 verify、version、workflow JSON，用 ajv 和 python-jsonschema 校验，0 失败。
- **`--tz`**：在在线 MySQL 8.0.36 和 5.7.44 生成的 binlog 上，4 种格式乘 5 种 `--tz` 设置都以 0 退出；不传 `--tz` 与 `--tz UTC` 输出相同；6 个不同时区写法的窗口选中同样的 8432 行。
- **8.0 上的 flashback**：4 个事务、1431 行；`flashback verify | mysql` 打印 `binlogviz: undo committed, 1431 statements`，`CHECKSUM TABLE` 与事故前一致。计划与 v0.34.1 相同。
- **MariaDB 上限**：在发布二进制上跑 22 个用例、110 次；每次触及上限都以退出码 10 退出、stdout 为空，三个有空档的 writer 在 en 和 zh-CN 下都报告 "at least 100001"。
- **与 v0.34.1 对比**（663 个语料文件、4437 次运行）：退出码 0 处差异，每处输出差异都属于上面列出的预期变更（#370 重点事务、#377 分钟、MariaDB 停止块和警告、`report_version`）。
- `go test ./...` 通过；`-race` 0 个 race。CI 全绿。

## 兼容性

- `report_version` 为 4；`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。退出码 10 `matched_gtid_intervals` 是新增的。其余退出码与 v0.34.1 相同，只是落在夏令时空档或重叠里的无时区 `--start`/`--end` 退出码 3。
- v0.34.1 及更早版本的 v3 快照仍可在 `compare` 和 `trend` 中读取。
- v0.33.0 到 v0.34.1 生成的 flashback 脚本在 MySQL 上仍可直接 verify 和执行。它们不带 MariaDB 停止块，在 MariaDB 上仍按老方式安全失败（`ERROR 1064`，`--force` 下 `ERROR 1758`，不改数据）；要得到明确的 ERROR 1231，请用本版本重新生成。v0.32.0 及更早版本的脚本仍需重新生成。
- 产物名称和支持的平台不变。

## 已知问题

- **#399（P3）**：`snapshot save` 会把任何形似 RFC3339 的字符串改写成 UTC，包括 `--workload-id`，之后 `compare` 会报 `workload_identity_mismatch`。
- **#400（P3）**：`--start`/`--end` 的夏令时和偏移错误在 zh-CN 下没有翻译，并且把非夏令时的时差变化也称作夏令时。
- **#403（P3）**：MariaDB GTID 上限的按 server 消息说被放弃的共用形式「可能需要更少」区间，即使它需要的一样多或更多。数字是对的。
- **#404（P3）**：计划加载失败时，`workflow describe --format json` 输出的错误文档不符合它自己发布的 schema。
- **#303（P2）**：Linux 发布包里的二进制在 `binlogviz version` 中 `commit` 和 `commit_time` 为空。修复在 `.github/workflows/release.yml`，需要有 workflow 权限的维护者。请用 `binlogviz_0.35.0_checksums.txt` 校验 Linux 二进制。
- `compare` 和 `trend` 的格式分支仍有 default 分支（#357）。`flashback verify` 不接受 `-` 作为 stdin（退出码 4）。
- **#255**、**#256**、**#257**（P3）延续到下个版本。

参见 `docs/adr/0017-json-schema-and-display-timezone.md`、`docs/adr/0018-matched-gtid-ceiling-count.md`、`docs/gtid-filter-streaming.md` 和 `docs/concept/limitations.md`。
