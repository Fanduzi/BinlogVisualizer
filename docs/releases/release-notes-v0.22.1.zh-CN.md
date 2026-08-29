# BinlogViz v0.22.1 发布说明

发布日期：2026-08-29

## 变更

- **发布包真正带上样例 ROW binlog**：各平台 tar.gz 含 `testdata/minimal.binlog`、`testdata/sample-binlog/mysql-bin.000001`，以及归档内相对路径的 `incident.yaml`。解压后 `./binlogviz analyze testdata/minimal.binlog` 和 `./binlogviz workflow run incident.yaml` 不必再 clone 仓库。
- **MariaDB 主机上的重放命令可直接粘贴**：`mysqlbinlog_cmd` 使用绝对路径。Format Description 是 MariaDB 时用 `mariadb-binlog`，MySQL 仍用 `mysqlbinlog`。纯 XID 跨度仍然不生成命令。

## Bug 修复

- **`analyze --format html > report.html` 能拿到完整 HTML**：stdout 非 TTY 且未指定 `--output` 时，HTML 走 stdout（与 `compare` / `trend` 一致）。TTY 仍写 cwd 派生文件。`--output path` 和 `--output -` 不变。
- **STATEMENT 分析不再看起来像成功**：没有 ROW image 时 exit 1，stdout 为空，stderr 一行 `Error:`。带 ROW image 的 MIXED 仍 exit 0，并在 JSON `alerts` 增加 `{type: input_format, severity: warning}`。`warnings` 仍是截断 query 的整数。
- **无数据是 exit 2**：完整 binlog 但计数为 0（空 `--start`/`--end` 窗口，或仅 Format Description / rotate）exit 2，stdout 不写报告，stderr 一行 `Error:`。仅 magic、没有 Format Description 仍是 exit 1。
- **亚秒窗口 TPS 不再显示 `0.0`**：分析时长小于 1 秒且至少有一个事务时，文本 TPS 峰值为 `N/A (sub-second)`。Rows/min 和 JSON `tps_series` 仍是数字。
- **workflow 失败路径与 analyze 对齐**：只打一遍 `Error:`，不 dump Usage（`run` / `resume` / `validate` / `describe` / `status` / `clean` / `export`）。

## 破坏性变更

- **空时间窗 / 仅 Format Description 的文件改为 exit 2**，不再是 0。把 0 事件报告当成成功的脚本会失败。
- **没有 ROW image 的 STATEMENT 不再写报告**（stdout 为空）。v0.21.1 仍会先打空简报再非 0 退出。
- **未指定 `--output` 且 stdout 被重定向时，`analyze --format html` 把 HTML 写到 stdout**，不再写 cwd 旁路文件。

## 兼容性说明

- JSON `warnings` 仍是整数。MIXED 少计可通过 `alerts` 以及 `diagnostics.input_format_guess` / `ignored_query_dml_events` 看到。
- Snapshot、compare、trend 的 JSON 形状除此之外不变。
