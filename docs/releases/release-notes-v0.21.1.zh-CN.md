# BinlogViz v0.21.1 发布说明

发布日期：2026-08-21

## 变更

- **Release archive 带上样例 ROW binlog**：`testdata/minimal.binlog` 和二进制放在一起，可以用 `binlogviz analyze testdata/minimal.binlog` 验证安装，不必先有生产 datadir。仓库里同一份 1500 字节 fixture 仍在 `cmd/binlogviz/testdata/minimal.binlog`。
- **仓库根目录提供可跑的 `incident.yaml`**：`binlogviz workflow run incident.yaml` 指向 `cmd/binlogviz/testdata/sample-binlog`。`from_dir` 仍是 `PLACEHOLDER` 时 validate 还是 exit 0，但会警告。全部成功后再 `workflow resume` 且未加 `--rerun` 时 exit 0。
- **默认文本报告改成值班简报**：先摘要、热表、最多 3 个最大事务，再 Findings。Write shape 的代表事务必须匹配该 pattern（DELETE large-batch 不再引用 40 万行 INSERT）。
- **可用事务给出可复制的 `mysqlbinlog` 命令**：同一文件跨度输出 `--start-position` / `--stop-position`。31 字节的纯 XID 区间不会当成 start-position。

## Bug 修复

- **文本 Findings 与 JSON alerts 对齐**：5 行 / 4 事务的样本不再在 JSON `alerts` 为空时标成 `[critical] Write spike`。热分钟和大事务只当证据，不再合成 finding。
- **单行 UPDATE 计 1 逻辑行**：旧的 `UPDATE` 子串匹配不到 go-mysql 的 `UpdateRowsEventV2` 这类 CamelCase 名称。
- **STATEMENT / MIXED 不再静默只统计 ROW 子集**：stderr 提示只统计 ROW image。没有 row image 的 STATEMENT 仍会打出报告，然后非 0 退出。
- **MariaDB 11.4+ 事务跨度重新按真实位置记录**：`LogPos=0` 的事件用文件 cursor 重建，40 万行事务不再被记成 31 字节 XID。Query 的 `CREATE DATABASE` / `CREATE TABLE` 进入 DDL Timeline。
- **`compare` / `trend` 跟值班员写的顺序走**：`trend last_week tonight` 保持 last_week → tonight（`--order cli`）。最大事务 compare 用 table/op/rows/file:span 对齐，不再用本地 `txn-1`。`0 → N` 显示为 `new`（JSON `delta_percent: null`），不再是 `+0.0%`。
- **analyze CLI dogfood**：HTML「saved to …」打到 stderr；失败只打一遍 `Error:`、不再刷 Usage；只有 magic 的截断文件非 0 退出；`--prefix mysql-bin` 能匹配 `mysql-bin.000008`。
- **失败的 workflow 发现不再写成 `complete`**：空的失败 discovery 的 `runtime_state` 为 `incomplete`。

## 破坏性变更

- **没有 ROW image 的 STATEMENT 会非 0 退出**：`analyze` 仍会写出报告，然后让进程失败，避免空 STATEMENT 文件看起来像一次成功的 ROW 分析。带 ROW 子集的 MIXED 仍 exit 0，并在 stderr 警告。
- **Compare `0 → N` 的百分比是 `null` / `new`，不再是 `0.0%`**：把 `delta_percent: 0` 当成「没变」的脚本，在 baseline=0 且 current>0 时会看到 `null`。
- **`trend` 默认按 CLI 参数顺序**：显式 `trend last_week tonight` 不再按 binlog 窗口开始时间重排。需要按时间排 First/Last 时用 `--order time`。
