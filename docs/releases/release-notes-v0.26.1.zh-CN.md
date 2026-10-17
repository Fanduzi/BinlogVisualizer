# BinlogViz v0.26.1 发布说明

发布日期：2026-10-17

## 概述

v0.26.1 是补丁版本，修复 `flashback --conflict guard` 在主键被修改后又被重新使用时的问题，这是 #270 最后一个未解决的问题。没有不兼容变更：退出码、`plan_version` 2、analyze 输出以及默认的 `--conflict refuse` 行为都与 v0.26.0 相同。

## 修复

- **`--conflict guard` 与被重新使用的主键（#270、#299）**：如果一个 UPDATE 修改了某行的主键（例如 `id 1 → 101`），之后又有一行重新使用了旧主键 `1`，guard 脚本中改回主键的语句会报 `ERROR 1062` 并中断整个块。现在改回主键的语句和其他后来被修改过的行一样受 guard 保护：它不改变任何行，块中其余语句照常执行。guard 脚本头部有相应说明。
- 默认的 `--conflict refuse` 遇到这种情况仍然以退出码 7 拒绝，所以只有选择了 `--conflict guard` 时行为才会变化。

## 验证

BinlogQA 对 main `5a7b2e84` 的发布前 QA 通过，以 v0.26.0 tag 为基准。

- **真实 MySQL 8.0.36**：经典的主键修改后被重新使用的场景以退出码 7 拒绝，guard 脚本执行后结果符合预期；复合主键部分修改、`_ci` 仅大小写不同的回滚、`_ci` 重新使用以及带重新使用主键的修改链，都能执行且不报 1062。
- **与 v0.26.0 对比**：analyze 输出、普通 flashback 的 SQL 和 stderr、refuse 路径都逐字节一致。guard 脚本唯一的差异是新增的头部注释。67 项退出码矩阵唯一的差异也是这份 guard 输出。
- **#293 的压缩炸弹**仍然以退出码 5 结束，stdout 为空，内存与 v0.26.0 持平。workflow 冒烟测试通过。
- CI 和 `go test ./...` 通过。

## 兼容性

- 没有不兼容变更。`plan_version` 仍为 2，`report_version` 仍为 `3`，退出码、快照、产物名称和支持的平台与 v0.26.0 相同。
- `flashback` 从不连接 MySQL。请先审阅并测试脚本，再带着脚本头，在主库上用一个新会话、`sql_log_bin=1` 执行。

## 已知问题

- #285 遗留的 `install.sh` P3，以及 #255、#256、#257，沿用自 v0.26.0。

更早版本遗留的 flashback 限制见 `docs/concept/limitations.md`。
