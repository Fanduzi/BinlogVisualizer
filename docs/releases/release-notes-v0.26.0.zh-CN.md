# BinlogViz v0.26.0 发布说明

发布日期：2026-10-17

## 概述

v0.26.0 让 `flashback --plan` 的 JSON 成为运维人员或 agent 不读 stderr 就能据此行动的契约：每次拒绝都会输出说明原因的 plan，warning 带稳定的 code，冲突列表可以完整列出。

**对读取 `flashback --plan` JSON 的脚本来说，这是一个不兼容的版本。** plan 现在是 `plan_version` 2，读取下面的字段之前请先检查 `plan_version`。

## 不兼容变更：`flashback --plan` 升级为 `plan_version` 2（#270、#282、#296）

- `warnings` 的每一项从字符串改为 `{"code", "message"}`。code 不随 `--lang` 变化：`partial_transaction`、`table_definition_missing`、`generated_column_unverified`、`schema_file_db_mismatch`、`schema_file_table_ambiguous`、`schema_file_case_folded`、`schema_file_case_mismatch`。message 不再带本地化的 `warning:` 前缀。
- 拒绝时也会输出 plan。退出码 6（DDL、非 FULL 行镜像、FLOAT 等拒绝）以前 stdout 什么都不输出；现在 plan 中 `would_refuse` 为 true，`refusal` 为 `{"kind": "refused", "message": ...}`。退出码 7 时 `refusal.kind` 为 `"conflict"`。没有拒绝时 `refusal` 为 `null`。
- 因此只看 JSON 就能区分正常（退出码 0，`refusal` 为 null）、拒绝（退出码 6）和冲突（退出码 7）。
- 用法、输入、损坏和输出错误（退出码 3/4/5/8）仍然不输出 plan。
- stderr 中的警告和 flashback SQL 不变。

## 新增

- **`flashback --conflict-limit N`（#270、#296）**：默认 20，`0` 表示全部列出。它限制 `--plan` 和退出码 7 错误详情中列出的冲突条数；`count` 始终是完整数量。负数或非整数退出码为 3。运维人员可以在选择 `--conflict guard` 之前查看每一个冲突的键。

## 验证

BinlogQA 对 main `d87b14a5` 的发布前 QA 通过；此前 #296 在 `9bb00d45` 的 PR QA 也已通过，两次都以 v0.25.0 tag 为基准。

- **Plan v2**：在一份后续有 25 个 UPDATE 的真实 MySQL 8.0.36 语料上，退出码 0/6/7 的 `refusal` 分别为 null/`refused`/`conflict`；冲突 plan 的 count 为 25，列出 20 条，省略 5 条；`--conflict-limit` 为 0、1、5、20、100 时表现符合文档。en 和 zh-CN 下的 warning code 完全相同。
- **与 v0.25.0 对比**：analyze 168 次，0 差异；普通 flashback 的 SQL 和 stderr 在三份语料上逐字节一致。67 项退出码矩阵唯一的差异是冲突 plan 的输出大小（v2 schema）。
- **#293 的压缩炸弹**仍然以退出码 5 结束，峰值内存不超过 43 MB，stdout 为空。workflow 冒烟测试通过。
- CI 和 `go test ./...` 通过。

## 兼容性

- **`flashback --plan` 的 JSON 有变化**（见上文）。v1 的调用方需要处理 `plan_version` 2。
- 退出码、`report_version` `3`、快照、产物名称和支持的平台与 v0.25.0 相同。
- `flashback` 从不连接 MySQL。请先审阅并测试脚本，再带着脚本头，在主库上用一个新会话、`sql_log_bin=1` 执行。

## 已知问题

- #270 第 1 项：如果主键被修改过、旧主键后来又被重新使用，回滚会报 1062；guard 不会跳过，plan 中也没有提示。它会安全地失败。
- #285 遗留的 `install.sh` P3，以及 #255、#256、#257，沿用自 v0.25.0。

更早版本遗留的 flashback 限制见 `docs/concept/limitations.md`。
