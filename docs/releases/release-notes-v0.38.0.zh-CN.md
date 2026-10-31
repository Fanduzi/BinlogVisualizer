# BinlogViz v0.38.0 发布说明

发布日期：2026-11-02

## 概述

v0.38.0 完成了对齐 `mysqlbinlog`、`binlog2sql`、`my2sql`、`pt-query-digest` 的全部路线图：路线图史诗里剩余的每一项功能缺口均已交付。`binlogviz redo` 生成可重放的正向 SQL；`flashback` 可直接从线上服务器读取 binlog；`--where`、`--thread-id`、`--output-dir` 让故障恢复可以外科级裁剪范围；`analyze` 新增 CSV/TSV 数据集导出与 pt-query-digest 风格的语句指纹。`report_version` 仍为 4，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。

## 新增

- **`binlogviz redo`** —— 所选变更的正向（redo）SQL，即 binlog2sql 默认模式：真实 `UPDATE` 语句（非 DELETE+INSERT）、主键 `<=>` 匹配或全镜像 `LIMIT 1` 回退、DDL 原样输出、`USE` 上下文跟踪、XA 透传、BEGIN/COMMIT 边界镜像、整脚本暂存（半成品绝不落到 stdout）。所有输入模式可用，含 `--host`，可做有界 PITR 重放（ADR 0022）。
- **`flashback --host`** —— analyze/redo 已交付的远端有界读取覆盖到 flashback；`--index-file` 及 `--from-dir`/`--prefix` 在三条命令上对齐（ADR 0023）。
- **`flashback`/`redo --where <expr>`** —— 基于 MySQL 比较语义的行级谓词过滤：UPDATE/DELETE 校验 before-image、INSERT 校验 after-image、保守三值逻辑、无法校验的列显式拒绝（MINIMAL 元数据继承 `names` 拒绝链）（ADR 0024）。
- **`--output-dir` 切分输出** —— staging 目录原子发布 + `MANIFEST`；`--file-per-table`（仅 flashback：每文件是完整密封守卫脚本，可独立 verify 与 apply）与 `--max-statements-per-file`（redo：事务续接注释 + 跨文件 GTID 警告）（ADR 0025）。
- **`analyze --format csv|tsv` + `--csv-dataset`** —— `tables`、`transactions`、`patterns`、`fingerprints` 四个数据集的机器可读导出，截断边界与 JSON 一致（ADR 0026）。
- **语句指纹** —— Rows_query 语句归一化（字面量归一为 `?`、`IN`/`VALUES` 折叠为 `(?)`、关键字折叠、标识符保留大小写），按写入行数在整个负载范围内归组排名，输出到 JSON（`statement_fingerprints`）、文本（`--show-fingerprints`，`--details` 隐含）与 `fingerprints` 数据集。8192 组上限 + 显式 overflow 计数（ADR 0026）。
- **`--thread-id` 选择器** —— 三条 SQL/分析命令统一按 binlog 连接 ID 选事务组，整组语义与 GTID 选择器一致（ADR 0027）。
- **`--index-file` 信任边界硬化** —— manifest 条目必须解析为 `--from-dir` 内互不相同的普通文件：`..`/绝对路径逃逸、自引用、重复（按文件身份判定，覆盖硬链接与 symlink 别名）、目录内 symlink 指向外部，均为命名拒绝；relay-log 输入已文档化（ADR 0028）。

## 修复

- `codehealth lint` 本地门此前不可用：golangci-lint 内容键缓存返回的 finding 位置带着陈旧 base 提取目录或其他 worktree 的路径，导致全部 finding 被误判为新增（或在双侧对称污染下假性通过）。现在每次 lint 运行使用独立 `GOLANGCI_LINT_CACHE`，finding 路径按被检 root 归一化（#466）。

## 架构

- `internal/sqlemit` 共享单元格字面量 emitter，undo 与 redo 同源 —— 无分叉 SQL 格式化。
- `internal/splitout` staged 原子目录发布（fsync、MANIFEST 最后写、rename）。
- `internal/mysqlexpr` 在标量文法旁新增布尔/谓词层 —— 单一比较引擎，非两套。
- 指纹捕获挂在 builder 缝的瞬态 `RetainedStatements` fold 上 —— 覆盖全部事务的逐语句视角，scoping 与 `query_summary` 完全一致。
- ADR 0022–0028 记录各 surface 决策；L2 模块 README 现已覆盖所有 internal 包。

## 测试与 CI

- 新增 MySQL 8.0 live e2e 覆盖：redo 重放、远端 redo/flashback、`--where`、切分回环、语句指纹、`--thread-id` —— 全部注册进强制 PASS 校验循环。
- macOS CI runner `macos-14` → `macos-15` 提前迁移（保护 darwin/arm64 release 构建）。
- 删除 20 个死 i18n key；全树 `gofmt -s` 干净。

## 验证

- `go test -count=1 ./...`、`go vet ./...`、`CGO_ENABLED=0 go build ./...`、`make codehealth-compare`、golangci-lint —— 在 tag 所指 commit 上全绿。
- live e2e 在 CI 的 MySQL 8.0.45 上验证通过（开发期间亦在本机验证）。

## 兼容性

- CLI 改动均为 flag 级新增；无删除、无改名。
- JSON 报告保持 `report_version` 4 —— 新键全部 additive；`--sql-context off` 对指纹 sample 的抑制与 query summary 一致。
- 非 delimited 格式下的 `--csv-dataset`、`--thread-id 0`、非法 index manifest 均为显式 usage/input 错误，非静默行为。

## 已知问题 / 范围外

- `--file-per-table` 有意仅支持 flashback；redo 保持事务镜像序。
- 指纹输入受既有 4096 字节 Rows_query 捕获上限约束；截断语句按已存前缀指纹。
- `--thread-id` 选择的是 binlog 连接 ID（服务器会复用），非应用身份 —— 见 `docs/reference/cli.md`。
