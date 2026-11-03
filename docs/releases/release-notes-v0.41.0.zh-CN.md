# BinlogViz v0.41.0 发布说明

发布日期：2026-11-03

## 安装或升级

```bash
brew upgrade --cask binlogviz          # macOS
# Linux：重新运行 install.sh，它会解析最新版本
```

只有新增：`analyze` 多了两个可选标志，JSON 多了一个键。没有删除或改名任何标志或键，`report_version` 仍是 4。

## 主要更新

- **撤销现在能在 MySQL 的默认配置上工作。** `binlog_row_metadata=MINIMAL`（默认值）以及完全没有行元数据的 MySQL 5.7 上，`flashback --schema-file` 会从定义中取得列名、字面量需要的类型事实和主键。此前同样的输入在采集阶段就被拒绝（退出码 6，`row_metadata`），尽管 `redo` 早已接受它。
- **主键来源会标注出来。** 当撤销按 schema 文件声明的主键定位行时，脚本注释、一条 stderr 警告和 `--plan`（`warnings[].code = schema_file_key`）都会说明这一点——它只和那份转储一样可靠。
- **后续 `ALTER` 改动过的主键不再被采信。** 解析到的文件里若有增删主键的 `ALTER`，定义里的主键会被丢弃并给出 `schema_file_key_unmodelled` 警告，撤销改为匹配全部列并加 `LIMIT 1`，而不是按转储已不再拥有的主键定位。
- **`analyze --show-rows --schema-file` 会打印列名。** 报告不再只给 `@1`..`@N`，而是用定义里的列名，摘要写明来源，JSON 行镜像带 `names_from: "schema_file"`。`analyze` 新增 `--schema-file` 与 `--schema-file-db`，仍然离线运行。

## 修复

- 解析到的文件里含改动主键的 `ALTER` 时，运行会同时打印 `schema_file_key_unmodelled` 和 `schema_file_key`，脚本却仍按旧主键生成。现在入库前会用 planner 自己解析的定义重算撤销地址，因此只会打印与实际脚本相符的那一条警告。
- 定义的列数与行不一致时，此前报告为“没有 `--schema-file` 定义覆盖这张表”。现在它是 `schema` 拒绝（退出码 6），同时给出两个列数，并建议使用事故当时的转储或 `--exclude-table`。

## 兼容性

- `report_version` 仍是 4，`plan_version` 仍是 2。`docs/schema/analyze.json` 的行镜像新增 `names_from`（只增不改）。
- `analyze` 新增两个可选标志；既有用法行为不变。
- `flashback` 少了一些拒绝：MySQL 5.7 的 binlog 和 8.x 的 `MINIMAL` binlog 在提供定义后可用。此前出于安全拒绝的情形全部保持：行镜像不完整、值无法精确渲染、生成列与表达式矛盾、选中范围内有 DDL、定义与可比 binlog 事实矛盾、在 MariaDB 上执行。
- 定义必须描述事故当时的表结构，这一点与之前一致；用错转储仍会得到错误的行。

## 已知问题 / 不在本次范围

- `analyze --schema-file` 遇到列数不符时退回 `@1`..`@N` 并说明列名不可用，而不是像 `flashback` 那样给出两个列数。
- 定义里没有声明主键时不会打印 `schema_file_key` 警告，因为没有采信任何主键；脚本会说明按全部列匹配并加 `LIMIT 1`。

## 测试与 CI

- 三个新的真库用例，CI 要求全部 PASS（SKIP 视为失败）：MySQL 8.0 的 MINIMAL 往返、8.0 上主键被 `ALTER` 改动的场景、MySQL 5.7.44 的往返。5.7 容器改为带 `--log-bin` 启动，测试用 `docker cp` 把 binlog 取出。
- 单元覆盖：定义命名、列数不符的拒绝、主键解析（行内、表级、复合、命名、带引号）、改动主键的 `ALTER`，以及 planner 侧重算地址。

## 验证

tag 所在提交的父提交上 `go test ./...`、`make codehealth`、`make lint`、`gofmt` 均干净；已合并的 PR 上 `verify`、`vet and race`、`flashback e2e`、`flashback difftest`、`MySQL 5.7 compatibility`、`codehealth`、`golangci-lint`、`release-prep self-test`、`landing overflow`、`darwin verify` 全部通过。
