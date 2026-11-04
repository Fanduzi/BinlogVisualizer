# BinlogViz v0.43.0 发布说明

发布日期：2026-11-04

## 安装或升级

```bash
brew upgrade --cask binlogviz          # macOS
# Linux：重新运行 install.sh，它会解析最新版本
```

只做加法：没有新增或删除参数，`report_version` 仍是 4，`plan_version` 仍是 2。新增的 JSON 字段列在「兼容性」里。

## 亮点

- **热点行现在会按 `--schema-file` 声明的主键排名。** binlog 没有行元数据时（`binlog_row_metadata=MINIMAL`，或 MySQL 5.7），这一节以前只会说逐键追踪不可用。给出定义后，它会按转储声明的主键排名：一行说明主键来自该文件，这些行标 `[--schema-file]`，JSON 里是 `hot_rows_key_source: "schema_file"`，每条 `hot_rows` 另带 `key_source`。
- **窗口里两种元数据都有时，标注仍然是真话。** `binlog_row_metadata` 可以在写 binlog 的过程中切换。binlog 回答过的键（哪怕只回答了它的一部分镜像）保持不标，两种都有的窗口不声称单一来源。
- **`key_status` 与 `no_primary_key` 告警没有变。** 它们仍然只反映 binlog 记录的内容；热点行是唯一也接受定义的排名，两边的分工写进了 ADR 0032。

## 修复

- Markdown 的热点行节现在也会说明主键来源；文本和 HTML 之前就会。
- `analyze --help` 以及 CLI、输出格式参考页不再说主键只来自 `binlog_row_metadata=FULL`。那句话早于这个功能存在，会让读者以为新路径不可能出现。
- 定义覆盖了一张表但没有声明主键时，`tables[].key_source` 不再出现：没有主键却给「主键来源」是误导。

## 兼容性

- `report_version` 仍是 4。`docs/schema/analyze.json` 增加顶层 `hot_rows_key_source` 和每条 `hot_rows` 的 `key_source`；都是加法，已有字段含义不变。
- `plan_version` 仍是 2。没有参数、退出码、节名或文本布局变化；不带 `--schema-file` 时输出与之前完全一致。

## 已知问题 / 不在本次范围

- 定义必须是事故当时的表结构。主键写错会把计数记到错误的列上，工具无法察觉。
- binlog 和定义都没有点名的键，仍然报这张表的追踪不可用，不会拿第 1 列 `@1` 去猜。
- 定义声明为无主键的表仍留在「无主键」一节，且仍不产生 `no_primary_key` 告警：告警按设计只来自 binlog 元数据。

## 测试与 CI

- 采集层：binlog 优先、定义补缺、列数不符被拒、没有定义时保持原路径（`TestRowKeysFallBackToDefinition`）。
- 排名与来源：全部来自定义、全部来自 binlog、混合窗口，以及同一个键在两种元数据下都被碰到（`TestHotRowsUseSchemaFileKeys`）；文本、Markdown、JSON 的标注（`TestHotRowsNameTheDefinitionSource`）；定义没声明主键的表没有 `key_source`（`TestTableKeySourceNeedsADeclaredKey`）。
- `--help` 与四个参考页不会再退回旧说法（`TestHotRowsDocsNameTheDefinitionFallback`）。
- 真库 MySQL 8.0.45：MINIMAL 加定义排出 `id=1/2/3` 并带来源行与 JSON 字段；FULL 带不带定义逐字节相同；仓库自带的 MySQL 5.7 MINIMAL fixture 同样能排名。
- `go test ./...`、`make codehealth`、`make lint`、`gofmt` 干净；七个合并的 PR 都通过了 11 项必需检查。

## 验证

打了 tag 的提交，其父提交包含全部已合并的 PR；`main` 在最后一个 PR 上通过了 `ci` 与 `lint`（`verify`、`vet and race`、`flashback e2e`、`flashback difftest`、`MySQL 5.7 compatibility`、`codehealth`、`golangci-lint`、`release-prep self-test`、`landing overflow`、`darwin verify`）。
