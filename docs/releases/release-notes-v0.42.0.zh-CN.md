# BinlogViz v0.42.0 发布说明

发布日期：2026-11-04

## 安装或升级

```bash
brew upgrade --cask binlogviz          # macOS
# Linux：重新运行 install.sh，它会解析最新版本
```

只有新增：没有增删任何标志，`report_version` 仍是 4，`plan_version` 仍是 2。两个新 JSON 键见「兼容性」。

## 主要更新

- **analyze 报告现在会说明主键来自哪里。** 配合 `--schema-file`、而 binlog 没有记录行元数据时，摘要不再写"主键是否存在未知"，而是指出主键来自定义。定义回答了主键的表在「无主键」一节里标为 `[--schema-file]`，JSON 的 `tables` 每项带 `key_source: "schema_file"`。
- **部分覆盖不再产生假话。** binlog 能回答的地方优先；定义只回答了一部分表时，摘要保留 `部分表的主键是否存在未知`，由该节逐行说明被标记的表，而不是把功劳记给整份文件。
- **定义与行不匹配时会明说，而不是沉默。** 列数不符时，摘要与 JSON 行镜像（`names_note`）都给出两个列数，列名仍是 `@1`..`@N` 并附上原因。
- **未带 workload 身份保存快照会提示。** `analyze --snapshot-name` 没有 `--workload-id` 时，在文件写好后打印一行 stderr 警告：`compare` 和 `trend` 对这份快照会报 unknown 可比性。

## 修复

- 定义声明无主键时，表会出现在「无主键」一节却不说来源；现在该节按被标记的表点名来源。
- 混合元数据运行时，会把从 FULL 元数据得到主键的表也算作来自 `--schema-file`；现在只有定义回答了每张表时，才出现运行级的来源句。
- 定义只覆盖部分表时，摘要会声称一个对未覆盖表为假的来源，而同节的注脚说存在未知；两个面现在一致。

## 兼容性

- `report_version` 仍是 4。`docs/schema/analyze.json` 新增行镜像的 `names_note`、表项的 `key_source`，`primary_key_note` 现在可能写明 `--schema-file`（都是只增）。
- `plan_version` 仍是 2。没有改名任何标志、退出码或文本小节。
- 快照警告只写 stderr；stdout、保存的快照载荷和 `report_version` 都不变。

## 已知问题 / 不在本次范围

- 「热点行」一节对没有行元数据的 binlog 仍写按主键追踪不可用：按主键计数需要在事件读取时就知道键，而 `--schema-file` 是事后才到的答案。
- 定义声明为无主键的表会出现在「无主键」一节，但不产生 `no_primary_key` 告警；告警来自 binlog 元数据。

## 测试与 CI

- 定义来源的优先级由七个用例覆盖：单表有键、单表无键（标记与摘要）、binlog 优先、混合有键、混合定义、混合元数据 + 无键定义、部分覆盖 + 无主键风险。
- 列数不符的说明与快照警告（写了快照才响、没写就静默）各有一个 CLI 层测试。
- `go test ./...`、`make codehealth`、`make lint`、`gofmt` 均干净；六个已合并的 PR 每个都通过全部 11 项必需检查。

## 验证

tag 所在提交的父提交包含这些已合并的改动，且 `main` 在每次合并上都通过 `ci` 与 `lint`（`verify`、`vet and race`、`flashback e2e`、`flashback difftest`、`MySQL 5.7 compatibility`、`codehealth`、`golangci-lint`、`release-prep self-test`、`landing overflow`、`darwin verify`）。
