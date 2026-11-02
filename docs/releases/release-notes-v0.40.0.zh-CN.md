# BinlogViz v0.40.0 发布说明

发布日期：2026-11-02

## 安装或升级

```bash
brew upgrade --cask binlogviz          # macOS
# Linux：重新运行 install.sh，它会解析最新版本
```

本次不新增或修改任何 CLI 标志、JSON 字段和 schema 版本；JSON 报告与 v0.39.0 完全一致。

## 主要更新

- **中文文本报告现在是完整的中文。** 表头改为中文（表、影响行数、DDL 事件、事务数、binlog 事件、行占比，线程表是 行数/事件数/字节/事务/占比），列宽按终端格数计算，所以中文库名或表名也能对在表头下面。`INSERT`/`UPDATE`/`DELETE` 和 `thread_id` 这类 binlog 字段名保持原样——它们是取值，不是叙述。
- **每个重点事务只打印一次。** 同时赢得多个榜单的事务现在只出现一块：`最大/最长/最宽事务: txn-2 rows=1 tables=1 dur=0s file=… table=… bytes=…`，配一条 `mysqlbinlog` 命令和一份行镜像，而不是每个榜单各打一整块。
- **发布包带上 `LICENSE`。** Apache-2.0 第 4 条要求随附，之前的每个归档都缺。
- **实测吞吐，不是口号。** `docs/reference/analyze-behavior.zh-CN.md` 写明本版本在两份真实 MySQL 8.0.45 binlog 上的实测：141,910,241 字节 / 3 个事务 0.45 秒、峰值 40 MB；99,478,756 字节 / 40,002 个事务 1.27 秒、峰值 40 MB。
- **README 说明自己的位置。** 新增与 `mysqlbinlog`、`pt-query-digest`、`binlog2sql`/`my2sql` 的三行分工表，说明第一次运行会打印什么并链到钉死的真实报告，安装路径去掉了那段 15 行手动解压脚本。

## 修复

- 中文文档里有三处退出码写成了 `退出 1`，实际是 6：生成列无法核对时的 flashback 拒绝，以及 CLI 参考里的拒绝清单。
- `CHANGELOG.md` 与两份 notes 里 v0.38.0、v0.36.0 的发布日期与 tag 不符。tag 是对的，日期现已改为 tag 的日期。

## 兼容性

- `report_version` 仍是 4，`plan_version` 仍是 2，`docs/schema/*.json` 全部未变。
- `docs/examples/analyze-output.json` 与 v0.39.0 逐字节相同。
- 文本输出有变化：列头跟随 `--lang`，同时赢得多个榜单的事务只出现一次并使用合并后的标签。用 `Largest transaction:` 做 grep 的脚本要放宽成 `Largest`。

## 已知问题 / 不在本次范围

- 保存快照时没给 `--workload-id`，`compare` 和 `trend` 仍会报 `unknown` 可比性，而且保存时没有任何提示（#478）。文档已写明该标志是必需的；保存时告警属于产品决策，本次没有做。
- 落地页仍然没有截图：它被自己的守卫限制为单文件、不加载外部资源（ADR 0029）。

## 测试与 CI

- 新增 `TestRenderTextLocalizedTopTablesKeepsColumnsAligned`：在中文表上按终端格数比较"影响行数"列的右边缘；三个断言旧标签拼写的测试改为断言它们真正关心的事务 key、`dur=` 和告警。
- 新增 `TestLandingPreviewsAreRealOutput`：落地页的输出面板必须是被钉住示例的逐字切片，且中英两个字典一致。
- `docs/examples/analyze-output.md` 加入钉死的示例，文本、JSON、Markdown 三种格式都与新运行逐字节比较。
- 从本次发布起，`check-release-prep.sh` 要求两份 notes 在工程细节之前带安装或升级一节。

## 验证

tag 所在提交的父提交上，全部必需检查通过：`verify`、`vet and race`、`codehealth`、`golangci-lint`、`flashback e2e`、`flashback difftest`、`MySQL 5.7 compatibility`、`release-prep self-test`、`landing overflow`，以及 Cloudflare Pages 部署。
