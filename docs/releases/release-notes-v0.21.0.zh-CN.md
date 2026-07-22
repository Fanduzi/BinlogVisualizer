# BinlogViz v0.21.0 发布说明

发布日期：2026-07-22

## 变更

- **HTML 默认输出改为写入文件而非 stdout**：`binlogviz analyze --format html` 现在会将 HTML 报告写入当前目录下的文件，而不是输出到 stdout。单个输入文件时，输出文件名从输入文件名推导（例如 `mysql-bin.000123.html`）；多个输入文件或 discovery 模式时，默认为 `binlogviz-report.html`。如果文件名已存在，会自动追加数字后缀（例如 `report.1.html`）。保存确认信息打印到 `stderr`。如需恢复旧行为让 HTML 输出到 stdout，请使用 `--output -`。

- **新增 `--output` 参数用于指定 HTML 输出路径**：`--output`（缩写 `-o`）参数允许为 HTML 报告指定自定义输出路径。仅支持 `--format html`；与其他格式一起使用会返回错误。

- **Text Top Tables 新增 INSERT/UPDATE/DELETE/DDL 分列展示**：Text 报告的 Top Tables 分区将原来的单一 `Rows` 列替换为 `INSERT`、`UPDATE`、`DELETE` 和 `DDL Events` 四列，每列显示数量和行内百分比。INSERT/UPDATE/DELETE 百分比基于受影响行数；DDL 百分比基于 binlog 事件数。表格下方附有脚注说明分母含义。

- **HTML Top Tables 新增操作类型分列和百分比展示**：HTML 报告的 Top Tables 分区将 INSERT/UPDATE/DELETE 原始计数列替换为带百分比的列，并新增 DDL 列。表格下方附有脚注说明百分比分母。

## Bug 修复

无。

## 破坏性变更

- **HTML 默认输出从 stdout 改为写入文件**：之前依赖 `binlogviz analyze --format html > report.html` 的脚本现在会创建一个空的重定向文件，而实际报告会作为并行文件生成。如需恢复旧行为，请显式使用 `--output -`：`binlogviz analyze --format html --output - > report.html`。`compare` 和 `trend` 命令不受影响，仍然将 HTML 输出到 stdout。
