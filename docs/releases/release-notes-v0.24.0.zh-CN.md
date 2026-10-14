# BinlogViz v0.24.0 发布说明

发布日期：2026-10-15

## 概述

v0.24.0 让 `binlogviz flashback` 能安全地处理大而繁忙的 binlog，并让每个命令的失败都可以被程序识别。设计记录在 ADR 0008（`docs/adr/0008-row-stream-streaming-flashback-typed-errors.md`）。

对 DBA 来说：

- flashback 通过临时文件流式处理行镜像，内存不再随回滚集合的大小增长。
- 要回滚的行如果在扫描到的 binlog 里后来又被改过，flashback 默认拒绝（退出码 7）。可以用 `--conflict guard` 和 `--plan` 自行选择。
- 失败有各自的退出码，并可输出单行 JSON 错误。损坏或恶意构造的 binlog 退出码为 5，不再崩溃、卡死或输出错误的 SQL。

## 变更

- **ADR 0008**：行流、流式 flashback 和分类错误。ADR 里列出了本版本未包含的后续工作（#255、#256、#257）。
- **流式 flashback（#258）**：行镜像写入一个已 unlink 的临时文件，第二遍再渲染。BinlogQA 的测试中，1.17 GB、单个 45 万行事务的 binlog 峰值 RSS 约 205 MB；306 MB、14 万个小事务的 binlog 约 164 MB。`$TMPDIR` 需要大约等于所选行镜像大小的空间。
- **冲突检测（#259、#267）**：flashback 检查所选的行在扫描到的 binlog 中是否后来又被修改。
  - `--conflict refuse`（默认）不输出脚本，退出码 7。错误中列出前 20 个冲突（表、键、后续 GTID 和位置）以及总数。
  - `--conflict guard` 仍然输出脚本；每条回滚语句都匹配 binlog 中记录的后镜像，后来被改过的行保持不动。
  - `--plan` 输出 JSON 计划（`plan_version` 1：事务、各表行数、冲突、guard），不输出 SQL；退出码与真正执行时相同。
  - 后续修改本身也在所选回滚集合内时，不算冲突（#267）。
- **分类退出码和 JSON 错误（#263、#268）**：每种失败对应一个类别和退出码：1 error、2 no_data、3 usage、4 input、5 corrupt、6 refused、7 conflict、8 output。`--error-format json`（或 `BINLOGVIZ_ERROR_FORMAT=json`）时，stderr 只输出一行 `{"error":{"kind","code","message","details"}}`。无效的 `--format` 是用法错误（#251），解析器的 Go 结构体不再暴露给用户（#254）。详见 [docs/reference/exit-codes.md](../reference/exit-codes.md)。
- **非终端输出和 `--quiet`（#253）**：stderr 不是终端或 `TERM=dumb` 时，不显示进度条，也没有 ANSI 转义码。`-q`/`--quiet` 还会去掉提示信息行；错误和警告照常输出。
- **密码脱敏（#250）**：flashback 中每条引用 SQL 的消息都会先脱敏凭据（CREATE USER、GRANT、SET PASSWORD），包括密码哈希。
- **更快的大事务排行（#252）**：排行改用有界堆，不再线性插入；事务很多时 `analyze` 不会再出现平方级变慢。排序结果不变。
- **CRC32 校验与恶意输入防护（#271、#272）**：BinlogViz 在 go-mysql 解码之前自己读取每个事件的字节。
  - 校验每个事件的 CRC32。未关闭或崩溃留下的 binlog 仍可读取，因为会像 MySQL 一样屏蔽 in-use 标志。
  - 计数和长度字段、行事件的列数以及 JSON 值都有边界：偏移量必须在所属容器内，嵌套深度与 MySQL 一样最多 100 层。
  - MySQL 8 事务 payload 声明的解压大小最多 1 GiB，且实际解压结果必须正好是这个大小。
  - 损坏的事件以退出码 5 结束，并给出文件和事件偏移。以前可能 panic（退出码 2）、卡死、内存耗尽，或以退出码 0 输出错误的 SQL。
- **MariaDB 11.4 排序规则与前导注释（#269）**：flashback 识别 MariaDB 11.4 的排序规则 id，包括 `utf8mb4_uca1400_ai_ci`。QUERY 文本在分类前会去掉前导注释，所以 MariaDB 原样记录的 `/* c */ CREATE USER` 与 MySQL 的分类一致；可执行注释 `/*!`、`/*M!` 保留。

## 行为变化

- **退出码**：以前退出码为 1 的若干失败，现在退出码为 3（usage）、4（input）、5（corrupt）或 6（refused）。退出码 1 只表示没有其他类别的失败，包括未结束的 `BEGIN` 和无法分类的 QUERY（ADR 0001）。匹配退出码 1 的脚本需要改为读取新的退出码或 JSON 中的 `kind`。
- **flashback 可能以退出码 7 拒绝**：所选的行后来又被修改时，以前退出码为 0 的 flashback 现在退出码为 7，且不输出脚本。可以用 `--conflict guard`、缩小范围，或先用 `--plan` 检查。
- **损坏的输入退出码为 5**：CRC32 不匹配、无法界定的事件或解码失败时，退出码为 5（`corrupt`）。以前可能以退出码 0 输出错误结果，或 panic 后退出码为 2。
- 在 BinlogQA 的离线语料（4,898 个用例，英文和 zh-CN）上，除上述退出码外，flashback 的 SQL 与 v0.23.34 逐字节相同。

## 验证

BinlogQA 在 main `7649df9a` 上的 dogfood 通过。

- **与 v0.23.34 对比**：
  - 离线 flashback：4,672 个相同，225 个只有退出码变化。唯一的 SQL 差异是 #261 的修复。
  - guard：252/252 相同。
  - analyze：1,460 个中 1,241 个相同，其余 219 个是上面所说的损坏输入和退出码变化。
- **在 MySQL 8.0.46 和 MariaDB 11.4（开启 CRC32）上实际执行 flashback**：一个后来有 33 处修改的峰值事务以退出码 7 结束，报告 33 个冲突。应用 `--conflict guard` 后保留这 33 行，回滚另外 1,167 行，其他行不受影响。
- **恶意输入**：自引用 JSON、损坏的 zstd payload 和随机字节翻转都以退出码 5 结束并输出一行 JSON，不会卡死，也不会出现退出码 2。
- CI 和 `go test ./...` 通过。

## 兼容性

- 应用脚本支持 MySQL 5.7 及以上和 MariaDB 10.2 及以上。
- `flashback` 从不连接 MySQL。请先审阅、测试脚本，再在主库上用一个新会话、`sql_log_bin=1`、连同脚本头一起执行。某条语句失败时，脚本中之前的事务已经提交。
- JSON `report_version` 仍为 `3`。快照、工作流、制品名称和支持的平台与 v0.23.34 相同。

## 已知不足

- #277：空的 `--error-format` 会被接受；缺少 workflow 计划或快照时退出码为 1 而不是 4。
- #278：payload 头部字段超过 8 字节时被提前拒绝，到不了 1 GiB 的大小检查。
- #255：面向 agent 的易用性：没有 AGENTS.md、skill 或 MCP 服务，`--help` 没有示例，发布包里没有文档。
- #256：与 mysqlbinlog、binlog2sql、my2sql、pt-query-digest 相比的功能差距。
- #257：`internal/analyzer` 仍是一个大包；ADR 0008 提出了拆分方案。
- #270：`--conflict guard` / `--plan` 的小问题：旧主键被重用时，移动主键的回滚会遇到 ERROR 1062；退出码 6 时 `--plan` 什么也不输出；最多只列出 20 个冲突。

v0.23.34 遗留的 flashback 限制见 `docs/concept/limitations.md`。
