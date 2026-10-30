# BinlogViz v0.37.0 发布说明

发布日期：2026-10-30

## 概述

v0.37.0 是功能版本：`binlogviz analyze` 现在可以通过复制协议直接从在线 MySQL 或 MariaDB 服务器流式读取 binlog 事件，补齐了与 `mysqlbinlog -R`、`binlog2sql`、`my2sql` 相比最大的缺口 —— 不再需要先把 binlog 文件拷下来，也没有有损的文本往返：原始复制事件帧直接喂给文件输入使用的同一条校验解码管线。`report_version` 仍为 4，`plan_version` 仍为 2，`flashback verify --format json` 的 `schema_version` 仍为 1。

## 新增

- **`analyze --host HOST` —— 远程复制读取(#256,ADR 0021)。** BinlogViz 以伪 replica 身份注册(go-mysql `BinlogSyncer`，纯 Go —— `CGO_ENABLED=0` 仍可编译），流式读取服务器 binlog 的一个有界切片：
  - 连接参数:`--port`(3306)、`--user`（默认 OS 登录名）、`--password`、`--server-id`(`0` = 加密随机非零伪 replica id)。
  - 起点定位:`--start-file`/`--start-pos`(file 必须在探测到的清单里；position 默认 `4`，不得超过探测到的大小）或 `--start-gtid`（表示"已消费的集合" —— 流从其后的第一个事务开始；裸 `uuid:N` 展开为 `uuid:1-N`)。不给起点选择器时，读取覆盖服务器保留的全部历史。
  - GTID 集合的格式决定线协议 flavor —— MySQL `uuid:intervals` 与 MariaDB `domain-server-seq` —— 因此没有 `--flavor` 参数。
  - `--ssl-mode` 默认 `preferred`（服务器支持就用 TLS，与 `mysql` 客户端一致）:`preferred` 只在握手阶段服务器拒绝 TLS 时降级为明文 —— 此时凭据尚未发送；`required` 加密但不校验证书（MySQL `REQUIRED` 语义），否则失败；`disabled` 不尝试 TLS。
  - 密码来自 `--password` 或 `BINLOGVIZ_MYSQL_PASSWORD`(flag 优先；环境变量避免出现在命令行），绝不打印或记日志。
- **读取天然有界。** 连接时客户端快照 `SHOW BINARY LOGS`；清单最后一个文件及其大小即为读取边界。探测之后写入的事件、以及服务器轮转出清单之外的文件，都会被排除而不是与报告竞速 —— 报告总是在一个确定的点终止。当最后一个事件恰好落在清单边界上时，它被正常发出，随后流干净地停止，不会空等一个永远不会来的帧。
- **流错误显式失败。** 读取中途断连会以 `input`(`reason: stream`）失败，而不是产出一份静默截短的报告。远程失败携带类型化 reason —— `connect`、`tls`、`auth`、`manifest`、`privilege`、`start_file`、`start_pos`、`stream` —— 映射进现有 `clierr` 分类体系，双语齐全。
- **互斥校验。** 远程 flag 不能与位置文件、`-`、`--from-dir`、`--prefix`、`--index-file` 或本地 `--start-position`/`--stop-position` 混用（`usage`)；没有 `--host` 时给远程 flag 是 `usage`;`--start-gtid` 与 `--start-file`/`--start-pos` 同给是 `usage`。
- **报告与快照集成。** 覆盖行将来源标记为 `remote://host:port/<name>`;`--start-file` 之前的文件记为 skipped。快照记录 `input_mode: "remote"`。

## 架构

- **`internal/remote`** —— 连接配置、清单探测、GTID 集合解析与 flavor 判定、TLS 策略，以及 `Source.Next`/`Manifest`/`Close` 背后的 `remote.Error` reason 分类。
- **`internal/binlog/remote.go`** —— `FrameSource` 接缝与 `NewRemoteParser`：完整复制事件帧走既有校验解码，远程与文件输入在下游共享同一条代码路径。
- **`cmd/binlogviz`** —— `analyze_remote.go` 把远程 flag 接入输入模式选择器（`analyze_options.go`),flag 注册抽到 `analyze_flags.go`。

## 测试与 CI

- 单元测试覆盖互斥矩阵的每条校验规则、GTID 解析（MySQL 与 MariaDB 两种形态）、TLS 模式映射、密码优先级（flag 覆盖 env)、远程错误到 `clierr` 的分类 —— 全部通过可替换的 `remoteOpenSource` 接缝完成，不触网。
- 远程 parser fixture 覆盖 rotate 事件、边界越界、以及恰好在边界终止的情形（一个 `hangingSource` 在边界帧之后再被 `Next` 会直接 panic —— 正是本次开发中 CI 抓到的挂死场景）。
- `TestAnalyzeRemoteMySQL80` 在 `flashback e2e` job 里跑真实 MySQL 8.0：建复制用户、造负载、端到端跑远程 analyze，并覆盖 `--start-file`、不存在的起点文件、错误密码三类错误形态。

## 验证

- **CI**:PR #457 的 run 40921665686(ci:verify、vet and race、darwin verify、flashback e2e、difftest、landing overflow、release-prep self-test）与 run 40921665414(lint:golangci-lint、codehealth）在候选提交上全绿；rebase 合入 main 为 `c6140a3..c1a665e`,main 上的 push run 40924667089(ci）与 40924667080(lint）全绿。
- **本地：** `go build ./...`、`CGO_ENABLED=0 go build ./...`、`go test -count=1 ./...`(25 个包）、`go vet ./...`、`make codehealth-compare`、`check_three_level_doc.sh` 全部通过。

## 兼容性

- `report_version` 4、`plan_version` 2、`schema_version` 1 均不变；唯一新增的 JSON 值是快照元数据里的 `input_mode: "remote"`。
- 全部远程 flag 均为增量；现有文件/stdin/`--from-dir` 模式不变。`--ssl-mode` 默认 `preferred` —— 与 `mysql` 客户端行为一致 —— 因此默认远程连接会尝试 TLS，仅在服务器握手阶段拒绝时才降级。
- 需要的服务器权限：`REPLICATION SLAVE` 与 `REPLICATION CLIENT`。

## 已知问题 / 范围外

- 远程输入目前只服务 `analyze` —— 远程 `flashback`、`--follow` 实时跟随、断线重连/续传、SSH 隧道、压缩复制协议、证书校验 TLS 模式都是本版本文档明确的非目标（见 ADR 0021)。
- **#256(P3,roadmap)仍开着：** 远程读取已在本版本落地；forward/redo SQL、CSV/TSV 统计、按表/事务拆分输出、行级过滤、语句指纹、relay-log 索引读取仍待做。
