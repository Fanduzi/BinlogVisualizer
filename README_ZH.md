<div align="center">

# BinlogViz

[![Release](https://img.shields.io/github/v/release/Fanduzi/BinlogVisualizer?display_name=tag)](https://github.com/Fanduzi/BinlogVisualizer/releases)
![Platform](https://img.shields.io/badge/platform-darwin%20amd64%20%7C%20darwin%20arm64%20%7C%20linux%20amd64%20%7C%20linux%20arm64-blue)
![Go Version](https://img.shields.io/badge/go-1.26.10-00ADD8?logo=go)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](#license)

[![English](https://img.shields.io/badge/docs-English-blue)](README.md) [![简体中文](https://img.shields.io/badge/docs-简体中文-yellow)](README_ZH.md)

[![变更记录](https://img.shields.io/badge/变更记录-informational)](https://github.com/Fanduzi/BinlogVisualizer/blob/main/CHANGELOG.md) [![安全策略](https://img.shields.io/badge/安全策略-important)](https://github.com/Fanduzi/BinlogVisualizer/blob/main/SECURITY.md) [![发行说明](https://img.shields.io/badge/发行说明-success)](https://github.com/Fanduzi/BinlogVisualizer/tree/main/docs/releases)
</div>

BinlogViz 是一个面向 DBA 和运维人员的本地 MySQL `ROW` binlog 分析 CLI。它专门用于回答真实运维问题：哪些表写入最重、哪些事务异常大、尖峰发生在哪些分钟、某个故障窗口内的负载究竟发生了什么。

BinlogViz 做的是 **ROW binlog 的快速摘要**：热表、写入形态、上线前后 compare。510 MB 级文件通常只要数秒。

它**不是** STATEMENT / MIXED 全量分析器——这类文件会空成功或只统计到 ROW 子集。报告里的位置是文件证据；只有当跨度覆盖事务事件、而不是只有 XID 区间时，才把它当作 `mysqlbinlog --start-position`。

## 截图

### Analyze HTML 报告

![Analyze HTML 报告](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/images/analyze-html.png)

### Compare HTML 报告

![Compare HTML 报告](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/images/compare-html.png)

## 安装

### macOS 首选：Homebrew Cask

Homebrew **仅适用于 macOS**。Linux 请用下面的 tarball 或 `install.sh`。

```bash
brew tap Fanduzi/binlogviz
brew install --cask binlogviz
```

这条路径会安装预编译 release artifact，并在安装时移除 macOS quarantine 属性；用户不需要额外安装 DuckDB。

### Linux：tarball 或 install.sh

```bash
# install.sh（当前版本）
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/install.sh
sh ./install.sh            # latest release; pin with --version vX.Y.Z

# 或 linux/amd64 tarball：先解析最新 tag，校验 checksum，再解压
VERSION="$(curl -fsSLI -o /dev/null -w '%{url_effective}' https://github.com/Fanduzi/BinlogVisualizer/releases/latest)"; VERSION="${VERSION##*/}"
curl -fsSLO "https://github.com/Fanduzi/BinlogVisualizer/releases/download/${VERSION}/binlogviz_${VERSION#v}_linux_amd64.tar.gz"
curl -fsSLO "https://github.com/Fanduzi/BinlogVisualizer/releases/download/${VERSION}/binlogviz_${VERSION#v}_checksums.txt"
grep " binlogviz_${VERSION#v}_linux_amd64.tar.gz\$" "binlogviz_${VERSION#v}_checksums.txt" | sha256sum -c - &&
  mkdir -p "binlogviz_${VERSION#v}" &&
  tar -xzf "binlogviz_${VERSION#v}_linux_amd64.tar.gz" -C "binlogviz_${VERSION#v}" &&
  install "binlogviz_${VERSION#v}/binlogviz" /usr/local/bin/binlogviz
```

ARM Linux 把 `linux_amd64` 换成 `linux_arm64`。macOS 不用 Homebrew 时，把上面命令里的 `linux_amd64` 换成 `darwin_arm64` 或 `darwin_amd64`，把 `sha256sum -c -` 换成 `shasum -a 256 -c -`。checksum 不符时会打印 `FAILED` 并以非零退出，此时不要继续。所有归档见 [GitHub Releases](https://github.com/Fanduzi/BinlogVisualizer/releases)。

`install.sh` 安装最新版本；sha256 与 `checksums.txt` 不符或找不到 sha256 工具时拒绝安装（确实要跳过校验时传 `--no-verify`）；支持 `--version vX.Y.Z`、`--bin-dir DIR` 和 `--dry-run`。

tarball 解压出来是散落的文件（二进制、两份 README、`docs/`、`testdata/`、`incident.yaml`），所以上面的命令先解压到单独的目录。写入 `/usr/local/bin` 需要 root：用 `sudo sh ./install.sh` 和 `sudo install …`，或者用 `sh ./install.sh --bin-dir ~/.local/bin` 装到你自己有权限的目录。

### 备选：从源码构建

```bash
git clone https://github.com/Fanduzi/BinlogVisualizer.git
cd BinlogVisualizer

go build -o binlogviz .
go install .
go run . analyze <binlog files...>
```

如果你从源码构建且没有注入 release ldflags，那么 `binlogviz --version` 会显示 `dev`，而不是某个发布版本号。

### 验证二进制

```bash
binlogviz --version
binlogviz version
```

- `binlogviz --version` 只输出版本号
- `binlogviz version` 输出 ASCII Logo 加 `binlogviz <version>`

## 快速开始

### 用样例 ROW binlog 验证安装

```bash
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/cmd/binlogviz/testdata/minimal.binlog
binlogviz analyze minimal.binlog
```

Release 归档也带这份样本，以及解压后即可运行的 `incident.yaml`。归档内容、stdin 输入、热点线程、热点行和退出码见 [Analyze 行为细节](docs/reference/analyze-behavior.zh-CN.md)。

### 检查你自己的文件

```bash
binlogviz analyze mysql-bin.000123
cat mysql-bin.000123 | binlogviz analyze -
```

### 按 binlog 顺序分析整个目录

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### 直接读取存活服务器，无需复制文件

远程模式以伪副本身份连接（`COM_BINLOG_DUMP`），等价于 `mysqlbinlog -R`。账号需要 `REPLICATION SLAVE` 和 `REPLICATION CLIENT`。密码来自 `BINLOGVIZ_MYSQL_PASSWORD` 或 `--password`，绝不打印。

```bash
export BINLOGVIZ_MYSQL_PASSWORD='secret'
binlogviz analyze --host db.internal --user replica
binlogviz analyze --host db.internal --user replica \
  --start-file binlog.000041 --start-pos 12345     # 或 --start-gtid 'uuid:1-5' / '0-1-300'
```

读取在连接时快照的 `SHOW BINARY LOGS` 末尾处停止，不会追赶正在写入的服务器。起点选择器、`--ssl-mode` 和错误契约见 [Analyze 行为细节](docs/reference/analyze-behavior.zh-CN.md)。

### 聚焦某个故障时间窗口

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

### 从 `SHOW MASTER STATUS` 位点或 GTID 起

位点是单个显式文件上的精确事件边界，使用半开区间 `[start, stop)`。也可以同时给时间条件；各谓词取交集。

```bash
binlogviz analyze mysql-bin.000015 --start-position 1651 --stop-position 4096
binlogviz analyze mysql-bin.000015 --include-gtids '24bc7850-2c16-11e6-a073-0242ac110002:7-12'
binlogviz analyze mariadb-bin.000015 --include-gtids '0-7-1857,0-7-1859' --exclude-gtids '0-7-1859'
```

### 只看某个 schema 或表

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders \
  --include-table payments
```

### 把机器可读结果交给脚本或其他工具

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

### 生成 Markdown 或 HTML 报告

```bash
# Markdown — 粘贴到 GitHub issue、wiki 或文档
binlogviz analyze mysql-bin.000123 --format markdown > report.md

# HTML — 默认写入文件（例如 mysql-bin.000123.html）
binlogviz analyze mysql-bin.000123 --format html

# HTML — 指定输出路径
binlogviz analyze mysql-bin.000123 --format html --output report.html

# HTML — 输出到 stdout 以供管道使用（旧行为）
binlogviz analyze mysql-bin.000123 --format html --output -
```

HTML 报告包含交互式图表（每分钟行数/事务数、热点表、操作类型分布）、高信号写入模式的可选模式钻取（Pattern Drilldowns），以及五主题切换器。

## 找到并撤销误改的行

### 找到误删并看到被删的行

```bash
binlogviz analyze mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  --start "2026-10-06 14:00:00" \
  --end "2026-10-06 14:10:00" \
  --show-rows
```

### 找到误操作的 DROP

```bash
binlogviz analyze mysql-bin.000123
```

报告里的 DDL 时间线列出每条 `DROP TABLE`、`TRUNCATE` 和 `ALTER` 的 GTID 和起始位置，可直接用作时间点恢复的 stop GTID 或 `--stop-position`。见 [找到误操作的 DROP](docs/recipe/flashback.zh-CN.md#找到误操作的-drop)。

### 撤销误改的行

先用 `analyze`、DDL 时间线或 `--show-rows` 找到那个事务，再打印只撤销这些行变更的 SQL——本地 binlog 或在线服务器（`--host` 加远程参数；读取以连接时的 binlog 清单为界）都可以。先审脚本，再自己执行。除了用 `--host` 读取远程 binlog 之外，flashback 不连接数据库。flashback 是实验功能：执行生成的 SQL 之前必须先审阅。

```bash
binlogviz analyze mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  --show-rows

binlogviz flashback mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  > flashback.sql

binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4
```

`--show-rows` 的上限、DDL 时间线的停止点、flashback 的前提条件、拒绝情形、冲突、执行权限和提交标记见 [找到并撤销误改的行](docs/recipe/flashback.zh-CN.md)。

### 把变更重演成前滚 SQL

```bash
binlogviz redo mysql-bin.000123 --include-table shop.orders > redo.sql
```

`redo` 打印可重放的前滚 SQL——即 `2sql` 方向——输入和选择条件与 `analyze` 相同，包括有界远端读取。redo 是实验功能：执行前先审阅脚本。[前提条件和拒绝情形](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/cli.zh-CN.md#redo-命令语法)。

## 快照、对比、趋势与 workflow

### 把两个故障窗口保存成快照后再做对比

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z" \
  --format json \
  --snapshot-name incident_current

binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-08T10:00:00Z" \
  --end "2026-03-08T10:30:00Z" \
  --format json \
  --snapshot-name incident_baseline

binlogviz snapshot list
binlogviz snapshot list --format json
binlogviz snapshot show incident_current
binlogviz snapshot show incident_current --format json
binlogviz compare \
  --current-snapshot incident_current \
  --baseline-snapshot incident_baseline \
  --format html > compare.html

# 用完之后再重命名或删除快照
binlogviz snapshot rename incident_current incident_current_renamed
binlogviz snapshot delete incident_current_renamed
```

当设置 `--snapshot-name` 时，`analyze --format json` 仍然会把 JSON 报告写到 `stdout`，同时把同一份载荷保存到 `~/.binlogviz/snapshots/<name>.json`。保存成功提示会打印到 `stderr`。

### 把多个快照当作一条时间线查看趋势

```bash
binlogviz trend incident_week1 incident_week2 incident_week3 --format text

binlogviz trend --from-snapshots 'incident_week*' \
  --baseline-snapshot baseline_weekly \
  --format html > trend.html
```

### 用一份 plan 文件跑多步调查

仓库自带一份可运行的 `incident.yaml`，指向 1500 字节的 ROW 样本，在仓库根目录即可运行：

```bash
binlogviz workflow run incident.yaml
tree artifacts/incident
```

compare 的可比性、trend 的排序，以及 `workflow resume`、`status`、`clean`、`export`、`validate`、`describe` 的生命周期见 [快照、对比、趋势与 workflow](docs/recipe/compare-trend-workflow.zh-CN.md)。

## BinlogViz 适合回答什么问题

BinlogViz 重点服务这些 DBA 常见问题：

- **哪些表承受了最重的写入负载？**
- **哪个主键被 UPDATE 或 DELETE 的次数最多？**
- **哪些 UPDATE 或 DELETE 打到了没有主键的表？**
- **这份副本当时落后多少，是哪些事务造成的？**见 [为什么副本在延迟？](docs/reference/analyze-behavior.zh-CN.md#为什么副本在延迟)。
- **哪些事务大到值得优先排查？**
- **某个分钟级尖峰是否真实发生过？**
- **指定故障窗口内到底发生了什么变化？**
- **当前窗口和可信基线相比，负载差异到底在哪里？**
- **结果能否安全交给脚本、管道或其他工具？**

## 多语言支持

BinlogViz 支持多语言运行时输出，包括错误信息、报告和进度提示。

```bash
binlogviz --lang zh-CN analyze mysql-bin.000123
LANG=zh_CN.UTF-8 binlogviz analyze mysql-bin.000123
```

支持的语言：

- `en` - English（默认）
- `zh-CN` - 简体中文

`--help` 当前仍然是英文，因为 help 文本生成早于语言初始化；运行时输出已支持本地化。

## 按任务阅读文档

### 建议先读这些

- [快速开始](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/recipe/quickstart.zh-CN.md)
- [分析本地 Binlog](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/recipe/analyze-local-binlogs.zh-CN.md)
- [常见错误排查](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/recipe/troubleshoot-common-errors.zh-CN.md)
- [找到并撤销误改的行](docs/recipe/flashback.zh-CN.md)
- [把变更重演成前滚 SQL（`redo`）](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/cli.zh-CN.md#redo-命令语法)
- [快照、对比、趋势与 workflow](docs/recipe/compare-trend-workflow.zh-CN.md)

### 需要精确契约和参数行为时

- [CLI 参考](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/cli.zh-CN.md)
- [输入发现参考](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/input-discovery.zh-CN.md)
- [输出格式参考](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/output-format.zh-CN.md)
- [Analyze 行为细节](docs/reference/analyze-behavior.zh-CN.md)

### 需要理解内部设计或分析模型时

- [产品架构](docs/concept/architecture.zh-CN.md)
- [DuckDB 临时存储](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/concept/duckdb-temp-store.zh-CN.md)
- [分析模型](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/concept/analysis-model.zh-CN.md)
- [限制与边界](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/concept/limitations.zh-CN.md)

### 发布与补充资料

- [示例输出](https://github.com/Fanduzi/BinlogVisualizer/tree/main/docs/examples)
- [发行说明](https://github.com/Fanduzi/BinlogVisualizer/tree/main/docs/releases)
- [变更记录](https://github.com/Fanduzi/BinlogVisualizer/blob/main/CHANGELOG.md)
- [安全策略](https://github.com/Fanduzi/BinlogVisualizer/blob/main/SECURITY.md)

## 架构

Cobra 命令层把规范化后的 binlog 事件流式交给分析器，再把结果交给 report、snapshot、compare、trend 和 workflow 模块。流水线和模块表见 [产品架构](docs/concept/architecture.zh-CN.md)。

## 环境要求

- 本地 MySQL `ROW` 格式 binlog 文件
- 如果从源码构建，需要 Go 1.26.10+

## License

Apache 2.0
