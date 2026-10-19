# BinlogViz v0.31.0 发布说明

发布日期：2026-10-20

## 概述

v0.31.0 实现了 ADR 0013：flashback 脚本只有在完整且未被修改时才会提交。发布的撤销脚本是一个事务，文件本身永远不会提交。`binlogviz flashback verify` 校验 digest，并且只有它会输出条件 `COMMIT`。被截断、篡改、重排、追加的脚本以退出码 6 拒绝，什么也不会提交（#318、#321）。`verify` 流式读取脚本，大文件上内存保持平稳（#327）。

**对执行 flashback 输出的使用方，这是不兼容变更。** `mysql < flashback.sql` 不再修改数据，完整的文件也一样。请这样执行：

```sh
binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4
```

## 不兼容变更

- **执行 flashback 脚本改为新命令（ADR 0013、#318、#321）。**
  - SQL 输出是新的封印格式，与 v0.30.0 不逐字节兼容。`plan_version` 仍为 2。
  - 整个撤销是一个事务，而不是每个原事务一个。任何一条语句失败，整个撤销回滚。
  - 磁盘上的文件以一个会报错的结尾收尾，所以 `mysql < flashback.sql`（加不加 `--force`）都会回滚，什么也不提交。
  - `binlogviz flashback verify <file>` 重新计算 digest，只有一致时才输出脚本，并把结尾换成在 MySQL 内根据封印状态选择的预处理 `COMMIT` / `ROLLBACK`。`--format json` 改为输出一个对象（`schema_version` 1、`command`、`ok`、`statements`、`digest`）。
- **`BINLOGVIZ_SPILL_BYTES` 和 `BINLOGVIZ_MEMORY_BYTES` 会做校验。** 设置时必须是十进制正整数。`abc`、`0`、`-1`、`1MB` 和空值在读取 binlog 之前就以退出码 3 结束。未设置时仍然没有溢出上限，内存上限仍是 `GOMEMLIMIT`。

## 修复

- **flashback 脚本只有在完整且未被修改时才会提交（#318、#321、ADR 0013）。**
  - 改过的值、删掉的撤销语句、调换的语句顺序、追加的 SQL、截断的文件，都会被 `flashback verify` 以退出码 6 拒绝（`details.reason` 为 `not_flashback`、`truncated`、`missing_trailer`、`appended` 或 `digest_mismatch`）。stdout 不写任何内容，在 `mysql` 下（包括 `--force`）什么也不提交。
  - 单条语句里守卫 token 不匹配时报 ERROR 1242，而不是悄悄更新 0 行。
  - 生成时先把脚本写进临时文件，写成功后才拷到 stdout，所以退出码 8 不会留下可执行的前缀。
- **`flashback verify` 改为流式，不再把整个脚本读进内存（#327）。** 边读边哈希，digest 一致后再从临时文件输出 SQL。在 56 MB–454 MB 的脚本上，峰值 RSS 约 34–41 MB；1.13 GB 的旧格式文件以约 34 MB 读完。堆上限（`BINLOGVIZ_MEMORY_BYTES`，未设置时为 `GOMEMLIMIT`）是退出码 10 `memory_limit`；写临时文件时磁盘写满是退出码 8。两者都不是退出码 2。
- `TestGeneratedReviewRSS` 在 `-race` 下跳过，失败信息写的是 64MB。

## 验证

BinlogQA 对 #326 的 PR QA（`325deb24`；此前在 `d0ac9c81` 判 FAIL 并发现 #327），以及对 main `f1a62e23` 的发布前 QA，都通过，没有 P0 或 P1。基准是已发布的 v0.30.0 归档。MySQL 8.0.36，全部在 3 GiB 地址空间限制下运行。

- **流式 verify（#327）**：56.5 / 109.7 / 166.9 / 403.6 MB 脚本和 453.7 MB 生成列脚本，verify VmHWM 34–41 MB，退出码 0（在 `d0ac9c81` 上 166.9 MB 和 403.6 MB 脚本以退出码 2 OOM）。`BINLOGVIZ_MEMORY_BYTES=1` → 退出码 10 `memory_limit`，stdout 为空。
- **完整性（#318、#321）**：完整脚本 verify 退出码 0；裸 `mysql` 加不加 `--force` 都不改行；`verify | mysql` 回滚后校验和回到事故前。篡改矩阵 52/52（PR 上 72/72）→ 退出码 6，没有提交。退出码 8（溢出写满、`/dev/full`、stdout 写满）不留可用前缀；8 MB 的半截被以 `truncated` 拒绝。
- **输出不变**：analyze 抽查 40/40 与 v0.30.0 逐字节一致。`flashback --plan` 只有文案差异。67 例矩阵退出码和 kind 一致。SQL 是新的封印格式（预期如此）。
- `go test` / `go vet` / `CGO_ENABLED=0` 构建通过；`version --format json` 正确。CI 绿。

## 兼容性

- **flashback 执行方式不兼容。** 脚本与 v0.30.0 不逐字节兼容，`mysql < file` 不再能执行。请通过 `binlogviz flashback verify` 管道执行。v0.30.0 及更早版本写出的脚本没有封印，会被 `verify` 拒绝；请用 v0.31.0 重新生成。
- `report_version` 仍为 3，`plan_version` 仍为 2。`flashback verify --format json` 是新的带版本文档（`schema_version` 1）。
- analyze 输出不变。
- `flashback` 和 `flashback verify` 都从不连接 MySQL。先审阅并测试脚本，再在主库上用一个新会话、`sql_log_bin=1` 执行。
- 生成的脚本比 v0.30.0 大（普通约 2.15 倍，`--conflict guard` 约 1.21 倍），因为每条语句都带着自己的 digest 载荷。

## 已知问题

- **#328（P2）**：目标缺表或缺列时，`verify | mysql --force` 可能提交部分撤销。结尾只检查文本完整，不检查每条语句是否成功。执行时不要加 `--force`；不加时什么也不会提交。
- **#329（P2）**：隐式提交会破坏"文件本身不提交"的保证。同一会话里截断的文件后面再跟另一个脚本、`BEGIN` 或 DDL，即使不加 `--force` 也可能把截断部分提交；加 `--force` 时，完整文件后面再跟另一个脚本、`BEGIN`、`START TRANSACTION` 或 DDL 也会提交。每个会话只执行一个脚本，并且通过 `verify` 执行。
- **#303（P2）**：Linux 发布版二进制在 `binlogviz version` 中报告的 `commit` 和 `commit_time` 为空。请用 `binlogviz_0.31.0_checksums.txt` 而不是 commit 来核对 Linux 二进制。
- #326 遗留的 P3：`flashback verify --help` 打出的是父命令帮助（`Usage` 写成 `flashback flashback`）；`verify` 不接受 `-` 作为 stdin（退出码 4）。
- **#312（P3）** 以及更早版本的其他 P3 继续沿用，`.github/workflows/README.md` 的归档清单、#285 的 `install.sh` P3，以及 #255（第 3、5、7 项）、#256、#257 也一样。

完整的保证表及其限制见 `docs/concept/limitations.zh-CN.md` 和 `docs/adr/0013-flashback-apply-integrity.md`。
