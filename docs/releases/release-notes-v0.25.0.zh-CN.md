# BinlogViz v0.25.0 发布说明

发布日期：2026-10-17

## 概述

v0.25.0 把退出码分类推广到所有命令，让 `workflow clean` 可以放心使用，让 `install.sh` 出错时拒绝安装，并能解析真实 MySQL 8.0 中 16 MiB 及以上的压缩事务。退出码的设计记录在 ADR 0009（`docs/adr/0009-exit-code-taxonomy-everywhere.md`）。

**对检查退出码的脚本来说，这是一个不兼容的版本。** 升级自动化脚本之前，请先阅读下面的“不兼容变更：退出码”。

## 不兼容变更：退出码（#277、#284、#286、ADR 0009）

- 每个命令错误现在都有类别（kind）。只要有命令错误没有类别，测试就会让构建失败。
- 新增退出码 **9 `unsupported`**，只用于格式和版本限制：没有 ROW 镜像的 STATEMENT 或 MIXED binlog、`report_version` 比当前版本新的报告、当前版本无法运行的 workflow plan 或 manifest 版本。
- 很多原来退出码为 1 的失败，现在改为 3（usage）、4（input）、5（corrupt）、6（refused）或 8（output）。例如：`snapshot save` 不带 `--name`（3）、快照不存在或 workflow plan 缺失（4）、报告或快照是 `{}`（5）、plan.yaml 改动后执行 `workflow resume`（6，`plan_changed`）、快照或产物写入失败（8）。
- `--error-format=`（空值）现在是用法错误（3），不再被忽略。`analyze /dev/null` 和空 stdin 退出码为 5，与空文件一致。
- 退出码 1 现在只用于没有更合适类别的失败：ADR 0001 的未结束 `BEGIN`、无法分类的 QUERY，以及内部错误。
- JSON 错误中的 `kind`（`--error-format json`）是稳定的契约。完整对照表见 `CHANGELOG.md` 和 [docs/reference/exit-codes.md](../reference/exit-codes.md)。用 `$? -eq 1` 或 `$? -eq 4` 判断的脚本需要更新。

## 变更

- **`workflow clean` 基于 ledger（#287、#288、#289、#290、#291）**：
  - `workflow run`/`resume` 会把写出的每个文件（路径、步骤、run id、sha256）追加到 `<output_dir>/ledger.jsonl`。clean 只删除 ledger 中记录、sha256 仍然匹配、且不属于当前任何步骤的文件；其余文件列在 `kept_orphans` 中。新增的 `--force` 还会删除已记录步骤类型中没有归属的产物。
  - 文件会移到 `<root>/.trash/<id>/`，而不是直接删除；移动前会先检查所有目标。
  - 以下情况 clean 会拒绝，且不删除任何文件：manifest 不可信（`{}` 退出码 5；没有记录步骤但存在产物，退出码 6）、上一次运行失败（6，`run_not_successful`）、类型目录是符号链接（6，`trust_boundary`）、ledger 路径是绝对路径或包含 `..`（5，`untrusted_ledger`）、`manifest.snapshot_dir` 与 `--snapshot-dir` 不一致（6，`snapshot_dir_mismatch`）。
  - `workflow run`、`resume` 和 `clean` 会对 `<root>/.lock` 加排他锁（6，`locked`）。`workflow export` 会包含 `ledger.jsonl`，并把 `snapshot_dir` 写成相对路径 `snapshots`。
- **`install.sh` 出错即拒绝安装（#281、#283、#285、#292）**：
  - 不带 `--version` 时安装最新版本，文档不会再与版本脱节。
  - 校验和不匹配、`checksums.txt` 中缺少该安装包、或者没有 `sha256sum`/`shasum`/`openssl` 时，退出码为 1，不安装任何东西。`--no-verify` 可以有意跳过校验，并会给出警告。
  - 没有 `curl` 时可以用 `wget`；接受不带 `v` 的 `--version 0.25.0`；版本不存在时会给出 URL。安装失败或被中断时，旧的二进制保持不变。
  - README 中的 tarball 示例会解析最新标签，校验失败时在解包前停止。
- **16 MiB 及以上的压缩事务（#278、#293）**：开启 `binlog_transaction_compression=ON` 时，MySQL 会用 9 字节字段记录 16 MiB 及以上 payload 的解压后大小，v0.24.0 会以退出码 5 拒绝。现在可以正常解析。
  - 解压改为有界的流式处理。每个内部事件解码后立即校验并释放；整个 payload 解码无误后才交给 `analyze`/`flashback`，因此最后一个事件损坏的 payload 不会输出任何内容，退出码为 5。
  - 一个 payload 声明的解压后大小最多 1 GiB，最多包含 65536 个不同的表。超出上限时报错类似 `payload declares 1.0 GiB (1073741825 bytes) uncompressed, more than the limit 1.0 GiB (1073741824 bytes)`（退出码 5）。
  - 合法的 payload 会解压两遍（先校验，再交付）。约 1 GiB、全是小事件的 payload 大约需要 45 秒。

## 验证

BinlogQA 在 main `353a2dd0` 上的 QA 通过，此前在 `9fde909b` 上的完整发版前 QA 也已通过。

- **压缩 binlog**：真实 MySQL 8.0.36 生成的 20、21、30 MB binlog（事务超过 16 MiB）可以解析，峰值内存 95–160 MB，flashback plan 与写入一致。小于 16 MiB 的 binlog 与 v0.24.0 逐字节一致。
- **恶意输入**：压缩炸弹（33 KB，声明 1 GiB）和一个 99 KB、约 1470 万个小事件、声明约 1 GiB 的文件，都以退出码 5 结束，峰值内存不超过 44 MB，stdout 为空。86 个构造文件都没有出现退出码 2。
- **退出码**：67 条矩阵中 3/4/5/6/9 的每个 reason 都与 CHANGELOG 和 `docs/reference/exit-codes.md` 一致。
- **与 v0.24.0 对比 analyze/flashback**：312 次运行，13 处差异：12 处是 STATEMENT binlog 改为退出码 9，1 处是 HTML 时间戳。
- **Workflow**：validate、run、status、resume、rerun、clean（预览、`--apply`、`--force`）、export 和恢复、并发锁、失败后的 clean 都通过。
- **install.sh**：篡改的安装包、缺失的校验和、没有 sha256 工具、只有 wget、安装被中断，行为都符合上面的描述。
- CI 和 `go test ./...` 通过。

## 兼容性

- **退出码有变化**（见上文）。JSON 错误格式本身不变。
- 早期版本的 workflow 目录没有 `ledger.jsonl`，`workflow clean` 会拒绝（6，`no_ledger`）。可以重新执行 `binlogviz workflow run <plan.yaml>` 到该目录来重建，或者加 `--force`（它不会删除快照）。
- clean 的 JSON 新增 `force`、`ledger`、`kept_orphans` 和 `skip_reasons`；`--apply` 新增 `trash_dir` 和 `snapshot_trash_dir`。manifest 新增 `run_id`。
- JSON `report_version` 仍为 `3`。快照、产物名称和支持的平台与 v0.24.0 相同。
- `flashback` 从不连接 MySQL。请先审阅并测试脚本，再在主库上用一个新会话、`sql_log_bin=1`、带脚本头执行。

## 已知问题

- #285 的 install.sh P3：大写十六进制的校验和会被判为不匹配；`--version` 可以包含 `/`；README 中的 `--dry-run` 示例需要改成 `sh ./install.sh`。
- #255、#256、#257、#270 沿用 v0.24.0 的状态。

从早期版本延续的 flashback 限制见 `docs/concept/limitations.md`。
