# BinlogViz v0.43.1 发布说明

发布日期：2026-11-05

## 安装或升级

```bash
brew upgrade --cask binlogviz          # macOS
# Linux：重新运行 install.sh，它会解析最新版本
```

v0.43.0 的补丁版：没有参数、退出码、JSON 字段或报告字节变化。

## 概览

v0.43.1 修的是 `--help`：它现在跟随 `--lang`。此前只有 `flashback` 会翻译自己的帮助，其它命令在报告是中文时仍打印英文。原因是命令树在构造时就把文案定下来了（早于 `--lang` 解析），而 help 又比任何运行钩子更早打印。

## 修复

- **`--help` 跟随 `--lang`。** `binlogviz --lang zh-CN analyze --help` 之前打印英文说明和英文参数说明。现在根帮助会按请求的语言重建命令树，因此 `analyze`、`compare`、`trend`、`snapshot`、`workflow`、`schema`、`redo`、`version` 以及 `help` 自身的说明、子命令列表和每一行参数说明都跟随 `--lang`。`flashback` 保留自己的帮助路径，行为不变。不带 `--lang` 时语言仍按环境变量检测，报告输出完全不变。
- **CLI 参考照着改。** `docs/reference/cli.md` 与 `.zh-CN.md` 的 `--lang` 一行现在写明它也作用于 `--help`。

## 兼容性

- 没有参数、退出码、节名或文本布局变化。`report_version` 仍是 4，`plan_version` 仍是 2，`flashback verify --format json` 的 `schema_version` 仍是 1。
- 唯一变化的输出是帮助文本，而且只在 `--lang` 指定的语言与进程启动时的语言不同时才会变。

## 测试与 CI

- `TestHelpFollowsTheLanguageFlag` 跑 `--lang zh-CN analyze --help`、`--lang en analyze --help` 和另一条命令，断言长说明与一行参数说明都用该语言。
- `TestFlashbackMarkedExperimental`（本来就会跑 `--lang zh-CN flashback --help`）与 `TestCommandExamplesIncludeSpikeRecipe` 仍然通过，说明 flashback 路径和示例没有受影响。
- `go test ./cmd/binlogviz ./internal/...`、`make codehealth`、`make lint`、`gofmt` 干净；PR 通过了全部 11 项必需检查。

## 验证

打了 tag 的提交，其父提交就是那个合并的修复；`main` 在它上面通过了 `ci` 与 `lint`（`verify`、`vet and race`、`flashback e2e`、`flashback difftest`、`MySQL 5.7 compatibility`、`codehealth`、`golangci-lint`、`release-prep self-test`、`landing overflow`、`darwin verify`）。
