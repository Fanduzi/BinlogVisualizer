# Reference Docs

Reference documents define stable user-facing contracts for BinlogViz.

## Available Documents

- [CLI Reference](cli.md) — command syntax, flags, and input rules
- [Input Discovery Reference](input-discovery.md) — discovery mode contract
- [Output Format Reference](output-format.md) — text and JSON output semantics
- [Analyze Behavior Details](analyze-behavior.md) — stdin input, Top Threads, Hot Rows, exit codes, replica delay, and the performance gate

All reference docs have Chinese counterparts: append `.zh-CN.md` to the filename.
Both CLI references document the global output flags; `cmd/binlogviz/cli_reference_flags_test.go` checks public command-tree flag names against each reference.
