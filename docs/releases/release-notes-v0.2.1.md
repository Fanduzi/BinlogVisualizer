# BinlogViz v0.2.1 Release Notes

## Scope

`v0.2.1` is the first publishable Phase 2 release after fixing the GitHub Actions release matrix and workflow alignment issues found during the original `v0.2.0` release attempt.

## Highlights

- Streaming command path:
  - `parser -> normalize -> analyzer.Consume -> analyzer.Finalize`
- DuckDB temp-store result assembly with command-owned cleanup
- `--sql-context summary|off|full`
- Expanded command-path benchmark and fixture coverage
- Release workflow aligned with current GitHub hosted runner labels

## Packaging Notes

- Planned release artifacts:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release downloads include a checksums manifest.
- Preferred installation path is GitHub Release artifacts; source builds remain the fallback.
