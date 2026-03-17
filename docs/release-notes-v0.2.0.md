# BinlogViz v0.2.0 Release Notes

## Scope

`v0.2.0` is the planned Phase 2 release that packages the completed streaming analysis path, DuckDB-backed temp store, SQL context presentation controls, and the accompanying benchmark/fixture coverage.

## Highlights

- Streaming command path:
  - `parser -> normalize -> analyzer.Consume -> analyzer.Finalize`
- DuckDB temp-store result assembly with command-owned cleanup
- `--sql-context summary|off|full`
- Expanded command-path benchmark and fixture coverage

## Packaging Notes

- Planned release artifacts:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release downloads include a checksums manifest.
- Preferred installation path is GitHub Release artifacts; source builds remain the fallback.
