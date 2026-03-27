# BinlogViz v0.3.0 Release Notes

## Scope

`v0.3.0` introduces discovery mode for easier binlog input, a version command, and a complete documentation architecture overhaul.

This is a backward-compatible feature release with no breaking changes to the analysis result model.

## Highlights

### Discovery Mode

- Added `--from-dir` and `--prefix` flags to automatically discover and order binlog files from a directory
- Files are matched by prefix and numeric suffix, then sorted by the numeric value
- Discovery mode is mutually exclusive with positional file arguments
- Resolved file list is printed to `stderr` before analysis begins

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### Version Command

- `binlogviz version` prints ASCII logo with version number
- `binlogviz --version` prints only the version number
- Version is injected at build time via ldflags

### Documentation Architecture

Restructured documentation into three sections by reader intent:

- **Concepts** (`docs/concept/`): what BinlogViz is and how it works
- **Recipes** (`docs/recipe/`): how to complete concrete tasks
- **Reference** (`docs/reference/`): stable CLI and output contracts

Each section has bilingual EN + zh-CN coverage. Root READMEs now serve as navigation hubs.

## Operator Notes

- Discovery mode requires both `--from-dir` and `--prefix` flags together
- Files without numeric suffixes after the prefix are excluded
- The resolved file list appears on `stderr`, keeping `stdout` clean for reports

## Packaging Notes

- Release artifacts continue to target:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release downloads include a checksums manifest.
- Preferred installation path remains GitHub Release artifacts; source builds remain the fallback.
