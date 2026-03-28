# BinlogViz v0.4.0 Release Notes

## Scope

`v0.4.0` introduces internationalization (i18n) support, allowing users to view runtime output in their preferred language.

This is a backward-compatible feature release with no breaking changes to the analysis result model or CLI interface.

## Highlights

### Internationalization (i18n)

BinlogViz now supports multiple languages for runtime output:

- **English** (`en`) - default
- **Simplified Chinese** (`zh-CN`)

### Language Selection

Two methods to switch the output language:

**Command-line flag:**
```bash
binlogviz --lang zh-CN analyze mysql-bin.*
```

**Environment variable:**
```bash
LANG=zh_CN.UTF-8 binlogviz analyze mysql-bin.*
```

### Localized Content

The following output is localized:

- Error messages (file not found, invalid arguments, etc.)
- Report sections (Workload Summary, Top Tables, Top Transactions, etc.)
- Report labels (Total Transactions, Duration, etc.)
- Alert messages (large transaction warnings, spike detection)
- Progress indicators (Parsing binlogs, Finalizing analysis)

### Known Limitation

Command help text (`--help`) and cobra's built-in validation messages remain in English. This is a limitation of the CLI framework where command structures are defined at compile time before language preferences are known.

## Operator Notes

- The `--lang` flag takes precedence over environment variables
- If an unsupported language is specified, the tool falls back to English
- All locale files are embedded in the binary; no external file dependencies

## Technical Details

- Uses `github.com/nicksnyder/go-i18n/v2` for translation management
- Thread-safe implementation with `sync.RWMutex`
- Test coverage: 85.9% for the i18n package
- All tests pass with the race detector enabled

## Packaging Notes

- Release artifacts continue to target:
  - `darwin/amd64`
  - `darwin/arm64`
  - `linux/amd64`
  - `linux/arm64`
- Release downloads include a checksums manifest
- Binary size increase: ~50KB (embedded locale files)
