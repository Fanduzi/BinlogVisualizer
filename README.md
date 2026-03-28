<div align="center">

# BinlogViz

[![Release](https://img.shields.io/github/v/release/Fanduzi/BinlogVisualizer?display_name=tag)](https://github.com/Fanduzi/BinlogVisualizer/releases)
![Platform](https://img.shields.io/badge/platform-darwin%20amd64%20%7C%20darwin%20arm64%20%7C%20linux%20amd64%20%7C%20linux%20arm64-blue)
![Go Version](https://img.shields.io/badge/go-1.26.1-00ADD8?logo=go)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](#license)

[![English](https://img.shields.io/badge/docs-English-blue)](README.md) [![简体中文](https://img.shields.io/badge/docs-简体中文-yellow)](README_ZH.md)

[Changelog](CHANGELOG.md) | [Security](SECURITY.md) | [Release Notes](docs/releases/)
</div>

A CLI tool for MySQL binlog analysis, designed to help DBAs quickly identify hot tables, large transactions, write spikes, and workload patterns from local ROW binlog files.

## Overview

BinlogViz answers critical operational questions:

- **Which tables have the heaviest writes?**
- **Are there abnormally large transactions?**
- **Did write spikes occur at specific minutes?**
- **What does the workload summary look like for a given time window?**

## Installation

### Preferred: Release Artifact

Download the release archive for your platform from GitHub Releases, verify the checksum, and move the binary onto your `PATH`.

The authoritative release artifacts are produced by the GitHub Actions release workflow on native runners. Local `goreleaser` is only intended for config checks and optional current-host validation.

Example for `darwin/arm64` and the current release `v0.3.0`:

```bash
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.3.0/binlogviz_0.3.0_darwin_arm64.tar.gz
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.3.0/binlogviz_0.3.0_checksums.txt
shasum -a 256 -c binlogviz_0.3.0_checksums.txt 2>/dev/null | grep "binlogviz_0.3.0_darwin_arm64.tar.gz: OK"
tar -xzf binlogviz_0.3.0_darwin_arm64.tar.gz
install ./binlogviz /usr/local/bin/binlogviz
```

Or use the included install helper:

```bash
curl -fsSL https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/install.sh | sh -s -- --version v0.3.0
```

To preview the resolved artifact without downloading:

```bash
./install.sh --version v0.3.0 --dry-run
```

### Fallback: Build From Source

```bash
git clone https://github.com/Fanduzi/BinlogVisualizer.git
cd BinlogVisualizer

# Build locally
go build -o binlogviz .

# Or install into GOPATH/bin
go install .

# Or run directly
go run . analyze <binlog files...>
```

## Usage

### Basic Analysis

```bash
# Analyze a single binlog file
binlogviz analyze mysql-bin.000123

# Analyze multiple files
binlogviz analyze mysql-bin.000123 mysql-bin.000124

# Discover an ordered binlog range from a directory and prefix
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.

# Use shell expansion for multiple files
binlogviz analyze mysql-bin.*
```

### Time Window Filtering

```bash
# Analyze a specific time range (RFC3339 format)
binlogviz analyze mysql-bin.* \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

### Output Options

```bash
# JSON output for scripting or further processing
binlogviz analyze mysql-bin.* --json

# Adjust number of top items shown
binlogviz analyze mysql-bin.* --top-tables 20 --top-transactions 20
```

### Alert Detection

```bash
# Enable spike detection
binlogviz analyze mysql-bin.* --detect-spikes

# Customize large transaction thresholds
binlogviz analyze mysql-bin.* \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

### Language Support

BinlogViz supports multiple languages for runtime output (error messages, reports, progress indicators).

```bash
# Use Chinese output via command-line flag
binlogviz --lang zh-CN analyze mysql-bin.*

# Or via environment variable
LANG=zh_CN.UTF-8 binlogviz analyze mysql-bin.*
```

**Supported languages:**
- `en` - English (default)
- `zh-CN` - Simplified Chinese

**Note:** Command help text (`--help`) is always displayed in English due to CLI framework limitations. Runtime output (errors, reports, alerts) is fully localized.

## Documentation

BinlogViz now splits product documentation by reader intent so the README can stay focused on installation and the shortest path to first success.

### Concepts

- [Architecture](docs/concept/architecture.md)
- [DuckDB Temp Store](docs/concept/duckdb-temp-store.md)
- [Analysis Model](docs/concept/analysis-model.md)
- [Limitations](docs/concept/limitations.md)

### Recipes

- [Quickstart](docs/recipe/quickstart.md)
- [Analyze Local Binlogs](docs/recipe/analyze-local-binlogs.md)
- [Troubleshoot Common Errors](docs/recipe/troubleshoot-common-errors.md)

### Reference

- [CLI Reference](docs/reference/cli.md)
- [Input Discovery Reference](docs/reference/input-discovery.md)
- [Output Format Reference](docs/reference/output-format.md)

### Additional Resources

- [Examples](docs/examples/)
- [Release Notes](docs/releases/)

## Requirements

- MySQL ROW-format binlog files
- Go 1.26.1+ (for building)

## License

MIT
