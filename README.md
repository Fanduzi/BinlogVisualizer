# BinlogViz

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

Example for `darwin/arm64` and the planned Phase 2 release `v0.2.0`:

```bash
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.2.0/binlogviz_0.2.0_darwin_arm64.tar.gz
curl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v0.2.0/binlogviz_0.2.0_checksums.txt
shasum -a 256 -c binlogviz_0.2.0_checksums.txt 2>/dev/null | grep "binlogviz_0.2.0_darwin_arm64.tar.gz: OK"
tar -xzf binlogviz_0.2.0_darwin_arm64.tar.gz
install ./binlogviz /usr/local/bin/binlogviz
```

Or use the included install helper:

```bash
curl -fsSL https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/install.sh | sh -s -- --version v0.2.0
```

To preview the resolved artifact without downloading:

```bash
./install.sh --version v0.2.0 --dry-run
```

### Fallback: Build From Source

```bash
git clone <repository-url>
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

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--start` | (none) | Start time (inclusive, RFC3339 format) |
| `--end` | (none) | End time (inclusive, RFC3339 format) |
| `--json` | false | Output in JSON format |
| `--sql-context` | summary | SQL context presentation mode: `summary`, `off`, or `full` |
| `--top-tables` | 10 | Number of top tables to show |
| `--top-transactions` | 10 | Number of top transactions to show |
| `--detect-spikes` | false | Enable write spike detection |
| `--large-trx-rows` | 1000 | Rows threshold for large transaction alerts |
| `--large-trx-duration` | 30s | Duration threshold for large transaction alerts |

## Report Sections

The output contains five sections:

### 1. Workload Summary

Overall statistics for the analyzed time window:
- Total transactions count
- Total rows affected
- Total events processed
- Time range and duration

### 2. Top Tables

Tables ranked by total rows affected, showing:
- Schema and table name
- Total row count
- Breakdown by operation (INSERT/UPDATE/DELETE)
- Number of distinct transactions touching the table

### 3. Top Transactions

Largest transactions ranked by total rows, showing:
- Transaction identifier
- Row count and duration
- Event count

### 4. Minute Activity

Per-minute breakdown of write activity:
- Rows written per minute
- Transaction count per minute

### 5. Alerts

Detected anomalies including:
- **Large Transaction**: Transactions exceeding row or duration thresholds
- **Write Spike**: Minutes with abnormally high write activity (when `--detect-spikes` is enabled)

## Examples

See example outputs in:
- [Text Output](docs/examples/analyze-output.txt)
- [JSON Output](docs/examples/analyze-output.json)

## Requirements

- MySQL ROW-format binlog files
- Go 1.24+ (for building)

## Large File Handling

BinlogViz is designed for MVP efficiency and has the following characteristics:

### Memory Model

The current implementation uses a streaming command path with DuckDB-backed finalize-time result assembly:

- **Parser**: Streams raw binlog events via callbacks
- **Command Layer**: Immediately normalizes and forwards events to `analyzer.Consume`
- **Analyzer**: Keeps bounded live state in memory
- **DuckDB Temp Store**: Persists completed high-cardinality results for `Finalize()`
- **Renderer**: Outputs the final assembled report

### Expected Performance

From benchmarks on Apple M4 Pro:

| Input Size | Time/op | Memory/op | Allocs/op |
|-----------|---------|------------|-----------|
| 1 event | ~1μs | 2.5 KB | 32 |
| 100 events | ~40μs | 55 KB | 756 |
| 1000 events | ~492μs | 665 KB | 7.1K |
| 100 tables | ~41μs | 55 KB | 756 |
| 10 transactions | ~245 ns | 469 B | 12 |

### Large File Recommendations

For large binlog files:

1. Prefer analyzing ordered binlog ranges directly; the command path is already streaming.
2. Ensure sufficient disk space for the temporary DuckDB result store used during analysis.

## Limitations

- **ROW binlog only**: STATEMENT and MIXED formats are not supported in MVP
- **Local files only**: Cannot connect to MySQL servers directly
- **No real-time streaming**: Analysis is performed on static files
- **No SQL reconstruction**: Shows aggregated statistics, not actual SQL statements
- **No row values**: Focuses on operation patterns, not data content

## Non-Goals

BinlogViz is intentionally not:

- A replication debugger
- A SQL replayer
- A real-time monitoring tool
- A Prometheus exporter
- A web-based dashboard
- An AI-powered anomaly detector

## Architecture

BinlogViz uses a single-pass streaming analysis pipeline:

```
binlog files → parser → normalizer → analyzer → renderer → output
```

Components:
- **Parser**: Wraps `go-mysql-org/go-mysql/replication` for binlog parsing
- **Normalizer**: Converts parser events to stable internal format
- **Analyzer**: Reconstructs transactions, aggregates tables/minutes, detects alerts
- **Renderer**: Produces text or JSON output

## License

MIT
