# BinlogViz

A CLI tool for MySQL binlog analysis, designed to help DBAs quickly identify hot tables, large transactions, write spikes, and workload patterns from local ROW binlog files.

## Overview

BinlogViz answers critical operational questions:

- **Which tables have the heaviest writes?**
- **Are there abnormally large transactions?**
- **Did write spikes occur at specific minutes?**
- **What does the workload summary look like for a given time window?**

## Installation

```bash
# Clone and build
git clone <repository-url>
cd BinlogVisualizer
go build -o binlogviz .

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

The current implementation loads all normalized events into memory before analysis:

- **Parser**: Uses `go-mysql-org/go-mysql/replication` which streams events via callbacks
- **Command Layer**: Collects all normalized events into a slice before passing to the analyzer
- **Analyzer**: Processes events in a single pass,- **Renderer**: Outputs final result

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

For binlog files exceeding 100MB or1M+ events, consider:

1. **Use time window filtering**: `--start` and `--end` flags reduce memory footprint
2. **Process in chunks**: Split large binlog files by time range
3. **Ensure sufficient RAM**: 2-4GB for large workloads

Future versions may add improved memory efficiency through true streaming analysis without full event retention in## Limitations

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
