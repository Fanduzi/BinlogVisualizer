<div align="center">

# BinlogViz

[![Release](https://img.shields.io/github/v/release/Fanduzi/BinlogVisualizer?display_name=tag)](https://github.com/Fanduzi/BinlogVisualizer/releases)
![Platform](https://img.shields.io/badge/platform-darwin%20amd64%20%7C%20darwin%20arm64%20%7C%20linux%20amd64%20%7C%20linux%20arm64-blue)
![Go Version](https://img.shields.io/badge/go-1.26.10-00ADD8?logo=go)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](#license)

[![English](https://img.shields.io/badge/docs-English-blue)](README.md) [![简体中文](https://img.shields.io/badge/docs-简体中文-yellow)](README_ZH.md)

[![Changelog](https://img.shields.io/badge/Changelog-informational)](https://github.com/Fanduzi/BinlogVisualizer/blob/main/CHANGELOG.md) [![Security](https://img.shields.io/badge/Security-important)](https://github.com/Fanduzi/BinlogVisualizer/blob/main/SECURITY.md) [![Release Notes](https://img.shields.io/badge/Release_Notes-success)](https://github.com/Fanduzi/BinlogVisualizer/tree/main/docs/releases)
</div>

BinlogViz is a local CLI for MySQL `ROW` binlog analysis. It is built for DBAs and operators who need to quickly answer practical questions from real binlog files: which tables are absorbing the most writes, which transactions are unusually large, where spikes happened, and how workload changed over a time window.

BinlogViz is a **fast ROW-binlog summary**: hot tables, write shapes, and before/after compare. A 510 MB file is typically a few seconds.

It is **not** a full STATEMENT/MIXED analyzer — those files come back empty or undercounted (only ROW images are counted). Printed positions are file evidence; use them as `mysqlbinlog --start-position` only when the reported span covers the transaction events, not an XID-only interval.

## Screenshots

### Analyze HTML report

![Analyze HTML report](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/images/analyze-html.png)

### Compare HTML report

![Compare HTML report](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/images/compare-html.png)

## Installation

### Preferred on macOS: Homebrew Cask

Homebrew is **macOS-only**. Linux users should use the tarball or `install.sh` one-liner below.

```bash
brew tap Fanduzi/binlogviz
brew install --cask binlogviz
```

This path installs the prebuilt release artifact and removes the macOS quarantine attribute during installation, so you do not need to install DuckDB separately.

### Linux: tarball or install.sh

```bash
# install.sh (current release)
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/install.sh
sh ./install.sh            # latest release; pin with --version vX.Y.Z

# or the linux/amd64 tarball: resolve the latest tag, verify the checksum, unpack
VERSION="$(curl -fsSLI -o /dev/null -w '%{url_effective}' https://github.com/Fanduzi/BinlogVisualizer/releases/latest)"; VERSION="${VERSION##*/}"
curl -fsSLO "https://github.com/Fanduzi/BinlogVisualizer/releases/download/${VERSION}/binlogviz_${VERSION#v}_linux_amd64.tar.gz"
curl -fsSLO "https://github.com/Fanduzi/BinlogVisualizer/releases/download/${VERSION}/binlogviz_${VERSION#v}_checksums.txt"
grep " binlogviz_${VERSION#v}_linux_amd64.tar.gz\$" "binlogviz_${VERSION#v}_checksums.txt" | sha256sum -c - &&
  mkdir -p "binlogviz_${VERSION#v}" &&
  tar -xzf "binlogviz_${VERSION#v}_linux_amd64.tar.gz" -C "binlogviz_${VERSION#v}" &&
  install "binlogviz_${VERSION#v}/binlogviz" /usr/local/bin/binlogviz
```

On ARM Linux, use `linux_arm64` in place of `linux_amd64`. On macOS without Homebrew, use the same commands with `darwin_arm64` or `darwin_amd64` in place of `linux_amd64` and `shasum -a 256 -c -` in place of `sha256sum -c -`. A checksum mismatch prints `FAILED` and exits non-zero; stop there. All archives are on [GitHub Releases](https://github.com/Fanduzi/BinlogVisualizer/releases).

`install.sh` installs the latest release, refuses to install when the sha256 does not match `checksums.txt` or no sha256 tool exists (pass `--no-verify` to skip the check on purpose), and accepts `--version vX.Y.Z`, `--bin-dir DIR`, and `--dry-run`.

The tarball unpacks loose files (the binary, both READMEs, `docs/`, `testdata/`, `incident.yaml`), so the commands above unpack it into its own directory. Writing to `/usr/local/bin` needs root: run `sudo sh ./install.sh` and `sudo install …`, or install into a directory you own with `sh ./install.sh --bin-dir ~/.local/bin`.

### Fallback: Build From Source

```bash
git clone https://github.com/Fanduzi/BinlogVisualizer.git
cd BinlogVisualizer

go build -o binlogviz .
go install .
go run . analyze <binlog files...>
```

If you build from source without release ldflags, `binlogviz --version` reports `dev` instead of a tagged release version.

### Verify the Binary

```bash
binlogviz --version
binlogviz version
```

- `binlogviz --version` prints only the version string
- `binlogviz version` prints the ASCII logo plus `binlogviz <version>`

## Quick Start

### Verify the install with the sample ROW binlog

```bash
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/cmd/binlogviz/testdata/minimal.binlog
binlogviz analyze minimal.binlog
```

Release archives ship the same sample and an `incident.yaml` that runs from the extract directory. Archive contents, stdin input, Top Threads, Hot Rows, and exit codes are in [Analyze Behavior Details](docs/reference/analyze-behavior.md).

### Inspect one of your own files

```bash
binlogviz analyze mysql-bin.000123
cat mysql-bin.000123 | binlogviz analyze -
```

### Analyze a whole directory in binlog order

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### Narrow to an incident window

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

### Start from `SHOW MASTER STATUS` position or GTID

Positions are exact event boundaries on one explicit file and use a half-open `[start, stop)` range. Time flags may be supplied too; the predicates intersect.

```bash
binlogviz analyze mysql-bin.000015 --start-position 1651 --stop-position 4096
binlogviz analyze mysql-bin.000015 --include-gtids '24bc7850-2c16-11e6-a073-0242ac110002:7-12'
binlogviz analyze mariadb-bin.000015 --include-gtids '0-7-1857,0-7-1859' --exclude-gtids '0-7-1859'
```

### Focus on one schema or table

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders \
  --include-table payments
```

### Send machine-readable output to another tool

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

### Generate a Markdown or HTML report

```bash
# Markdown — paste into GitHub issues, wikis, or docs
binlogviz analyze mysql-bin.000123 --format markdown > report.md

# HTML — redirected stdout receives the document
binlogviz analyze mysql-bin.000123 --format html > report.html

# HTML — explicit output path
binlogviz analyze mysql-bin.000123 --format html --output report.html

# HTML — force stdout (or omit --output when stdout is already redirected)
binlogviz analyze mysql-bin.000123 --format html --output -
```

The HTML report includes interactive charts (rows/txns per minute, top tables, operation mix), optional pattern drilldowns for high-signal write patterns, and a five-theme switcher.

## Find and Undo Bad Rows

### Find the bad DELETE and see its rows

```bash
binlogviz analyze mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  --start "2026-10-06 14:00:00" \
  --end "2026-10-06 14:10:00" \
  --show-rows
```

### Find an accidental DROP

```bash
binlogviz analyze mysql-bin.000123
```

The report's DDL Timeline lists each `DROP TABLE`, `TRUNCATE`, and `ALTER` with its GTID and start position, to use as the stop GTID or `--stop-position` of a point-in-time restore. See [Find an accidental DROP](docs/recipe/flashback.md#find-an-accidental-drop).

### Undo the bad rows

Find the transaction with `analyze`, the DDL Timeline, or `--show-rows`. Then print SQL that reverses only those row changes. Review the script. Apply it yourself. Flashback does not connect to a database.

```bash
binlogviz analyze mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  --show-rows

binlogviz flashback mysql-bin.000123 \
  --include-table shop.orders \
  --dml delete \
  > flashback.sql

binlogviz flashback verify flashback.sql | mysql --default-character-set=utf8mb4
```

`--show-rows` limits, the DDL Timeline stop points, flashback requirements, refusals, conflicts, apply grants, and commit markers are in [Find and Undo Bad Rows](docs/recipe/flashback.md).

## Snapshots, Compare, Trend, and Workflow

### Save two incident windows as named snapshots, then compare them

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z" \
  --workload-id orders-production \
  --format json \
  --snapshot-name incident_current

binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-08T10:00:00Z" \
  --end "2026-03-08T10:30:00Z" \
  --workload-id orders-production \
  --format json \
  --snapshot-name incident_baseline

binlogviz snapshot list
binlogviz snapshot list --format json
binlogviz snapshot show incident_current
binlogviz snapshot show incident_current --format json
binlogviz compare \
  --current-snapshot incident_current \
  --baseline-snapshot incident_baseline \
  --format html > compare.html

# rename or delete a snapshot after you are done with it
binlogviz snapshot rename incident_current incident_current_renamed
binlogviz snapshot delete incident_current_renamed
```

When `--snapshot-name` is set, `analyze --format json` still writes the JSON report to `stdout` and also saves the same payload under `~/.binlogviz/snapshots/<name>.json`. The save confirmation is printed to `stderr`.

### Review multiple snapshots as one ordered trend

```bash
binlogviz trend incident_week1 incident_week2 incident_week3 --format text

binlogviz trend --from-snapshots 'incident_week*' \
  --baseline-snapshot baseline_weekly \
  --format html > trend.html
```

### Run a multi-step investigation from one plan file

The repository ships a runnable `incident.yaml` that points at the 1500-byte ROW sample, so this works from the repository root:

```bash
binlogviz workflow run incident.yaml
tree artifacts/incident
```

Compare comparability, trend ordering, and the `workflow resume`, `status`, `clean`, `export`, `validate`, and `describe` lifecycle are in [Snapshots, Compare, Trend, and Workflow](docs/recipe/compare-trend-workflow.md).

## What BinlogViz Helps You See

BinlogViz is optimized for these common DBA questions:

- **Which tables are taking the heaviest write load?**
- **Which primary key was updated or deleted the most?**
- **Which UPDATE or DELETE rows hit a table with no primary key?**
- **How far behind was this replica, and which transactions caused it?** See [Why is my replica lagging?](docs/reference/analyze-behavior.md#why-is-my-replica-lagging).
- **Which transactions are large enough to deserve attention?**
- **Did a spike happen at a specific minute?**
- **What changed inside a known incident window?**
- **How does the current window differ from a trusted baseline report?**
- **Can I hand the result to another script or pipeline safely?**

## Language Support

BinlogViz supports multiple languages for runtime output such as errors, reports, and progress messages.

```bash
binlogviz --lang zh-CN analyze mysql-bin.000123
LANG=zh_CN.UTF-8 binlogviz analyze mysql-bin.000123
```

Supported languages:

- `en` - English (default)
- `zh-CN` - Simplified Chinese

`--help` output currently remains in English because help text is generated before language initialization. Runtime output is localized.

## Documentation by Task

### Start with these

- [Quickstart](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/recipe/quickstart.md)
- [Analyze Local Binlogs](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/recipe/analyze-local-binlogs.md)
- [Troubleshoot Common Errors](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/recipe/troubleshoot-common-errors.md)
- [Find and Undo Bad Rows](docs/recipe/flashback.md)
- [Snapshots, Compare, Trend, and Workflow](docs/recipe/compare-trend-workflow.md)

### When you need contracts and exact behavior

- [CLI Reference](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/cli.md)
- [Input Discovery Reference](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/input-discovery.md)
- [Output Format Reference](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/reference/output-format.md)
- [Analyze Behavior Details](docs/reference/analyze-behavior.md)

### When you need internals or operating model detail

- [Architecture](docs/concept/architecture.md)
- [DuckDB Temp Store](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/concept/duckdb-temp-store.md)
- [Analysis Model](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/concept/analysis-model.md)
- [Limitations](https://github.com/Fanduzi/BinlogVisualizer/blob/main/docs/concept/limitations.md)

### Release and supporting material

- [Examples](https://github.com/Fanduzi/BinlogVisualizer/tree/main/docs/examples)
- [Release Notes](https://github.com/Fanduzi/BinlogVisualizer/tree/main/docs/releases)
- [Changelog](https://github.com/Fanduzi/BinlogVisualizer/blob/main/CHANGELOG.md)
- [Security Policy](https://github.com/Fanduzi/BinlogVisualizer/blob/main/SECURITY.md)

## Architecture

The Cobra command layer streams normalized binlog events into the analyzer, then hands the result to report, snapshot, compare, trend, and workflow modules. The pipeline and the module table are in [Architecture](docs/concept/architecture.md).

## Requirements

- local MySQL `ROW` binlog files
- Go 1.26.10+ if building from source

## License

Apache 2.0
