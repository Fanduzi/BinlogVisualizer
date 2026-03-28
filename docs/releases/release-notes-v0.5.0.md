# v0.5.0 Release Notes

Release date: 2026-03-28

## Overview

v0.5.0 exposes previously hidden analyzer controls as CLI flags and adds schema/table filtering at the analysis stage.

## New Features

### Exposed CLI Flags (Feature A)

Several analyzer parameters were previously hardcoded or unavailable via the CLI. They are now fully configurable:

| Flag | Default | Description |
|------|---------|-------------|
| `--top-minutes` | `60` | Number of busiest minutes to show in the report |
| `--spike-window` | `5` | Rolling baseline window (minutes) for spike detection |
| `--spike-factor` | `3.0` | Multiplier above baseline that triggers a spike alert |
| `--spike-min-rows` | `100` | Minimum row count for a minute to be considered for spike detection |

Example:

```bash
binlogviz analyze mysql-bin.000001 \
  --detect-spikes \
  --spike-window 10 \
  --spike-factor 5.0 \
  --top-minutes 30
```

### Schema/Table Filtering (Feature B)

Filter which schemas and tables are included in analysis at ingestion time — not just in output. This reduces noise and improves performance when only specific schemas or tables are relevant.

| Flag | Description |
|------|-------------|
| `--include-schema` | Only analyze the listed schemas (comma-separated) |
| `--exclude-schema` | Skip the listed schemas (comma-separated) |
| `--include-table` | Only analyze the listed tables (comma-separated) |
| `--exclude-table` | Skip the listed tables (comma-separated) |

Exclude rules take priority over include rules. Table names are matched without schema prefix.

Example:

```bash
# Only analyze the orders schema, skip the audit table
binlogviz analyze mysql-bin.000001 \
  --include-schema orders \
  --exclude-table audit_log

# Skip internal schemas entirely
binlogviz analyze mysql-bin.000001 \
  --exclude-schema mysql,sys,information_schema,performance_schema
```

## Bug Fixes

None.

## Breaking Changes

None. All new flags have defaults that preserve existing behavior.
