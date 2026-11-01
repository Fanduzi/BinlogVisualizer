# BinlogViz v0.39.0 Release Notes

Release date: 2026-11-01

## Highlights

- Human-readable workflow, snapshot, compare, and trend command messages, including compare and trend text reports, support English and Simplified Chinese.
- Remote MySQL GTID handling preserves disjoint intervals when canonical text ends in a singleton. A disappearing MariaDB GTID entry is rejected before the replication connection.

## Fixes

- Quiet and JSON-error modes suppress ordinary workflow, snapshot, and trend progress while preserving warnings and errors.
- `version` and root `-v` propagate output writer failures. Cleanup errors do not replace an earlier operation error.

## Tests and CI

- Regression tests cover SQL emission, credential redaction, JSON Schema validation, and streaming generated-column review boundaries. Six fuzz targets span parser, remote, expression, index, and DDL behavior, with their regressions retained.
- Lint findings were reduced without disabling rules or loosening the baseline.
- A dedicated CI job is configured to run `TestFlashbackMySQL57DeprecatedMode` on the pinned MySQL 5.7.44 service container, rejecting skips and requiring the test's root PASS.

## Compatibility

- No CLI flags were added and JSON/schema versions are unchanged.
