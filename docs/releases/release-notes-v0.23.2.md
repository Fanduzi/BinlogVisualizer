# BinlogViz v0.23.2 Release Notes

Release date: 2026-08-30

## Overview

v0.23.2 is a patch release for MariaDB operators. It restores analysis of binlogs containing prepared XA transactions, which previously failed before producing a report when the next GTID appeared.

## Bug Fixes

- **Physical MariaDB XA PREPARE events close their GTID group (#62)**: `XAPrepareLogEvent` now normalizes to the existing `XA_PREPARE` boundary, so the following legal GTID starts a new transaction instead of raising `conflicting GTID`.
- **Position and GTID selection work across prepared XA transactions**: selectors still consume the required prefix for transaction context, but no longer fail on the legal GTID after XA PREPARE.
- **Real in-transaction GTID conflicts still fail**: the provenance integrity guard is unchanged; this patch adds the missing physical XA boundary rather than weakening conflict detection.

## Breaking Changes

None.

## Compatibility

- CLI flags, exit codes, report schemas, artifact names, and supported platforms are unchanged from v0.23.1.
- Query-form XA statements and MariaDB's physical XA PREPARE log event are both supported.
