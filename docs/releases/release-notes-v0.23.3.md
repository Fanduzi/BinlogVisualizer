# BinlogViz v0.23.3 Release Notes

Release date: 2026-08-30

## Overview

v0.23.3 restores the MariaDB XA recovery evidence needed during incident response. Prepared transactions retain their XA identity, and a zero-row `XA COMMIT` remains visible and selectable under its own GTID.

## Bug Fixes

- **XA PREPARE retains its XA identity (#63)**: physical MariaDB `XAPrepareLogEvent` payloads now populate `xa_xid` on the prepared transaction.
- **Zero-row XA COMMIT remains a transaction**: the commit GTID and `xa_xid` are retained even when the transaction changes no rows.
- **GTID and position selectors can target XA COMMIT**: `--include-gtids` and commit-position windows return the matching zero-row transaction instead of reporting no events.

## Breaking Changes

None.

## Compatibility

- CLI flags, exit codes, report schemas, artifact names, and supported platforms are unchanged from v0.23.2.
- Normal zero-row GTID groups remain omitted; retention is limited to XA transactions with physical in-window evidence.
