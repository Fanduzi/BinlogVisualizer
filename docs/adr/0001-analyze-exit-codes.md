# Analyze exit codes distinguish parse failure from no-data

Analyze used to exit 0 whenever the parser returned, including empty time windows and Format-Description-only files. Operators treated that as success. Exit codes are now: **0** counted at least one event (ROW, or MIXED that has images); **1** could not analyze (corrupt, no Format Description, STATEMENT with zero images); **2** parsed a complete binlog but counted zero events (window miss or Format Description only). Scripts can tell “bad file” from “nothing in range” without scraping stderr.

## Considered Options

- Exit 0 plus a `NO DATA` banner: `&&` still treats it as success.
- Exit 1 for every non-success: a wider window and a truncated file look the same.

## Consequences

- Tests that expected exit 0 on a window miss must change.
- Non-zero analyze exits write nothing to stdout (same contract as STATEMENT failure).
