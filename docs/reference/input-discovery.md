# Input Discovery Reference

This document defines how BinlogViz resolves binlog files when you use discovery mode.

## Discovery Mode Contract

Use discovery mode with both flags together:

```bash
binlogviz analyze --from-dir DIR --prefix PREFIX
```

Requirements:

- `--from-dir` supplies the directory to scan.
- `--prefix` supplies the filename prefix that matching files must start with.
- Both flags are required together.
- Discovery mode cannot be combined with positional binlog file arguments.

If you already know the exact files you want, use positional file mode instead.

## Matching Rules

BinlogViz scans only the immediate entries under `DIR` and applies these rules to each entry:

1. Directories are excluded.
2. Symlinks are excluded.
3. The filename must start with `PREFIX`.
4. The suffix after `PREFIX` must be non-empty.
5. The suffix after `PREFIX` must be digits, optionally with a single `.` immediately after `PREFIX`.

That means `--prefix mysql-bin` and `--prefix mysql-bin.` both match `mysql-bin.000008` (the form produced by `log_bin_basename`).

Discovery accepts names such as:

- `mysql-bin.000123`
- `mysql-bin.000124`

And excludes names such as:

- `mysql-bin.` because the suffix is empty
- `mysql-bin.index` because the suffix is not numeric
- `mysql-bin.000123.tmp` because the suffix is not digits only
- `relay-bin.000123` because the prefix does not match

## Numeric Suffix Matching

The portion after the prefix is parsed as a base-10 integer.

For example, with `--prefix mysql-bin.`:

- `mysql-bin.9` matches with suffix `9`
- `mysql-bin.010` matches with suffix `010`
- `mysql-bin.000123` matches with suffix `000123`

The numeric-only rule is what makes discovery suitable for ordered MySQL-style binlog sequences.

## Ordering Rules

After matching, BinlogViz sorts the files before analysis.

Primary rule:

- Files are ordered by the parsed numeric suffix in ascending order.

This avoids lexical ordering mistakes such as placing `mysql-bin.10` before `mysql-bin.2`.

Tie-break rule:

- If two matched filenames produce the same parsed numeric value, BinlogViz falls back to filename lexical order.

The resolved paths are then passed to the analysis pipeline in that final order.

## Resolved File Reporting

When discovery succeeds, BinlogViz prints the ordered file list to `stderr` before parsing starts:

```text
Resolved binlog files:
- /var/lib/mysql/mysql-bin.000123
- /var/lib/mysql/mysql-bin.000124
```

This helps operators verify exactly which files will be analyzed while keeping `stdout` available for text or JSON report output.

## Invalid Usage

BinlogViz rejects these discovery-related input combinations:

### Mixed input modes

You cannot combine positional files with discovery flags.

```bash
binlogviz analyze mysql-bin.000123 --from-dir /var/lib/mysql --prefix mysql-bin.
```

This fails with:

```text
cannot combine positional binlog files with --from-dir/--prefix
```

### Incomplete discovery mode

You cannot provide only one of the two discovery flags.

```bash
binlogviz analyze --from-dir /var/lib/mysql
binlogviz analyze --prefix mysql-bin.
```

These fail with:

```text
--from-dir and --prefix must be provided together
```

### No matches

If the directory can be read but no entries satisfy the matching rules, the command fails.

Representative error:

```text
no matching binlog files found under /var/lib/mysql with prefix "mysql-bin."
```

## Operator Guidance

Use discovery mode when:

- the binlog files are stored together in one directory
- the sequence follows a stable numeric naming scheme
- you want BinlogViz to determine the ordered input list for you

Use positional file mode when:

- you need a hand-picked subset
- the files do not share one numeric-prefix contract
- you want shell expansion or explicit per-file control
