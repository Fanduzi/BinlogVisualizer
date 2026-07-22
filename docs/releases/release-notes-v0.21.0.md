# BinlogViz v0.21.0 Release Notes

Release date: 2026-07-22

## Changes

- **Default HTML output writes to a file instead of stdout**: `binlogviz analyze --format html` now writes the HTML report to a file in the current directory rather than flooding stdout. For a single input file, the output filename is derived from the input (e.g., `mysql-bin.000123.html`); for multiple inputs or discovery mode, it defaults to `binlogviz-report.html`. If the filename already exists, a numeric suffix is appended automatically (e.g., `report.1.html`). The save confirmation is printed to `stderr`. Use `--output -` to restore the previous stdout behavior for scripts that intentionally pipe HTML.

- **New `--output` flag for explicit HTML destination**: The `--output` (short: `-o`) flag lets you specify a custom output path for HTML reports. Only supported with `--format html`; using it with other formats returns an error.

- **Text Top Tables now show INSERT/UPDATE/DELETE/DDL breakdown**: The text report's Top Tables section replaces the single `Rows` column with separate `INSERT`, `UPDATE`, `DELETE`, and `DDL Events` columns, each showing the count with an inline percentage. INSERT/UPDATE/DELETE percentages are of affected rows; DDL percentage is of binlog events. A footnote explains the denominators.

- **HTML Top Tables now show operation breakdown with percentages**: The HTML report's Top Tables section replaces the INSERT/UPDATE/DELETE raw-count columns with percentage-enriched columns and adds a DDL column. A footnote explains the percentage denominators.

## Bug Fixes

None.

## Breaking Changes

- **HTML default output changed from stdout to file**: Scripts that previously relied on `binlogviz analyze --format html > report.html` will now create an empty redirect file and the actual report as a sidecar file. To restore the old behavior, use `--output -` explicitly: `binlogviz analyze --format html --output - > report.html`. The `compare` and `trend` commands are unaffected and still write HTML to stdout.
