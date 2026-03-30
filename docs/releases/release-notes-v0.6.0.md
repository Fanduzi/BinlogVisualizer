# BinlogViz v0.6.0 Release Notes

Release date: 2026-03-30

## Overview

v0.6.0 adds Markdown and HTML output formats to the `analyze` command. The HTML report includes interactive ECharts charts and a built-in 5-theme switcher. Theme preference is persisted in `localStorage`.

## New Features

### Markdown Output (`--format markdown`)

The `analyze` command now supports `--format markdown` (alias: `md`), which renders a GitHub-flavored Markdown report with tables for workload summary, top tables, top transactions, per-minute activity, and alerts.

### HTML Output (`--format html`)

The `analyze` command now supports `--format html`, which renders a self-contained single-file HTML report with:

- Summary stat cards (transactions, rows, events, time range)
- Interactive line chart: rows and transactions per minute
- Interactive bar chart: top tables by rows
- Interactive donut chart: INSERT / UPDATE / DELETE operation mix
- Top tables detail table with per-operation row counts
- Alert list with severity badges

All charts are rendered client-side using ECharts (bundled inline). No external dependencies or internet connection required to view the report.

### 5-Theme Switcher

The HTML report includes a theme switcher in the header (five colored dots). Available themes:

| Theme | Style |
|---|---|
| Nebula (default) | Dark, indigo + cyan |
| Forest | Dark, emerald + amber |
| Navy | Dark, sky blue + gold |
| Ember | Dark, orange + rose |
| Light | Light, clean report style |

Theme preference is saved in `localStorage` and restored on next open.

## Bug Fixes

None.

## Breaking Changes

None. Existing `--format text` and `--format json` behavior is unchanged.
