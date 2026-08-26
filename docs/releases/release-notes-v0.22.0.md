# BinlogViz v0.22.0 Release Notes

Release date: 2026-08-26

## Highlights

- **Complete HTML UI Redesign**: Modern dark-mode aesthetic with enhanced typography, glassmorphism cards, glowing status pills, and responsive layout across Analyze, Compare, and Trend reports.
- **Physical Binlog Volume (Binlog Bytes) Metrics**: Top Tables now displays a dedicated, sortable `BINLOG BYTES` column (e.g. `36.2 MB`, `850 KB`), allowing instant identification of write amplification and high-volume table churn. Transaction diagnostic cards and hot interval badges now include formatted physical byte metrics.
- **Synchronized Multi-Chart Linkage**: Crosshairs, tooltips, and time zoom synchronize across related timeline charts in real time (`AVG TPS/MIN` and `ROWS PER MINUTE`, table activity and operation breakdown drilldowns, and multi-snapshot trend trajectories).
- **Interactive Mouse Selection Range Zoom**: Every timeline chart now includes a sleek DataZoom Slider bar for intuitive range selection and panning, plus top-right Toolbox controls for direct mouse area selection zoom and one-click restoration.
- **Rich Interactive Tools & Polish**: Floating back-to-top button, quick theme switcher, one-click `mysqlbinlog` command copy buttons, optimized card padding, and robust chart legends.

## Changes

- `internal/report`: Integrated physical byte metrics into table rows, diagnostic cards, and hot intervals; added `makeToolbox()`, `makeDataZoom()`, and chart synchronization via `echarts.connect`.
- `internal/compare`: Enhanced Key Findings and Recommended Checks cards with glassmorphism styling; added toolbox selection and range zoom to compare charts; added physical volume metrics to hot intervals.
- `internal/trend`: Added full multi-chart synchronization across snapshot trends; added interactive metric toggle between `Share of Rows` and `Rows` with selection zoom.
- `internal/i18n`: Added localized translations for `Binlog Bytes` and chart interaction tooltips in English and Simplified Chinese.

## Compatibility

- 100% backward-compatible with existing CLI arguments, JSON outputs, snapshots, and workflows.
