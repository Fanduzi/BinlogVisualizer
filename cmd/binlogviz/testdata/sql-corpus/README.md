# Analyze SQL Corpus

This corpus validates DBA-facing analyze report behavior. Each scenario is deterministic and targets one report contract:

| Scenario | Contract |
|---|---|
| `baseline-small` | normal workload with no high-signal anomaly |
| `tps-spike` | TPS timeline and hot interval detection |
| `rows-spike` | rows/minute spike without necessarily high TPS |
| `ddl-incident` | DDL Timeline and DDL series |
| `event-mix-burst` | INSERT/UPDATE/DELETE minute bursts plus hot-table skew |
| `incident-mixed` | combined DDL, large, long, and wide transaction evidence with multi-file spans |
| `large-transaction` | Largest by Rows ranking |
| `long-transaction` | Longest by Duration ranking |
| `wide-transaction` | Widest by Touched Tables ranking |
| `multi-file-window` | file metadata coverage for discovery-mode-adjacent scenarios |

Corpus scenarios intentionally model MySQL 5.7/8.0 ROW-style evidence: row events carry table and row counts, `ROWS_QUERY` carries bounded SQL context and DDL text, and selected scenarios include file positions/bytes so transaction evidence has realistic location metadata. Event types use the parser's canonical kinds (`QUERY`, `WRITE_ROWS`, `UPDATE_ROWS`, `DELETE_ROWS`, `ROWS_QUERY`, `XID`).
