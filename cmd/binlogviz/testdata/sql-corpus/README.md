# Analyze SQL Corpus

This corpus validates DBA-facing analyze report behavior. Each scenario is deterministic and targets one report contract:

| Scenario | Contract |
|---|---|
| `baseline-small` | normal workload with no high-signal anomaly |
| `tps-spike` | TPS timeline and hot interval detection |
| `rows-spike` | rows/minute spike without necessarily high TPS |
| `ddl-incident` | DDL Timeline and DDL series |
| `large-transaction` | Largest by Rows ranking |
| `long-transaction` | Longest by Duration ranking |
| `wide-transaction` | Widest by Touched Tables ranking |
| `multi-file-window` | file metadata coverage for discovery-mode-adjacent scenarios |
