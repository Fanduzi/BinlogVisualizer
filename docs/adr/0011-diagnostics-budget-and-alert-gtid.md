# ADR 0011: An agent can shrink analyze JSON, and a transaction alert names its GTID

Status: accepted (2026-10-19). Tracks #282 items 2 and 3. #282 item 4 and #270 item 4 are already done; this ADR does not reopen them.

## Context

`analyze --format json` on a short window is too large for an agent context window, and the part that will not shrink is `diagnostics`. On the fixtures in this repo the object is 38–46% of the document. Almost all of that is four copies of the same transactions: `largest_transactions`, `longest_transactions`, `widest_transactions`, and `largest_byte_transactions`. `--top` and `--top-transactions` already cap the top-level `transactions` array. They do not cap these copies. #282 measured the same shape on a 3-minute window of a 427 KB binlog: about 45 KB of JSON, of which `diagnostics` was about 15 KB.

An alert that is about a transaction names it as `txn-207` in `message` and `txn_key`. `flashback --include-gtids` takes a GTID. The GTID is already on `transactions[].gtid`, so the agent has to join before it can call flashback. A spike, an input-format warning, and a no-primary-key warning are not about one transaction.

`flashback --plan` warnings were checked on this tip (`61414d6`). `plan_version` is 2. Each warning is `{"code","message"}`. The code is the same in en and zh-CN, and the message has no localized `warning:` prefix. Stderr still prints that prefix. That is #296. Nothing here changes it.

## Decision

1. **`--diagnostics full|summary|off` on `analyze`.** Default `full`. An empty value is `full`. Anything else is a usage error (exit 3): `invalid --diagnostics %q (allowed: full, summary, off)`.

   The flag changes only the JSON document (`--format json`, and a snapshot saved from that document). Text, Markdown, and HTML ignore it and stay byte-identical.

   | Mode | `diagnostics` |
   |---|---|
   | `full` | Today's object. Byte-identical. |
   | `summary` | Keeps what is not already at the top level: `file_coverage`, `counted_event_bytes`, `ddl_events`, `open_dml_groups`, `duration_buckets`, `input_format_guess`, and the ignored-query / open-group / unmapped counts. The four transaction rankings, `file_segments`, `hot_intervals`, and `findings` are empty arrays (the omitempty rankings are omitted). Those lists are already `transactions`, `timeseries`, `minutes`, and `alerts`. |
   | `off` | The key is omitted. |

   `report_version` stays 3. `full` removes nothing. `summary` and `off` are opt-in, so a consumer that did not pass the flag still sees the v3 object. A snapshot saved under `summary` or `off` is that thinner document. Compare and trend do not require `diagnostics`; a missing or emptied list compares as empty.

2. **`alerts[].gtid`.** When the alert has a `txn_key` and that transaction has a GTID, the alert carries the same GTID string `transactions[].gtid` and `diagnostics.open_dml_groups[].gtid` already use (MySQL `uuid:seq` or MariaDB `domain-server-seq`). An empty identity, including an anonymous GTID, omits the key. `message` and `txn_key` stay, so text, Markdown, and HTML stay byte-identical. The key is additive, so `report_version` stays 3 (ADR 0010: adding a key is not a bump). Default JSON is therefore not byte-identical only for an alert that has a GTID: one extra key. Alerts with no GTID are unchanged.

3. **Not in scope.** `plan_version` stays 2. Warning codes stay as #296 shipped them. `schema_version` is unchanged. No workflow or release file changes.

## Consequences

- An agent passes `--diagnostics summary` or `off` and still has `alerts`, `transactions`, and the coverage counts. With `summary`, the DDL timeline and open uncommitted groups remain, because they are not copied elsewhere.
- An agent reads `alerts[].gtid` and passes it to `flashback --include-gtids` without joining on `txn_key`.
- en and zh-CN: the flag help is translated. JSON keys, `gtid`, and warning codes are not. Alert and warning messages stay translated, as they are today.
