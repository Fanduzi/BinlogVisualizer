# ADR 0020: Flashback stays, marked experimental, and checked against MySQL

Status: accepted (2026-10-27).

## Context

Flashback stays in the product. It is not a stable contract. The undo is built offline from row images, and generated columns are checked by `internal/mysqlexpr`, which models only the subset named in `docs/concept/limitations.md`. A wrong literal is what the operator applies.

Scripts already run `binlogviz flashback` and `binlogviz flashback verify` with the flags they have today. An opt-in flag would make those invocations fail until every script was edited. That is a break for current users, so this ADR does not add one.

`flashback --plan` is `plan_version` 2. ADR 0010 says adding a key is not a breaking change and does not bump the version. ADR 0017 publishes a closed schema (`additionalProperties: false`) generated from the Go struct, so the new key is added on the struct and in `docs/schema/flashback-plan.json` together.

The flashback text surfaces are the README flashback line, `docs/recipe/flashback.md`, the `flashback` section of `docs/reference/cli.md`, `docs/concept/limitations.md`, and `docs/agents/USING-BINLOGVIZ.md` (each with a zh-CN twin).

## Decision

1. **Experimental means the command is shipped, and the operator reviews the SQL.** It is not a second mode. Exit codes, selectors, the sealed script, and `plan_version` 2 stay. A literal or a refusal may still change when a differential run shows the checker disagreed with MySQL. That change is a fix. A disagreement is never left in place as an accepted difference.

2. **No opt-in flag.** The marker is visible instead, and existing invocations keep working:
   - Help for `flashback` and `flashback verify`, and for the flags those commands define, is prefixed with `[experimental]`. Inherited root flags (`--lang`, `--quiet`, `--tz`, `--error-format`) are not flashback flags and are not marked.
   - Every run of those two commands prints one stderr line, including under `--quiet`, on `--plan`, on a refusal, and on a usage error (bad flag, missing or extra args — the flag-error hook and `Args` reach the warning before `RunE` does). The line says the feature is experimental and that the generated SQL must be reviewed before it is applied. `binlogviz flashback --help` does not print it, because help is not a run. With `--error-format json` the warning is still that text line, and the error object is the next line. A reader that decodes stderr as one JSON value skips the warning line and decodes the object.
   - `--plan` adds `"experimental": true`. The key is additive, so `plan_version` stays 2. The schema is regenerated from the struct.

3. **Differential tests compare this process with MySQL.** They run only when `BINLOGVIZ_DIFFTEST=1` and `BINLOGVIZ_DIFFTEST_MYSQL` is the mysql client command. CI runs the same tests on MySQL 8.0 and 8.4. Those servers are Docker service containers. A service container cannot pass `mysqld` arguments, so the job restarts each image with `ROW`, `GTID`, `binlog_row_image=FULL`, and `binlog_row_metadata=FULL` before the tests. The corpus uses a fixed seed. `BINLOGVIZ_DIFFTEST_NIGHTLY=1` enlarges the generated part. Without the env the tests skip. The CI job fails if a test skips.

   - **`mysqlexpr`.** A fixed catalog plus the seeded corpus covers NULL, case and collation (`UPPER` / `LOWER`, charset introducers, a `COLLATE` clause), numeric and `DECIMAL` precision (`EvalGenExpr` and `EvalDecExprInc`, including `div_precision_increment`), temporal values, JSON extraction, string functions, and type coercion. The general domain is `SELECT <expr>` over one derived-table row, the same values the checker receives. The `DECIMAL` domain is the internal width a generated column keeps, which is not the scale of a bare `SELECT` (integer `/` integer displays at `div_precision_increment`, so `1/7` is `0.1429`, while `DECIMAL(65,9) AS (1/7)` stores `0.142857142`). That comparison creates `DECIMAL(65, scale)` generated as the expression, at the session `div_precision_increment`, and selects the column. A value the checker returns must match the server. `ok=false` ("not evaluated") is the explicit unsupported result: the test records it and does not call it a match. Signed `BIGINT` `+`, `-`, `*`, and `DIV` past the 64-bit range are unsupported, because MySQL rejects them (`ERROR 1690`) and the checker must not claim the mathematical result. A catalog entry that is a paired refusal requires both sides to refuse. A returned value that differs from the server, or a value returned for an expression the server rejects, fails the test. The resolution is a code change with a regression case that does not need a server, or a change that makes the checker return unsupported for that input.

   - **Planner and guard.** On each server the test creates a table, saves `CHECKSUM TABLE` and an ordered row dump, applies DML, builds the flashback script from the closed binlog, applies it through `flashback verify`, and requires the checksum and the row dump to match the baseline. Three rejections are required, and each leaves the table at the pre-apply rows: a `FLOAT` image is refused and no script is written; a later change of a selected key is a conflict and no script is written; a schema-file guard whose target column is not generated rolls the apply back.

## Consequences

- Current scripts keep the same flags and the same exit codes. They see one new stderr line, and `--plan` gains one JSON key.
- A consumer that rejects unknown JSON keys takes the updated schema. That is the ADR 0010 rule for an added key.
- The notice is one line on each README and one sentence on each flashback page (`docs/recipe/flashback`, `docs/reference/cli`, `docs/concept/limitations`, `docs/agents/USING-BINLOGVIZ`, both languages).
- The CI job is named `flashback difftest`. It is not `continue-on-error`. The release gate still waits on the existing flashback end-to-end job. This job is the pull-request check for the checker, the guard, and the planner.
