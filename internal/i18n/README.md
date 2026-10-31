# I18n Module

Embedded English and Simplified Chinese messages for CLI and report presentation.

## Members

| File | Responsibility |
|------|----------------|
| `i18n.go` | Loads embedded locale files and exposes concurrency-safe translation and language-selection helpers. |
| `locales/en.json` | Defines English CLI, error, and report messages, including UTC timestamp semantics, local `YYYY-MM-DD HH:MM:SS` `--start`/`--end` help, `--prefix` exact-filename hints, `--dml` / `--show-rows` labels, flashback refusals, the flashback `--dml` help line, the unknown-definition warning, `--schema-file`, `--schema-file-db`, schema-file mismatch errors, the partial-transaction warning, Unclassified QUERY errors, open BEGIN / SAVEPOINT / Ignored-only group errors, Ignored QUERY diagnostic labels, and the `redo` command surface: description, examples, flag help, the statement-format refusal, and the cut-transaction trailer warning. The split-output surface (ADR 0025): `--output-dir`/`--file-per-table`/`--max-statements-per-file` flag help on both replay commands, the split validation errors (`splitNeedsOutputDir`, `splitOutputNeedsFlag`, `splitMaxInvalid`, `splitDirNotEmpty`, `splitDirNotDir`, `splitDirParent`, `redoFilePerTable`, `splitPlan`), the file-split transaction warnings, and the `splitWrote` progress line. The `workflow`/`snapshot`/`compare`/`trend` command surface: usage lines, flag help, confirmations, and operator output under `cmd.<command>.*`, workflow stderr progress under `progress.workflow*`, the `warning.workflowPlanNotIncluded` notice, authored `clierr` usage/error messages under `error.*`, and the compare/trend terminal renderers under `report.text.compare.*` / `report.text.trend.*`. |
| `locales/zh-CN.json` | Defines Simplified Chinese equivalents for the same message contract. |
| `i18n_test.go` | Verifies initialization, locale detection, translation, Unclassified QUERY / open BEGIN / Ignored-only catalog strings, and runtime language switching. |

## Interfaces

- `Init(lang string) error` initializes messages and selects a locale.
- `T(key string, templateData ...map[string]any) string` translates a message key.
- `Tf(key string, data map[string]any) string` translates a templated message key.
- `SetLanguage(lang string) bool` changes the active locale after initialization.

## Dependencies

- Upstream: embedded JSON locale files and `go-i18n`.
- Downstream: CLI commands and human report renderers.

## Update Rule

Keep English and Simplified Chinese message keys synchronized.
