# I18n Module

Embedded English and Simplified Chinese messages for CLI and report presentation.

## Members

| File | Responsibility |
|------|----------------|
| `i18n.go` | Loads embedded locale files and exposes concurrency-safe translation and language-selection helpers. |
| `locales/en.json` | Defines English CLI, error, and report messages, including UTC timestamp semantics and `--prefix` exact-filename hints. |
| `locales/zh-CN.json` | Defines Simplified Chinese equivalents for the same message contract. |
| `i18n_test.go` | Verifies initialization, locale detection, translation, and runtime language switching. |

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
