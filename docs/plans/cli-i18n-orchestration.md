# Orchestration: internationalize workflow/snapshot/compare/trend command surface

Date: 2026-11-02. Post-v0.38.0 runtime-quality hardening unit.

## Evidence (orchestrator audit)

- `cmd.*` i18n namespace: 104 keys — analyze/flashback/redo fully translated (flashback.go 62 `i18n.T` calls, analyze.go 15).
- **Zero-i18n command family**: `cmd/binlogviz/workflow*.go`, `snapshot.go`, `compare.go`, `trend.go` — all `Short:`/`Long:` help, all `fmt.Fprintf` output, English literals.
- **Zero-i18n internal renderers**: `internal/compare/text.go` (RenderText, 151 lines), `internal/trend/text.go` — the actual report text users read.
- `internal/workflow/` is a pure data layer — no `fmt.Fprintf`; its only user-visible text is clierr-wrapped error strings (separate, smaller concern — NOT in this unit's scope).
- en.json/zh-CN.json currently 551 keys each, symmetric.

This violates the codebase's own established convention (user-facing CLI surface is translated). Rule 10 fix.

## Scope — in

- `cmd/binlogviz/workflow.go` — all Short/Long/Use help strings
- `cmd/binlogviz/workflow_output.go` — ~40 Fprintf status/describe/validate/clean output strings
- `cmd/binlogviz/workflow_exec.go` — literal progress/status output if any (clierr.Wrap calls pass through underlying errors — do NOT translate wrapped error text, they're internal)
- `cmd/binlogviz/snapshot.go` — confirmation strings + help
- `cmd/binlogviz/compare.go` + `internal/compare/text.go` — help + rendered report text
- `cmd/binlogviz/trend.go` + `internal/trend/text.go` — same

## Scope — out (explicit)

- clierr-wrapped underlying error messages (internal errors stay English, consistent with existing convention where `error.*` keys cover only some paths — do NOT create a new inconsistency; worker should flag if existing convention actually translates clierr messages)
- JSON/YAML output keys, file formats, plan field names
- `internal/workflow/` data layer (no printing anyway)
- Debug/internal strings

## Conventions

- Follow existing `cmd.*` key naming (e.g., `cmd.redo.*` precedent in en.json) — new `cmd.workflow.*`, `cmd.snapshot.*`, `cmd.compare.*`, `cmd.trend.*`; renderer strings under `report.text.*` only if they share the report surface, else `compare.*`/`trend.*` namespaces — worker picks the cleaner precedent after reading existing key layout.
- `i18n.T` / `i18n.Tf` usage patterns identical to analyze/flashback.
- en.json/zh-CN.json strictly symmetric; zh translations must be professional-grade (not machine-translated tone) — study existing zh-CN phrasing for analogous strings.

## Verification

- `go test -count=1 ./...`; i18n parity/symmetry tests if present
- `BINLOGVIZ_LANG=zh-CN` smoke on `workflow status`, `snapshot list`, `compare`, `trend` (fixture or synthetic inputs) — output actually Chinese
- gates: build/vet/gofmt -s/codehealth compare+lint/doc checker
- baseline.json should not need changes (string additions don't hit ceilings; if writeWorkflowStatusOutput complexity stays 41 fine)
