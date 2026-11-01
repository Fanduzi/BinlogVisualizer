# Engineering records

This folder is the project's own paper trail. It is not documentation for people who use
BinlogViz — those pages live one level up, indexed in [docs/README.md](../README.md).

| Folder or file | What it holds |
|---|---|
| `adr/` | Architecture decision records: one decision, its context, and the alternatives rejected |
| `development/` | Coding standards and the size/complexity ratchet |
| `plans/` | Tracked implementation plans and release evidence for shipped versions |
| `superpowers/` | Design specs and long-form plans for the report redesign |
| `agents/` | Delivery records, issue-tracker and triage conventions, the domain glossary |
| `production-readiness.md` | The production checklist and which items are met |
| `large-input-baseline.md` | The time and memory gate for large inputs, and how it is measured |
| `gtid-filter-streaming.md` | Why GTID filtering is decided at the group's first event |
| `CODE_SMELLS.md` | Known structural debt, with the reason it is still there |
| `architecture-review-2026-09-06.html` | A dated review snapshot |

Two rules keep this folder honest:

- A record is kept as written. Paths and version numbers inside a dated plan or delivery
  record describe the tree at that time; they can be stale and are not maintained.
- A live document (a checklist, a baseline, a standard) is updated with the code that
  changes it, the same as the reference pages.
