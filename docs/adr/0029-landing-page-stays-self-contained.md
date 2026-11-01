# ADR 0029: The landing page stays one self-contained file, with no image asset

Status: accepted (2026-11-01). A docs review called the landing page's missing screenshot a defect,
because the product's main artifact is a visual HTML report. The page is deliberately text-only, so
that review will recur until the reason is written down.

## Context

- **The constraint is a guard, not an accident.** `cmd/binlogviz/landing_page_test.go` holds the
  page to an allowlist: `landingFiles` is `index.html` alone, `landingSections`,
  `landingOtherIDs`, `landingHeadings`, `landingExternalLinks` and `landingMetas` are exhaustive,
  and `cssResource = (?i)\burl\s*\(|@import` fails on any stylesheet, font or image loaded from CSS.
  A relative `<img src>` is rejected twice: as an unlisted link (`landingExternalLinks`) and as a
  listed file that is not an HTML page (`landingFiles` is validated as pages).
- **The page already answers the "show me" need in text.** `#outputs` carries four switchable
  preview panes (text, JSON, Markdown, HTML) with a real report excerpt, and `#how` carries the
  pipeline. That is the design's substitute for a screenshot.
- **A bitmap is not free.** A 1440-wide PNG of the real report is 239 KiB. Inlined as a base64 data
  URI it is about 320 KiB, which takes `index.html` from 92 KiB to roughly 410 KiB against a
  104 KiB cap; shipped as a file it is a second request and a second cache entry.
- **Attempted and reverted.** A screenshot was generated from a real 233 MB report (41,208
  transactions, 2,755,870 rows, 224,066 events) with headless Chrome, added to `#outputs`, and the
  guard rejected it on the three rules above. The change was reverted rather than weakening the
  guard inside a documentation task.

## Decision

The landing page keeps one file, no stylesheet, no font and no image. A screenshot is not added
until someone decides one of the two changes below is worth its cost, and that decision belongs in
a commit that edits `landing_page_test.go` on purpose.

## Considered options

- **Inline the PNG as a data URI.** Keeps the page self-contained and offline-friendly, needs
  `landingPageMaxBytes` raised to about 512 KiB and leaves a base64 blob in a hand-edited file.
- **Add `landingAssets` to the guard.** One allowlist entry plus a resource rule; the page gains a
  second request and an `img` to keep in sync with the report design.
- **Link to a hosted sample report.** Moves the decision off the page and onto where a 1.2 MB HTML
  artifact is served; Cloudflare Pages publishes `docs/landing/` only, and GitHub shows HTML as
  source.

## Consequences

- A reader who wants to see the report opens `binlogviz analyze --format html` themselves, or the
  two screenshots in `README.md` (`docs/images/`), which are outside this guard.
- Any future proposal to add visual material to the landing page starts from this ADR, not from a
  review comment, and must name which of the two costs it accepts.
- `docs/README.md` lists the landing page as a text surface, so the docs tree does not promise an
  image the page cannot carry.
