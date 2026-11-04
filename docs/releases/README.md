# Release notes

One file per tagged version, in English and Chinese:

```
release-notes-vX.Y.Z.md
release-notes-vX.Y.Z.zh-CN.md
```

The GitHub Release body is these two files joined under `## English` and `## 中文`, so the
release page and the repository show the same text. `CHANGELOG.md` in the repository root has
one short section per version, newest first, and links to these files.

## Recently released

| Version | Released |
|---|---|
| [v0.43.0](release-notes-v0.43.0.md) | 2026-11-04 |
| [v0.42.0](release-notes-v0.42.0.md) | 2026-11-04 |
| [v0.41.0](release-notes-v0.41.0.md) | 2026-11-03 |
| [v0.40.0](release-notes-v0.40.0.md) | 2026-11-02 |
| [v0.39.0](release-notes-v0.39.0.md) | 2026-11-01 |
| [v0.38.0](release-notes-v0.38.0.md) | 2026-10-31 |
| [v0.37.0](release-notes-v0.37.0.md) | 2026-10-30 |
| [v0.36.0](release-notes-v0.36.0.md) | 2026-10-29 |
| [v0.35.2](release-notes-v0.35.2.md) | 2026-10-27 |
| [v0.35.1](release-notes-v0.35.1.md) | 2026-10-26 |
| [v0.35.0](release-notes-v0.35.0.md) | 2026-10-21 |
| [v0.34.1](release-notes-v0.34.1.md) | 2026-10-20 |

Every other version is listed on [GitHub Releases](https://github.com/Fanduzi/BinlogVisualizer/releases),
which also carries the published date and the archives.

Versions v0.2.1 through v0.4.0 predate this convention and have no note file; their entries in
`CHANGELOG.md` are the only record.

## What a note contains

| Section | Written for |
|---|---|
| `## Highlights` / `## Added` | What a user can now do that they could not before |
| `## Fixes` | What used to be wrong and is right now |
| `## Compatibility` | Flags, JSON report versions, and anything that changes for an existing script |
| `## Known issues / out of scope` | What this version still does not do, so nobody files it as a bug |
| `## Tests and CI`, `## Verification`, `## Architecture` | The engineering record of how it was proven; skip it if you use the tool |

A note states no install command and no download link: the README install lines resolve
`releases/latest`, so they never go stale.

## Adding a note

Start from [TEMPLATE.md](TEMPLATE.md). From v0.40.0 on, both files must carry a
`## Install or upgrade` section before the engineering detail, because the GitHub release body
is these notes and the first thing a reader needs is the upgrade line. The section order in
the template is the order the release page should read in.

Both files, the `CHANGELOG.md` section, and the table above belong in the release-preparation
commit. `make release-check VERSION=vX.Y.Z` (and the `release-prep` job in
`.github/workflows/release.yml`) fails the release when a surface is missing, names a
different version, or (from v0.40.0 on) has no install section.
