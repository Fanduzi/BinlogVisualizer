# BinlogViz vX.Y.Z Release Notes

Release date: YYYY-MM-DD

## Install or upgrade

```bash
brew upgrade --cask binlogviz          # macOS
# or rerun the installer: sh ./install.sh
```

Schemas and flag names that did not change: say so here, in one line.

## Highlights

- What a user can now do that they could not before. One line per change.
- Name the command and the flag; leave the rules to `docs/reference/`.

## Fixes

- What used to be wrong, and what is right now.

## Breaking changes

- Removed or renamed flags, changed defaults, JSON `report_version` bumps, and
  what an existing script has to change. Write `None.` when there are none.

## Known issues / out of scope

- What this version still does not do, so nobody files it as a bug.

## Compatibility

- `report_version`, `plan_version`, and schema versions; additive-only keys.

## Tests and CI

- The engineering record: what was proven and how. A reader who only uses the
  tool can stop above this heading.

## Verification

<!--
The zh-CN file mirrors this one, section for section, with the same order.
`make release-check VERSION=vX.Y.Z` fails when the install section is missing
(from v0.40.0 on), when a title names another version, or when the CHANGELOG
section is missing or empty.
-->
