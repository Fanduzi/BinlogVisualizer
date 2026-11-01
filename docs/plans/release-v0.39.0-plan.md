# v0.39.0 Release Plan

## Scope

Publish the reviewed candidate plus the changes after v0.38.0. The user
approved v0.39.0, candidate commits/push/PR, independent database CI,
merge, tag publication, and the existing Homebrew update/temp-ref lifecycle.
Do not rewrite or delete existing tags, branches, worktrees, or local databases.

## Baseline And Version

- Source baseline: f637c308b89c1c449cd5db77e1cc350792b0f963.
- Previous release: v0.38.0 at 8842cd2b698c95cc1eaab1c077a3f95e2198c00c.
- New human-readable CLI localization is a feature, so use v0.39.0.
- The remote release/tag names were unoccupied during preflight.
- Preserve the source version default `dev`; official builds inject version
  and source identity through the existing linker flags.

## Minimal Verification Path

The existing CI selectors reach 27 of the 28 locally skipped database tests.
`TestFlashbackMySQL57DeprecatedMode` requires MySQL 5.7 and needs its own job.
Use the official Linux amd64 mysql:5.7.44 image pinned to its verified digest:
`sha256:dab0a802b44617303694fb17d166501de279c3031ddeb28c56ecf7fcab5ef0da`.
Use `docker exec -i` with the Actions service-container ID, not a host MySQL.
Health checks must require a working TCP query after server initialization.
Run only the exact compatibility test, reject SKIP, and require its root PASS.
Keep the existing MySQL 8.0/8.4, guard, race, lint, and native-platform gates.
Add a static regression guard for the new unsilenceable compatibility job.

## Release Materials

- Write English and zh-CN release notes plus a nonempty CHANGELOG section.
- Describe CLI localization, the two GTID fixes, output/error fixes, and
  regression/fuzz coverage; do not invent benchmarks or validation results.
- Preserve dynamic-latest installation and version-free landing pages.
- Update impacted module member/CI documentation only.
- Run the existing release-prep gate and its self-test.

## Delivery

1. Commit and push the isolated candidate, then create a PR.
2. Require all normal checks and the new 5.7 job; collect applicable PASS
   evidence for each of the original 28 skipped database tests.
3. Use the repository's rebase-merge policy, then fast-forward local main.
4. Validate main CI and the documented manual release-workflow run, including
   four native/manylinux builds and the Homebrew credential probe.
5. Create and push the new annotated tag only after all prerequisites pass.
6. Verify the public release, four archives/checksums, runtime version/commit,
   Homebrew cask, and public landing page in a separate read-only phase.
7. Commit factual delivery evidence without moving the released tag.

Historical local/remote tag disagreements remain untouched. The relevant
v0.38.0 baseline matches; no all-tags force fetch or retagging is permitted.
