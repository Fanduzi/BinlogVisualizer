#!/usr/bin/env bash
# input: scripts/check-release-prep.sh from this repo
# output: exit 1 unless the release-prep check passes a complete fake release and fails each incomplete one
# pos: self-test for the release-prep gate; CI runs it on every PR so the gate cannot rot between releases
# note: if this file changes, update this header and scripts/README.md.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CHECK="$ROOT_DIR/scripts/check-release-prep.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

notes_en() { echo "docs/releases/release-notes-v$1.md"; }
notes_zh() { echo "docs/releases/release-notes-v$1.zh-CN.md"; }
write_notes() { # version title-version
  printf '# BinlogViz v%s Release Notes\n' "$2" > "$(notes_en "$1")"
  printf '# BinlogViz v%s 发布说明\n' "$2" > "$(notes_zh "$1")"
}
write_changelog() { printf '# Changelog\n\n## [Unreleased]\n\n## v%s\n' "$1" > CHANGELOG.md; }
# The README installs releases/latest; a pinned tag only appears when $2 is set.
write_readme() {
  if [[ -n "${2:-}" ]]; then
    printf '# BinlogViz\n\ncurl -fsSLO https://github.com/Fanduzi/BinlogVisualizer/releases/download/v%s/x.tar.gz\n' "$2" > "$1"
  else
    printf '# BinlogViz\n\ncurl -fsSLI https://github.com/Fanduzi/BinlogVisualizer/releases/latest\n' > "$1"
  fi
}
write_release() {
  write_notes "$1" "$1"; write_changelog "$1"
  write_readme README.md; write_readme README_ZH.md
}
extra_cases() {
  reset_complete; write_readme README.md 1.1.0
  expect pass "a README pin that names this release passes" "README.md install pin is 1.1.0"
  reset_complete; printf 'sh install.sh --version v1.0.0\n' >> README_ZH.md
  expect 1 "a stale install.sh --version pin fails" "README_ZH.md pins 1.0.0"
}

git_q() { git -c user.name=fixture -c user.email=fixture@example.invalid -c commit.gpgsign=false -c tag.gpgsign=false "$@" >/dev/null; }

repo="$work/repo"
mkdir -p "$repo/scripts" "$repo/docs/releases" "$repo/docs/landing"
cp "$CHECK" "$repo/scripts/check-release-prep.sh"
cd "$repo"
git_q init -q
write_release 1.0.0
echo '<p>Get v1.0.0</p>' > docs/landing/index.html
git_q add -A && git_q commit -qm v1.0.0 && git_q tag v1.0.0
git_q tag base

failures=0
expect() {
  local want="$1" label="$2" pattern="$3"; shift 3
  [[ $# -gt 0 ]] || set -- v1.1.0
  local out rc=0
  out="$(./scripts/check-release-prep.sh "$@" 2>&1)" || rc=$?
  if [[ "$want" == pass && "$rc" -ne 0 ]]; then
    echo "FAIL $label: expected pass, got exit $rc"; echo "$out"; failures=$((failures + 1)); return
  fi
  if [[ "$want" != pass && "$rc" -ne "$want" ]]; then
    echo "FAIL $label: expected exit $want, got $rc"; echo "$out"; failures=$((failures + 1)); return
  fi
  if [[ -n "$pattern" ]] && ! grep -Fq -- "$pattern" <<<"$out"; then
    echo "FAIL $label: output lacks '$pattern'"; echo "$out"; failures=$((failures + 1)); return
  fi
  echo "ok   $label"
}

# Start each case from a complete v1.1.0 working tree on top of v1.0.0.
reset_complete() {
  git checkout -q -f --detach base && git clean -qfd
  write_release 1.1.0
  echo '<p>Get v1.1.0</p>' > docs/landing/index.html
}

reset_complete
expect pass "complete release passes from the working tree" "all release surfaces ready for v1.1.0"

reset_complete; rm "$(notes_en 1.1.0)"
expect 1 "missing English notes fail" "release notes missing"
reset_complete; rm "$(notes_zh 1.1.0)"
expect 1 "missing Chinese notes fail" "release notes missing"
reset_complete; write_notes 1.1.0 1.0.0
expect 1 "notes titled for another version fail" "do not open with a title"
reset_complete; write_changelog 1.0.0
expect 1 "CHANGELOG without the section fails" "has no '## v1.1.0' section"
reset_complete; write_readme README.md 1.0.0
expect 1 "stale README pin fails" "README.md pins 1.0.0"
reset_complete; write_readme README_ZH.md 1.0.0
expect 1 "stale README_ZH pin fails" "README_ZH.md pins 1.0.0"
extra_cases
reset_complete; echo '<p>Get v1.0.0</p>' > docs/landing/index.html
expect 1 "landing without the version fails" "does not name v1.1.0 (newest release it shows: v1.0.0)"
reset_complete; echo '<p>Get v1.10.0</p>' > docs/landing/index.html
expect 1 "landing naming only a longer version fails" "does not name v1.1.0"

# Freshness: the landing page already names v1.1.0 at v1.0.0 and is not touched.
git checkout -q -f --detach base && git clean -qfd
echo '<p>Get v1.0.0; v1.1.0 is next</p>' > docs/landing/index.html
git_q commit -qam "landing names next" && git_q tag -f v1.0.0
write_release 1.1.0
expect 1 "landing unchanged since the previous tag fails" "unchanged since v1.0.0"
git_q tag -f v1.0.0 base

# Ref mode reads the tagged commit, not the working tree.
reset_complete
git_q add -A && git_q commit -qm v1.1.0 && git_q tag v1.1.0
expect pass "complete tagged release passes at the tag" "ready for v1.1.0 at v1.1.0" v1.1.0 v1.1.0
rm "$(notes_zh 1.1.0)"
expect pass "a dirty working tree does not change a tag check" "ready for v1.1.0 at v1.1.0" v1.1.0 v1.1.0
git checkout -q -f v1.1.0
expect 1 "an incomplete commit fails at its ref" "release notes missing" v1.1.0 base

expect 2 "a malformed version is a usage error" "must look like vX.Y.Z" 1.1.0
expect 2 "an unknown ref is a usage error" "unknown ref" v1.1.0 no-such-ref
expect 1 "a fake next version fails against a complete earlier tag" "release notes missing" v1.2.0 v1.1.0

if [[ "$failures" -gt 0 ]]; then
  echo "$failures release-prep self-test case(s) failed" >&2
  exit 1
fi
echo "release-prep self-test passed"
