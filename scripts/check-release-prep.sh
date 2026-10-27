#!/usr/bin/env bash
# input: a release version (vX.Y.Z), an optional git ref (default: the working tree), and the repo's release surfaces
# output: one line per check; exit 1 when any release surface is missing or stale for that version
# pos: release-prep gate run by `make release-check` and by release.yml before anything is built
# note: if this file changes, update this header and scripts/README.md.
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: check-release-prep.sh vX.Y.Z [git-ref]

Checks that the release surfaces are ready for vX.Y.Z:
  1. release notes exist in English and Chinese and name the version
  2. CHANGELOG.md has a section for the version
  3. README.md and README_ZH.md install pins name the version (and no other)
  4. no file under docs/landing/ names a release version (HTML comments aside),
     so a release never edits the landing page

With a git ref (release.yml passes the tag), files are read from that commit.
Without one, the working tree is read, so the check can run before committing.
USAGE
  exit 2
}

[[ $# -ge 1 && $# -le 2 ]] || usage
TAG="$1"
REF="${2:-}"
if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "[release-check] error: version must look like vX.Y.Z, got '$TAG'" >&2
  usage
fi
VER="${TAG#v}"
VER_RE="${VER//./\\.}"

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# --- repo layout -----------------------------------------------------------
NOTES_EN="docs/releases/release-notes-${TAG}.md"
NOTES_EN_TITLE_RE="^# BinlogViz v${VER_RE} Release Notes"
NOTES_ZH="docs/releases/release-notes-${TAG}.zh-CN.md"
NOTES_ZH_TITLE_RE="^# BinlogViz v${VER_RE} 发布说明"
# Install snippets resolve releases/latest (#281, #285), so a README carries no
# pin; any pinned tag that does appear must be this release.
PIN_RE='releases/download/v[0-9]+\.[0-9]+\.[0-9]+/|BinlogVisualizer/v[0-9]+\.[0-9]+\.[0-9]+/install\.sh|--version v[0-9]+\.[0-9]+\.[0-9]+'
PIN_REQUIRED=0
PIN_HINT="none"
EXTRA_PINS=()
LANDING_DIR="docs/landing"
# ---------------------------------------------------------------------------

failures=0
pass() { echo "[release-check] ok   $*"; }
fail() { echo "[release-check] FAIL $*" >&2; failures=$((failures + 1)); }

if [[ -n "$REF" ]]; then
  git rev-parse --verify --quiet "$REF^{commit}" >/dev/null || { echo "[release-check] error: unknown ref '$REF'" >&2; exit 2; }
fi

has_file() {
  if [[ -n "$REF" ]]; then git cat-file -e "$REF:$1" 2>/dev/null; else [[ -f "$1" ]]; fi
}
read_file() {
  if [[ -n "$REF" ]]; then git show "$REF:$1"; else cat "$1"; fi
}

# 1. Release notes in both languages, each naming the version in its title.
check_note() {
  local path="$1" title_re="$2"
  if ! has_file "$path"; then
    fail "release notes missing: $path"
  elif ! grep -Eq "$title_re" <<<"$(head -n 5 <<<"$(read_file "$path")")"; then
    fail "release notes $path do not open with a title for $TAG"
  else
    pass "release notes $path"
  fi
}
check_note "$NOTES_EN" "$NOTES_EN_TITLE_RE"
check_note "$NOTES_ZH" "$NOTES_ZH_TITLE_RE"

# 2. CHANGELOG section for the version.
if ! has_file CHANGELOG.md; then
  fail "CHANGELOG.md missing"
elif grep -Eq "^## \[?v?${VER_RE}\]?([[:space:]]|$)" <<<"$(read_file CHANGELOG.md)"; then
  pass "CHANGELOG.md has a $TAG section"
else
  fail "CHANGELOG.md has no '## $TAG' section"
fi

# 3. README install pins: every pinned version must be this one.
check_pins() {
  local path="$1" pins wrong
  if ! has_file "$path"; then
    fail "$path missing"
    return
  fi
  pins="$(grep -Eo "$PIN_RE" <<<"$(read_file "$path")" | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | sort -u || true)"
  if [[ -z "$pins" ]]; then
    if [[ "$PIN_REQUIRED" == 1 ]]; then
      fail "$path has no install pin (expected $PIN_HINT)"
    else
      pass "$path pins no release (install resolves the latest release)"
    fi
    return
  fi
  wrong="$(grep -vx "$VER_RE" <<<"$pins" || true)"
  if [[ -n "$wrong" ]]; then
    fail "$path pins $(tr '\n' ' ' <<<"$wrong")instead of $VER"
  else
    pass "$path install pin is $VER"
  fi
}
for readme in README.md README_ZH.md; do
  check_pins "$readme"
done

# Extra pinned files (for example a VERSION file) that must equal the version.
for spec in ${EXTRA_PINS[@]+"${EXTRA_PINS[@]}"}; do
  path="${spec%%=*}"
  want="${spec#*=}"
  want="${want//@VER@/$VER}"
  if ! has_file "$path"; then
    fail "$path missing"
  elif grep -Fxq -- "$want" <<<"$(read_file "$path")"; then
    pass "$path is $want"
  else
    fail "$path is '$(head -n 1 <<<"$(read_file "$path")")', expected '$want'"
  fi
done

# 4. The landing page is version-free: it installs the latest release and
# links to GitHub Releases, so no landing file may name a release.
list_landing() {
  if [[ -n "$REF" ]]; then git ls-tree -r --name-only "$REF" -- "$LANDING_DIR"; else find "$LANDING_DIR" -type f 2>/dev/null | sort; fi
}
# Strip HTML comments, decode numeric and &period; character references, then
# print each version-like token: v1.2, v1.2.3 or 1.2.3, also inside names such
# as binlogviz_v1.2.3_linux. A bare two-part number is a CSS value, not a release.
landing_versions() {
  perl -0777 -ne '
    s/<!--.*?-->//gs;
    s/&#[xX]([0-9a-fA-F]+);?/chr(hex $1)/ge; s/&#([0-9]+);?/chr($1)/ge; s/&period;/./gi;
    while (/(?:^|[^0-9.])([vV][ \t]*[0-9]+[ \t]*\.[ \t]*[0-9]+(?:[ \t]*\.[ \t]*[0-9]+)?|[0-9]+[ \t]*\.[ \t]*[0-9]+[ \t]*\.[ \t]*[0-9]+)/g) {
      (my $t = $1) =~ s/[ \t]+//g; print "$t\n";
    }'
}
landing_files="$(list_landing || true)"
if [[ -z "$landing_files" ]]; then
  fail "landing page missing: no files under $LANDING_DIR/"
else
  while IFS= read -r path; do
    found="$(read_file "$path" | landing_versions | sort -u | tr '\n' ' ')"
    found="${found% }"
    if [[ -n "$found" ]]; then
      fail "$path names release version $found; the landing page is version-free, so remove it (release history goes to docs/releases and GitHub Releases)"
    else
      pass "$path names no release version"
    fi
  done <<<"$landing_files"
fi

if [[ "$failures" -gt 0 ]]; then
  echo "[release-check] $failures check(s) failed for $TAG${REF:+ at $REF}" >&2
  exit 1
fi
echo "[release-check] all release surfaces ready for $TAG${REF:+ at $REF}"
