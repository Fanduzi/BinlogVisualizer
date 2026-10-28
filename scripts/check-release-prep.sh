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
  0. this clone has release tags, and one of them is older than vX.Y.Z
  1. release notes exist in English and Chinese and name the version
  2. CHANGELOG.md has a non-empty section for the version (outside code
     fences and HTML comments)
  3. README.md and README_ZH.md install pins name the version (and no other)
  4. no file under docs/landing/ names a release version, so a release never
     edits the landing page. Only release-shaped tokens count (vX.Y, vX.Y.Z
     or bare X.Y.Z — not 1.5 GB, MySQL 8.0 or 10.0.0.1), minus the contexts
     in scripts/landing-allowed-numbers.txt, which the Go landing guard
     (cmd/binlogviz) reads too so both gates agree

With a git ref (release.yml passes the tag), files are read from that commit.
Without one, the working tree is read, so the check can run before committing.
Pre-release tags (vX.Y.Z-rc1) are rejected on purpose: the gate is for final
releases only.
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
# A pin is matched with its whole version (extra segments, suffixes, quotes),
# so v1.1.0.1 or "1.0.0" is never read as a match. --version requires the v
# (--version 8.0.36 is another tool's flag, not our pin) and the version must
# end alnum, so a trailing '.' or ')' at a sentence end is not part of it.
PIN_VER='[0-9]([0-9A-Za-z.+-]*[0-9A-Za-z])?'
PIN_RE="releases/download/v${PIN_VER}/|BinlogVisualizer/v${PIN_VER}/install\\.sh|--version[ =][\"']?v${PIN_VER}"
PIN_REQUIRED=0
PIN_HINT="none"
EXTRA_PINS=()
LANDING_DIR="docs/landing"
# Contexts in which a release-shaped token may appear on the landing page;
# the Go landing guard reads the same file, so CI and this gate agree.
ALLOWED_NUMBERS="scripts/landing-allowed-numbers.txt"
allowed_contexts=()
if [[ -f "$ALLOWED_NUMBERS" ]]; then
  while IFS=$'\t' read -r ctx _why; do
    [[ -n "$ctx" && "$ctx" != \#* ]] && allowed_contexts+=("$ctx")
  done < "$ALLOWED_NUMBERS"
fi
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

# 0. Release tags. A shallow clone (fetch-depth 1) has none, and with no older
# tag nothing can be compared, so both stop here with a message instead of
# passing vacuously or exiting silently.
release_tags="$(git tag -l 'v*' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' || true)"
if [[ -z "$release_tags" ]]; then
  fail "no vX.Y.Z release tags in this clone; a shallow clone (fetch-depth 1) has none: run git fetch --tags"
  echo "[release-check] $failures check(s) failed for $TAG${REF:+ at $REF}" >&2
  exit 1
fi
older_tag="$({ grep -Fxv -- "$TAG" <<<"$release_tags" || true; } | { cat; echo "$TAG"; } | sort -V \
  | awk -v t="$TAG" '$0 == t { print prev; exit } { prev = $0 }')"
if [[ -z "$older_tag" ]]; then
  fail "no release tag older than $TAG in this clone; run git fetch --tags"
else
  pass "previous release tag is $older_tag"
fi

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

# 2. CHANGELOG section for the version, with at least one line of body before
# the next '## ' heading. Headings and body inside ``` fences or <!-- -->
# comments do not count.
changelog_section() { # prints "none", "empty" or "ok"
  CL_RE="^## \\[?v?${VER_RE}\\]?([[:space:]]|$)" awk '
    BEGIN { re = ENVIRON["CL_RE"] }
    { line = $0 }
    in_comment { if (index(line, "-->")) { in_comment = 0; line = substr(line, index(line, "-->") + 3) } else next }
    { while (match(line, /<!--/)) { rest = substr(line, RSTART + 4); e = index(rest, "-->")
        if (e) { line = substr(line, 1, RSTART - 1) substr(rest, e + 3) } else { line = substr(line, 1, RSTART - 1); in_comment = 1; break } } }
    line ~ /^[ \t]*(```|~~~)/ { in_fence = !in_fence; next }
    in_fence { if (found && line ~ /[^ \t]/) body = 1; next }
    found && line ~ /^## / { exit }
    found && line ~ /[^ \t]/ { body = 1 }
    !found && line ~ re { found = 1 }
    END { print (found ? (body ? "ok" : "empty") : "none") }'
}
if ! has_file CHANGELOG.md; then
  fail "CHANGELOG.md missing"
else
  case "$(read_file CHANGELOG.md | changelog_section)" in
    ok) pass "CHANGELOG.md has a $TAG section" ;;
    empty) fail "CHANGELOG.md's '## $TAG' section is empty" ;;
    *) fail "CHANGELOG.md has no '## $TAG' section" ;;
  esac
fi

# 3. README install pins: every pinned version must be this one.
check_pins() {
  local path="$1" pins wrong
  if ! has_file "$path"; then
    fail "$path missing"
    return
  fi
  pins="$(grep -Eo "$PIN_RE" <<<"$(read_file "$path")" \
    | sed -E 's#^releases/download/v##; s#/$##; s#^BinlogVisualizer/v##; s#/install\.sh$##; s#^--version[ =]["'"'"']?v?##' \
    | sort -u || true)"
  if [[ -z "$pins" ]]; then
    if [[ "$PIN_REQUIRED" == 1 ]]; then
      fail "$path has no install pin (expected $PIN_HINT)"
    else
      pass "$path pins no release (install resolves the latest release)"
    fi
    return
  fi
  wrong="$(grep -Fvx -- "$VER" <<<"$pins" || true)"
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
# links to GitHub Releases, so no landing file may name a release. The rule
# is shared with cmd/binlogviz's landing guard: only release-shaped tokens
# count, comments included, minus the shared allowlist's contexts.
list_landing() {
  if [[ -n "$REF" ]]; then git ls-tree -r --name-only "$REF" -- "$LANDING_DIR"; else find "$LANDING_DIR" -type f 2>/dev/null | sort; fi
}
# Decode numeric and &period; character references, then print each
# release-shaped token: v1.2, v1.2.3 or bare 1.2.3, also inside names such
# as binlogviz_v1.2.3_linux. A bare two-part number is a CSS value or a
# "MySQL 8.0", not a release, and a token followed by another .digit is a
# longer run (10.0.0.1 is an address).
landing_versions() {
  perl -0777 -ne '
    s/&#[xX]([0-9a-fA-F]+);?/chr(hex $1)/ge; s/&#([0-9]+);?/chr($1)/ge; s/&period;/./gi;
    while (/(?:^|[^0-9.])([vV][ \t]*[0-9]+[ \t]*\.[ \t]*[0-9]+(?:[ \t]*\.[ \t]*[0-9]+)?|[0-9]+[ \t]*\.[ \t]*[0-9]+[ \t]*\.[ \t]*[0-9]+)(?!\.\d)/g) {
      (my $t = $1) =~ s/[ \t]+//g; print "$t\n";
    }'
}
landing_files="$(list_landing || true)"
if [[ -z "$landing_files" ]]; then
  fail "landing page missing: no files under $LANDING_DIR/"
else
  while IFS= read -r path; do
    body="$(read_file "$path")"
    for ctx in ${allowed_contexts[@]+"${allowed_contexts[@]}"}; do
      if ! grep -Fq -- "$ctx" <<<"$body"; then
        fail "$path no longer contains allowed number context '$ctx'; drop it from $ALLOWED_NUMBERS"
      else
        body="$(CTX="$ctx" perl -pe 's/\Q$ENV{CTX}\E//g' <<<"$body")"
      fi
    done
    found="$(landing_versions <<<"$body" | sort -u | tr '\n' ' ')"
    found="${found% }"
    if [[ -n "$found" ]]; then
      fail "$path names release version $found; the landing page is version-free, so remove it (release history goes to docs/releases and GitHub Releases); if it is not a BinlogViz release, add its context to $ALLOWED_NUMBERS"
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
