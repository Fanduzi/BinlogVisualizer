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

# The landing page names no release; it installs the latest one.
write_landing() { printf '<style>.a { opacity: 0.45; line-height: 1.6; }</style>\n<p>Get BinlogViz</p>\n' > docs/landing/index.html; }

git_q() { git -c user.name=fixture -c user.email=fixture@example.invalid -c commit.gpgsign=false -c tag.gpgsign=false "$@" >/dev/null; }

repo="$work/repo"
mkdir -p "$repo/scripts" "$repo/docs/releases" "$repo/docs/landing"
cp "$CHECK" "$repo/scripts/check-release-prep.sh"
cd "$repo"
git_q init -q
write_release 1.0.0
write_landing
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
  write_landing
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
# Landing: version-free in every file, whatever the spelling.
landing_case() { # label pattern html [file]
  reset_complete; printf '%s\n' "$3" >> "docs/landing/${4:-index.html}"
  expect 1 "$1" "$2"
}
landing_case "landing naming this release fails" "names release version v1.1.0;" '<p>Get v1.1.0</p>'
landing_case "landing naming an older release fails" "names release version v1.0.0;" '<p>Get v1.0.0</p>'
landing_case "landing naming a bare 1.1.0 fails" "names release version 1.1.0;" '<p>Latest: 1.1.0</p>'
landing_case "landing naming a two-part v1.10 fails" "names release version v1.10;" '<p>v1.10</p>'
landing_case "landing naming a multi-digit V10.20.300 fails" "names release version V10.20.300;" '<p>V10.20.300</p>'
landing_case "landing archive name binlogviz_v1.1.0_linux fails" "names release version v1.1.0;" '<code>binlogviz_v1.1.0_linux_amd64.tar.gz</code>'
landing_case "landing entity-encoded v1&#46;1&#46;0 fails" "names release version v1.1.0;" '<p>v1&#46;1&#46;0</p>'
landing_case "landing hex-entity &#x76;1.1.0 fails" "names release version v1.1.0;" '<p>&#x76;1.1.0</p>'
landing_case "landing &period; entity fails" "names release version v1.1.0;" '<p>v1&period;1&period;0</p>'
landing_case "landing spaced v 1 . 1 . 0 fails" "names release version v1.1.0;" '<p>v 1 . 1 . 0</p>'
landing_case "landing listing two releases names both" "names release version v1.0.0 v1.1.0;" '<p>v1.1.0 after v1.0.0</p>'
landing_case "a second landing file naming a release fails" "docs/landing/releases.html names release version v1.0.0;" '<p>v1.0.0</p>' releases.html
reset_complete; mkdir -p docs/landing/js; printf "const v = 'v1.0.0';\n" > docs/landing/js/r.js
expect 1 "a nested landing file naming a release fails" "docs/landing/js/r.js names release version v1.0.0;"
reset_complete; printf '<!-- v1.1.0\n v1.0.0 -->\n' >> docs/landing/index.html
expect pass "a version only inside an HTML comment passes" "docs/landing/index.html names no release version"
reset_complete; rm -rf docs/landing
expect 1 "a missing landing directory fails" "landing page missing"

# The landing page is not edited per release: unchanged since the previous tag passes.
reset_complete
expect pass "an untouched landing page passes" "docs/landing/index.html names no release version"

# Ref mode reads the tagged commit, not the working tree.
reset_complete
git_q add -A && git_q commit -qm v1.1.0 && git_q tag v1.1.0
expect pass "complete tagged release passes at the tag" "ready for v1.1.0 at v1.1.0" v1.1.0 v1.1.0
rm "$(notes_zh 1.1.0)"
expect pass "a dirty working tree does not change a tag check" "ready for v1.1.0 at v1.1.0" v1.1.0 v1.1.0
git checkout -q -f v1.1.0
expect 1 "an incomplete commit fails at its ref" "release notes missing" v1.1.0 base

reset_complete; printf '<p>v1.1.0</p>\n' >> docs/landing/index.html
git_q add -A && git_q commit -qm "landing names v1.1.0" && git_q tag landing-names-release
write_landing
expect 1 "ref mode reads the landing page from the ref, not the clean tree" "names release version v1.1.0;" v1.1.0 landing-names-release

expect 2 "a malformed version is a usage error" "must look like vX.Y.Z" 1.1.0
expect 2 "an unknown ref is a usage error" "unknown ref" v1.1.0 no-such-ref
expect 1 "a fake next version fails against a complete earlier tag" "release notes missing" v1.2.0 v1.1.0

if [[ "$failures" -gt 0 ]]; then
  echo "$failures release-prep self-test case(s) failed" >&2
  exit 1
fi
echo "release-prep self-test passed"
