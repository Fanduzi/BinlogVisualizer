#!/usr/bin/env bash
# Read-only readiness check for verify-binlogviz.
# EXIT 0 = ready to drive; non-zero = not ready (message on stderr).
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$SKILL_DIR/../../.." && pwd)"
MINIMAL="$REPO_ROOT/cmd/binlogviz/testdata/minimal.binlog"
FALLBACK_BIN="/home/box/bin/binlogviz"

resolve_bin() {
  if [[ -n "${BINLOGVIZ:-}" ]]; then
    printf '%s\n' "$BINLOGVIZ"
    return 0
  fi
  if [[ -x "$REPO_ROOT/binlogviz" ]]; then
    printf '%s\n' "$REPO_ROOT/binlogviz"
    return 0
  fi
  if [[ -x "$FALLBACK_BIN" ]]; then
    printf '%s\n' "$FALLBACK_BIN"
    return 0
  fi
  return 1
}

err() { printf 'doctor: %s\n' "$*" >&2; }

BIN="$(resolve_bin)" || {
  err "no binlogviz binary found"
  err "set BINLOGVIZ, or run: cd $REPO_ROOT && go build -o binlogviz ."
  err "or install a release at $FALLBACK_BIN"
  exit 1
}

if [[ ! -e "$BIN" ]]; then
  err "binary path does not exist: $BIN"
  exit 1
fi
if [[ ! -x "$BIN" ]]; then
  err "binary is not executable: $BIN"
  exit 1
fi

VERSION_OUT="$("$BIN" version 2>&1)" || {
  err "'$BIN' version failed"
  err "$VERSION_OUT"
  exit 1
}
# Strip banner; keep a line that looks like "binlogviz X.Y.Z"
VERSION_LINE="$(printf '%s\n' "$VERSION_OUT" | grep -E '^binlogviz ' | head -n1 || true)"
if [[ -z "$VERSION_LINE" ]]; then
  # Some builds print only the banner + version on last non-empty line
  VERSION_LINE="$(printf '%s\n' "$VERSION_OUT" | grep -E '[0-9]+\.[0-9]+' | tail -n1 || true)"
fi
if [[ -z "$VERSION_LINE" ]]; then
  err "could not parse version from: $BIN"
  err "$VERSION_OUT"
  exit 1
fi

if [[ ! -f "$MINIMAL" ]]; then
  err "ROW fixture missing: $MINIMAL"
  exit 1
fi

SOURCE="unknown"
if [[ -n "${BINLOGVIZ:-}" && "$BIN" == "$BINLOGVIZ" ]]; then
  SOURCE="env BINLOGVIZ"
elif [[ "$BIN" == "$REPO_ROOT/binlogviz" ]]; then
  SOURCE="repo build (./binlogviz)"
elif [[ "$BIN" == "$FALLBACK_BIN" ]]; then
  SOURCE="fallback release ($FALLBACK_BIN)"
fi

GO_VER="(go not on PATH)"
if command -v go >/dev/null 2>&1; then
  GO_VER="$(go version 2>&1 || true)"
fi

printf 'doctor: PASS\n'
printf '  binary:  %s\n' "$BIN"
printf '  source:  %s\n' "$SOURCE"
printf '  version: %s\n' "$VERSION_LINE"
printf '  fixture: %s (%s bytes)\n' "$MINIMAL" "$(wc -c < "$MINIMAL" | tr -d ' ')"
printf '  go:      %s\n' "$GO_VER"
printf '  repo:    %s\n' "$REPO_ROOT"
printf '  skill:   %s\n' "$SKILL_DIR"
exit 0
