#!/usr/bin/env bash
# Drive binlogviz once: capture exit, stdout, stderr under evidence/<RUN_ID>/.
# Usage: drive.sh <label> -- <binlogviz args...>
# Does NOT abort on non-zero CLI exit; always writes meta and returns the CLI exit.
set -uo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$SKILL_DIR/../../.." && pwd)"
FALLBACK_BIN="/home/box/bin/binlogviz"
EVIDENCE_ROOT="$SKILL_DIR/evidence"
SCRATCH_ROOT="$SKILL_DIR/scratch"

usage() {
  printf 'usage: %s <label> -- <binlogviz args...>\n' "$(basename "$0")" >&2
  exit 2
}

[[ $# -ge 3 ]] || usage
LABEL="$1"
shift
[[ "$1" == "--" ]] || usage
shift
[[ $# -ge 1 ]] || usage

# Sanitize label for filesystem
SAFE_LABEL="$(printf '%s' "$LABEL" | tr -cs 'A-Za-z0-9._-' '-' | sed 's/^-//;s/-$//')"
[[ -n "$SAFE_LABEL" ]] || SAFE_LABEL="run"

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

BIN="$(resolve_bin)" || {
  printf 'drive: no binlogviz binary (set BINLOGVIZ or build/fallback)\n' >&2
  exit 1
}

RUN_ID="$(date +%Y%m%d-%H%M%S)-${SAFE_LABEL}"
RUN_DIR="$EVIDENCE_ROOT/$RUN_ID"
mkdir -p "$RUN_DIR"

# Optional scratch for callers that need snapshot-dir / workflow outs
SCRATCH="$SCRATCH_ROOT/$RUN_ID"
mkdir -p "$SCRATCH/snapshots"

UTC_NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
LOCAL_NOW="$(date +%Y-%m-%dT%H:%M:%S%z)"

# Record argv literally
CMD_LINE="$BIN"
for a in "$@"; do
  CMD_LINE+=" $(printf '%q' "$a")"
done

set +e
# Run from repo root so relative fixture paths work
(
  cd "$REPO_ROOT"
  "$BIN" "$@"
) >"$RUN_DIR/stdout" 2>"$RUN_DIR/stderr"
EXIT=$?
set -e

{
  printf 'run_id=%s\n' "$RUN_ID"
  printf 'label=%s\n' "$LABEL"
  printf 'binary=%s\n' "$BIN"
  printf 'cwd=%s\n' "$REPO_ROOT"
  printf 'utc=%s\n' "$UTC_NOW"
  printf 'local=%s\n' "$LOCAL_NOW"
  printf 'exit=%s\n' "$EXIT"
  printf 'scratch=%s\n' "$SCRATCH"
  printf 'cmd=%s\n' "$CMD_LINE"
} >"$RUN_DIR/meta.txt"

printf 'drive: evidence=%s exit=%s\n' "$RUN_DIR" "$EXIT" >&2
printf 'drive: scratch=%s (use --snapshot-dir %s/snapshots)\n' "$SCRATCH" "$SCRATCH" >&2
# Export hint for wrappers that source this (not used when executed)
printf '%s\n' "$RUN_DIR"
exit "$EXIT"
