#!/usr/bin/env bash
# Remove only skill-local scratch/ (snapshot-dirs, workflow outs from drives).
# NEVER deletes evidence/.
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRATCH="$SKILL_DIR/scratch"

if [[ ! -d "$SCRATCH" ]]; then
  printf 'cleanup: nothing to do (no scratch/)\n'
  exit 0
fi

# Safety: only delete under the skill's own scratch path
case "$SCRATCH" in
  */.cursor/skills/verify-binlogviz/scratch) ;;
  *)
    printf 'cleanup: refusing unexpected scratch path: %s\n' "$SCRATCH" >&2
    exit 1
    ;;
esac

rm -rf "${SCRATCH:?}"/*
mkdir -p "$SCRATCH"
printf 'cleanup: removed contents of %s\n' "$SCRATCH"
printf 'cleanup: evidence preserved at %s/evidence\n' "$SKILL_DIR"
exit 0
