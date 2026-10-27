#!/usr/bin/env bash
# Prints sha=<revision> for the lint workflow: the merge base with the target
# branch on a pull request, the previous tip on a push. golangci-lint
# --new-from-rev and the baseline comparison both diff against it.
set -euo pipefail

case "${EVENT:-}" in
pull_request)
  git fetch --no-tags --quiet origin "${BASE_REF}"
  base=$(git merge-base "origin/${BASE_REF}" HEAD)
  ;;
*)
  base="${BEFORE:-}"
  # A new branch or a force push has no usable "before".
  if [[ -z "${base}" || "${base}" =~ ^0+$ ]] || ! git cat-file -e "${base}^{commit}" 2>/dev/null; then
    base=$(git rev-parse HEAD~1)
  fi
  ;;
esac
echo "sha=${base}"
