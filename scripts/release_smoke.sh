#!/usr/bin/env bash
set -euo pipefail

archive="${1:-}"
if [[ -z "${archive}" ]]; then
  echo "usage: release_smoke.sh <archive>" >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

tar -xzf "${archive}" -C "${workdir}"

bin="${workdir}/binlogviz"
fixture="${repo_root}/internal/binlog/testdata/minimal.binlog"
snapdir="${workdir}/snapshots"

if [[ ! -x "${bin}" ]]; then
  echo "expected extracted binary at ${bin}" >&2
  exit 1
fi

if [[ ! -f "${fixture}" ]]; then
  echo "expected fixture at ${fixture}" >&2
  exit 1
fi

"${bin}" --version | grep -E '.+'
"${bin}" analyze "${fixture}" --format json > "${workdir}/current.json"
"${bin}" analyze "${fixture}" --format json --snapshot-name current --snapshot-dir "${snapdir}" > /dev/null
"${bin}" analyze "${fixture}" --format json --snapshot-name baseline --snapshot-dir "${snapdir}" > /dev/null
"${bin}" compare --current-snapshot current --baseline-snapshot baseline --snapshot-dir "${snapdir}" --format json > "${workdir}/compare.json"
"${bin}" trend current baseline --snapshot-dir "${snapdir}" --format json > "${workdir}/trend.json"

test -s "${workdir}/current.json"
test -s "${workdir}/compare.json"
test -s "${workdir}/trend.json"
