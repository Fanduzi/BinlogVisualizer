#!/usr/bin/env bash
# input: a packed release tar.gz produced by scripts/pack_release_archive.sh.
# output: non-zero exit if the archive is missing the binary/sample/plan or if analyze/compare/trend/workflow fail on the bundled fixture.
# pos: maintainer smoke path that proves a GitHub Release extract works without a git clone.
# note: if this file changes, update this header and scripts/README.md.

set -euo pipefail

archive="${1:-}"
if [[ -z "${archive}" ]]; then
  echo "usage: release_smoke.sh <archive>" >&2
  exit 1
fi

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

tar -xzf "${archive}" -C "${workdir}"

bin="${workdir}/binlogviz"
fixture="${workdir}/testdata/minimal.binlog"
sample_dir="${workdir}/testdata/sample-binlog"
plan="${workdir}/incident.yaml"
snapdir="${workdir}/snapshots"

if [[ ! -x "${bin}" ]]; then
  echo "expected extracted binary at ${bin}" >&2
  exit 1
fi

if [[ ! -f "${fixture}" ]]; then
  echo "expected bundled sample at ${fixture}" >&2
  exit 1
fi

if [[ ! -d "${sample_dir}" ]]; then
  echo "expected bundled discovery directory at ${sample_dir}" >&2
  exit 1
fi

if [[ ! -f "${plan}" ]]; then
  echo "expected bundled workflow plan at ${plan}" >&2
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

(
  cd "${workdir}"
  ./binlogviz workflow run incident.yaml
)
