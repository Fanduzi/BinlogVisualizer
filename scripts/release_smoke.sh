#!/usr/bin/env bash
# input: a packed release tar.gz produced by scripts/pack_release_archive.sh.
# output: non-zero exit if the archive is missing the binary/sample/plan, if version --format json is not a versioned document (or, with BINLOGVIZ_SMOKE_EXPECT_COMMIT / BINLOGVIZ_SMOKE_REQUIRE_COMMIT, lacks the expected commit), or if analyze/compare/trend/workflow fail on the bundled fixture.
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

# Build identity (ADR 0010). version --format json must be a versioned
# document. BINLOGVIZ_SMOKE_EXPECT_COMMIT=<sha> requires that exact commit;
# BINLOGVIZ_SMOKE_REQUIRE_COMMIT=1 requires a non-empty one. Both default off
# so a build without VCS info (e.g. -buildvcs=false) still passes.
"${bin}" version --format json > "${workdir}/version.json"
sed -n 1p "${workdir}/version.json" | grep -q '^{$' || { echo "version --format json: not a JSON object" >&2; exit 1; }
sed -n 2p "${workdir}/version.json" | grep -Eq '^  "schema_version": [0-9]+,$' || { echo "version --format json: schema_version is not the first key" >&2; exit 1; }
built_commit="$(sed -n 's/^  "commit": "\([^"]*\)",$/\1/p' "${workdir}/version.json")"
built_commit_time="$(sed -n 's/^  "commit_time": "\([^"]*\)",$/\1/p' "${workdir}/version.json")"
echo "smoke: binary commit=${built_commit:-<empty>} commit_time=${built_commit_time:-<empty>}"
if [[ -n "${BINLOGVIZ_SMOKE_EXPECT_COMMIT:-}" ]] && [[ "${built_commit}" != "${BINLOGVIZ_SMOKE_EXPECT_COMMIT}" ]]; then
  echo "version --format json: commit is \"${built_commit}\", want \"${BINLOGVIZ_SMOKE_EXPECT_COMMIT}\"" >&2
  exit 1
fi
if [[ "${BINLOGVIZ_SMOKE_REQUIRE_COMMIT:-}" == "1" ]] && { [[ -z "${built_commit}" ]] || [[ -z "${built_commit_time}" ]]; }; then
  echo "version --format json: commit or commit_time is empty; build with VCS info or -X binlogviz/internal/version.Commit/CommitTime" >&2
  exit 1
fi

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
