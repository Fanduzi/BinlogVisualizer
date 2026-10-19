#!/usr/bin/env bash
# input: a built binlogviz binary path, a destination tar.gz path, and repo extra files (sample ROW fixture, release-oriented incident.yaml, README, and the agent usage guide).
# output: a tar.gz whose members are the binary, testdata/minimal.binlog, testdata/sample-binlog/mysql-bin.000001, incident.yaml, README.md, README_ZH.md, and docs/agents/USING-BINLOGVIZ.md plus the zh-CN guide.
# pos: shared packing helper used by the GitHub release job, CI smoke, and the packing-contract test so Linux and Darwin archives share one extra-file set.
# note: if this file changes, update this header and scripts/README.md.

set -euo pipefail

binary="${1:-}"
archive="${2:-}"
if [[ -z "${binary}" || -z "${archive}" ]]; then
  echo "usage: pack_release_archive.sh <binary> <archive.tar.gz>" >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

if [[ ! -f "${binary}" ]]; then
  echo "missing binary: ${binary}" >&2
  exit 1
fi

binary_dir="$(cd -- "$(dirname -- "${binary}")" && pwd)"
binary="${binary_dir}/$(basename -- "${binary}")"

sample="${repo_root}/cmd/binlogviz/testdata/minimal.binlog"
discovery_sample="${repo_root}/cmd/binlogviz/testdata/sample-binlog/mysql-bin.000001"
plan="${repo_root}/scripts/release-incident.yaml"
readme="${repo_root}/README.md"
readme_zh="${repo_root}/README_ZH.md"
guide="${repo_root}/docs/agents/USING-BINLOGVIZ.md"
guide_zh="${repo_root}/docs/agents/USING-BINLOGVIZ.zh-CN.md"

for extra in "${sample}" "${discovery_sample}" "${plan}" "${readme}" "${readme_zh}" "${guide}" "${guide_zh}"; do
  if [[ ! -f "${extra}" ]]; then
    echo "missing extra file: ${extra}" >&2
    exit 1
  fi
done

archive_dir="$(dirname -- "${archive}")"
mkdir -p "${archive_dir}"
archive_dir="$(cd -- "${archive_dir}" && pwd)"
archive="${archive_dir}/$(basename -- "${archive}")"

staging="$(mktemp -d)"
trap 'rm -rf "${staging}"' EXIT

mkdir -p "${staging}/testdata/sample-binlog" "${staging}/docs/agents"
cp "${binary}" "${staging}/binlogviz"
chmod +x "${staging}/binlogviz"
cp "${sample}" "${staging}/testdata/minimal.binlog"
cp "${discovery_sample}" "${staging}/testdata/sample-binlog/mysql-bin.000001"
cp "${plan}" "${staging}/incident.yaml"
cp "${readme}" "${staging}/README.md"
cp "${readme_zh}" "${staging}/README_ZH.md"
cp "${guide}" "${staging}/docs/agents/USING-BINLOGVIZ.md"
cp "${guide_zh}" "${staging}/docs/agents/USING-BINLOGVIZ.zh-CN.md"

tar -C "${staging}" -czf "${archive}" binlogviz testdata incident.yaml README.md README_ZH.md docs
