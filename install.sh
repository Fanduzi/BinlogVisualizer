#!/bin/sh
# input: optional --version, --bin-dir, and --dry-run flags plus a GitHub Releases endpoint that serves tagged archives and checksums.
# output: installs the matching release artifact into the requested bin directory, or prints the resolved download plan in dry-run mode.
# pos: minimal release-install helper that prefers GitHub Release artifacts while keeping go build/go install as documented fallbacks.
# note: if this file changes, update README.md and release workflow docs to keep install instructions aligned.

set -eu

REPO="Fanduzi/BinlogVisualizer"
VERSION=""
BIN_DIR="/usr/local/bin"
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --version)
      VERSION="$2"
      shift 2
      ;;
    --bin-dir)
      BIN_DIR="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [ -z "$VERSION" ]; then
  echo "--version is required, for example: ./install.sh --version v0.2.0" >&2
  exit 1
fi

case "$(uname -s)" in
  Darwin) OS="darwin" ;;
  Linux) OS="linux" ;;
  *)
    echo "unsupported OS: $(uname -s)" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64|amd64) ARCH="amd64" ;;
  arm64|aarch64) ARCH="arm64" ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

VERSION_STRIPPED="${VERSION#v}"
ARCHIVE="binlogviz_${VERSION_STRIPPED}_${OS}_${ARCH}.tar.gz"
CHECKSUMS="binlogviz_${VERSION_STRIPPED}_checksums.txt"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
ARCHIVE_URL="${BASE_URL}/${ARCHIVE}"
CHECKSUMS_URL="${BASE_URL}/${CHECKSUMS}"

if [ "$DRY_RUN" -eq 1 ]; then
  printf 'version=%s\nos=%s\narch=%s\narchive=%s\narchive_url=%s\nchecksums_url=%s\nbin_dir=%s\n' \
    "$VERSION" "$OS" "$ARCH" "$ARCHIVE" "$ARCHIVE_URL" "$CHECKSUMS_URL" "$BIN_DIR"
  exit 0
fi

TMPDIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT INT TERM

curl -fsSL "$ARCHIVE_URL" -o "$TMPDIR/$ARCHIVE"
curl -fsSL "$CHECKSUMS_URL" -o "$TMPDIR/$CHECKSUMS"

if command -v shasum >/dev/null 2>&1; then
  (
    cd "$TMPDIR"
    shasum -a 256 -c "$CHECKSUMS" 2>/dev/null | grep "$ARCHIVE: OK" >/dev/null
  )
elif command -v sha256sum >/dev/null 2>&1; then
  (
    cd "$TMPDIR"
    sha256sum -c "$CHECKSUMS" 2>/dev/null | grep "$ARCHIVE: OK" >/dev/null
  )
else
  echo "warning: no checksum tool found; skipping checksum verification" >&2
fi

mkdir -p "$BIN_DIR"
tar -xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"
install "$TMPDIR/binlogviz" "$BIN_DIR/binlogviz"

echo "installed binlogviz to $BIN_DIR/binlogviz"
