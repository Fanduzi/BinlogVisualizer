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
      [ $# -ge 2 ] || { echo "--version needs a value (vX.Y.Z or latest)" >&2; exit 1; }
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

# Default to the newest published release so docs and this script can never
# pin a stale version again (#281). Resolved from the releases/latest redirect,
# which needs no API token and is not rate limited like api.github.com.
if [ -z "$VERSION" ] || [ "$VERSION" = "latest" ]; then
  LATEST_URL="${BINLOGVIZ_LATEST_URL:-https://github.com/${REPO}/releases/latest}"
  RESOLVED="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$LATEST_URL")" || {
    echo "could not resolve the latest release from $LATEST_URL; pass --version vX.Y.Z" >&2
    exit 1
  }
  VERSION="${RESOLVED##*/}"
  case "$VERSION" in
    v[0-9]*) ;;
    *)
      echo "could not resolve the latest release (got: $RESOLVED); pass --version vX.Y.Z" >&2
      exit 1
      ;;
  esac
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
