#!/bin/sh
# input: optional --version, --bin-dir, --no-verify, and --dry-run flags plus a GitHub Releases endpoint that serves tagged archives and checksums.
# output: installs the matching release artifact into the requested bin directory after a sha256 check, or prints the resolved download plan in dry-run mode.
# pos: minimal release-install helper that prefers GitHub Release artifacts while keeping go build/go install as documented fallbacks.
# note: if this file changes, update README.md and release workflow docs to keep install instructions aligned.

set -eu

REPO="Fanduzi/BinlogVisualizer"
VERSION=""
BIN_DIR="/usr/local/bin"
DRY_RUN=0
VERIFY=1

die() {
  echo "install.sh: $*" >&2
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version)
      [ $# -ge 2 ] && [ -n "$2" ] || die "--version needs a value (vX.Y.Z or latest)"
      VERSION="$2"
      shift 2
      ;;
    --bin-dir)
      [ $# -ge 2 ] && [ -n "$2" ] || die "--bin-dir needs a directory"
      BIN_DIR="$2"
      shift 2
      ;;
    --no-verify)
      VERIFY=0
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

# Downloads go through curl, or wget when curl is missing.
if command -v curl >/dev/null 2>&1; then
  DOWNLOADER="curl"
elif command -v wget >/dev/null 2>&1; then
  DOWNLOADER="wget"
else
  die "need curl or wget to download the release"
fi

# fetch URL FILE: download URL to FILE; non-2xx is a failure.
fetch() {
  if [ "$DOWNLOADER" = "curl" ]; then
    curl -fsSL "$1" -o "$2"
  else
    wget -q -O "$2" "$1"
  fi
}

# resolve_redirect URL: print the URL that URL finally redirects to.
resolve_redirect() {
  if [ "$DOWNLOADER" = "curl" ]; then
    curl -fsSLI -o /dev/null -w '%{url_effective}' "$1"
  else
    wget -q -S --spider "$1" 2>&1 | awk 'tolower($1) == "location:" { loc = $2 } END { if (loc == "") exit 1; sub(/\r$/, "", loc); print loc }'
  fi
}

# Default to the newest published release so docs and this script can never
# pin a stale version again (#281). Resolved from the releases/latest redirect,
# which needs no API token and is not rate limited like api.github.com.
if [ -z "$VERSION" ] || [ "$VERSION" = "latest" ]; then
  LATEST_URL="${BINLOGVIZ_LATEST_URL:-https://github.com/${REPO}/releases/latest}"
  RESOLVED="$(resolve_redirect "$LATEST_URL")" ||
    die "could not resolve the latest release from $LATEST_URL; pass --version vX.Y.Z"
  VERSION="${RESOLVED##*/}"
  case "$VERSION" in
    v[0-9]*) ;;
    *) die "could not resolve the latest release (got: $RESOLVED); pass --version vX.Y.Z" ;;
  esac
fi

# Accept 0.24.0 as well as v0.24.0; release tags always carry the v.
case "$VERSION" in
  [0-9]*) VERSION="v$VERSION" ;;
esac
case "$VERSION" in
  v[0-9]*.[0-9]*.[0-9]*) ;;
  *) die "invalid --version '$VERSION'; expected vX.Y.Z or latest" ;;
esac

case "$(uname -s)" in
  Darwin) OS="darwin" ;;
  Linux) OS="linux" ;;
  *) die "unsupported OS: $(uname -s)" ;;
esac

case "$(uname -m)" in
  x86_64|amd64) ARCH="amd64" ;;
  arm64|aarch64) ARCH="arm64" ;;
  *) die "unsupported architecture: $(uname -m)" ;;
esac

# sha256_of FILE: print the hex sha256 of FILE using whichever tool exists.
SHA_TOOL=""
if command -v sha256sum >/dev/null 2>&1; then
  SHA_TOOL="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  SHA_TOOL="shasum"
elif command -v openssl >/dev/null 2>&1; then
  SHA_TOOL="openssl"
fi
sha256_of() {
  case "$SHA_TOOL" in
    sha256sum) sha256sum "$1" | awk '{ print $1 }' ;;
    shasum) shasum -a 256 "$1" | awk '{ print $1 }' ;;
    openssl) openssl dgst -sha256 "$1" | awk '{ print $NF }' ;;
  esac
}

if [ "$VERIFY" -eq 1 ]; then
  [ -n "$SHA_TOOL" ] || die "no sha256 tool found (sha256sum, shasum, or openssl); install one, or pass --no-verify to install without checking the download"
  VERIFY_MODE="sha256 ($SHA_TOOL)"
else
  VERIFY_MODE="skipped (--no-verify)"
fi

VERSION_STRIPPED="${VERSION#v}"
ARCHIVE="binlogviz_${VERSION_STRIPPED}_${OS}_${ARCH}.tar.gz"
CHECKSUMS="binlogviz_${VERSION_STRIPPED}_checksums.txt"
BASE_URL="${BINLOGVIZ_DOWNLOAD_BASE:-https://github.com/${REPO}/releases/download}/${VERSION}"
ARCHIVE_URL="${BASE_URL}/${ARCHIVE}"
CHECKSUMS_URL="${BASE_URL}/${CHECKSUMS}"

if [ "$DRY_RUN" -eq 1 ]; then
  printf 'version=%s\nos=%s\narch=%s\narchive=%s\narchive_url=%s\nchecksums_url=%s\nbin_dir=%s\nverify=%s\n' \
    "$VERSION" "$OS" "$ARCH" "$ARCHIVE" "$ARCHIVE_URL" "$CHECKSUMS_URL" "$BIN_DIR" "$VERIFY_MODE"
  exit 0
fi

TMPDIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT INT TERM

fetch "$ARCHIVE_URL" "$TMPDIR/$ARCHIVE" ||
  die "could not download $ARCHIVE_URL; release $VERSION may not exist or has no $OS/$ARCH build (see https://github.com/${REPO}/releases)"

if [ "$VERIFY" -eq 1 ]; then
  fetch "$CHECKSUMS_URL" "$TMPDIR/$CHECKSUMS" ||
    die "could not download $CHECKSUMS_URL; refusing to install an unverified archive (pass --no-verify to skip the check)"
  EXPECTED="$(awk -v f="$ARCHIVE" '$2 == f || $2 == "*" f { print $1; exit }' "$TMPDIR/$CHECKSUMS")"
  [ -n "$EXPECTED" ] || die "no checksum for $ARCHIVE in $CHECKSUMS; refusing to install"
  ACTUAL="$(sha256_of "$TMPDIR/$ARCHIVE")"
  [ "$ACTUAL" = "$EXPECTED" ] ||
    die "checksum mismatch for $ARCHIVE (expected $EXPECTED, got $ACTUAL); the download is corrupt or was tampered with, nothing was installed"
else
  echo "warning: --no-verify given; installing $ARCHIVE without a checksum check" >&2
fi

tar -xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR" || die "could not unpack $ARCHIVE"
[ -f "$TMPDIR/binlogviz" ] || die "$ARCHIVE has no binlogviz binary"

DEST="$BIN_DIR/binlogviz"
if [ -e "$DEST" ]; then
  OLD="$("$DEST" --version 2>/dev/null | tail -n 1)" || OLD=""
  echo "replacing existing $DEST (${OLD:-unknown version}) with $VERSION" >&2
fi
mkdir -p "$BIN_DIR"
install "$TMPDIR/binlogviz" "$DEST"

echo "installed binlogviz $VERSION to $DEST"
