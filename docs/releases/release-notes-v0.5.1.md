# BinlogViz v0.5.1 Release Notes

Release date: 2026-03-29

## Overview

v0.5.1 improves installation and release distribution. The project now publishes a Homebrew cask definition from the release workflow, making macOS installation easier while keeping GitHub Release artifacts as the authoritative distribution source.

## New Features

### Homebrew Cask Installation Path

BinlogViz now supports a tap-based Homebrew installation flow for macOS:

```bash
brew tap Fanduzi/binlogviz
brew install --cask binlogviz
```

The generated cask points at the tagged GitHub Release artifacts and removes the macOS quarantine attribute during installation so first-run behavior is smoother.

### Release Workflow Tap Sync

The release workflow now:

- builds versioned archives directly on native runners
- generates per-platform checksums from the final release artifacts
- creates a Homebrew cask definition for the tagged version
- syncs that cask into the `Fanduzi/homebrew-binlogviz` tap repository

This keeps the Homebrew installation path aligned with the same release artifacts documented in the repository.

## Bug Fixes

### Release Version Injection

The build workflow now injects the tagged version directly into the binary during release builds. This avoids snapshot-style version strings leaking into tagged release binaries.

## Breaking Changes

None.
