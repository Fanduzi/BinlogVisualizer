# BinlogViz v0.5.2 Release Notes

Release date: 2026-03-29

## Overview

v0.5.2 fixes the Homebrew tap synchronization path introduced in v0.5.1. The release workflow now configures git authentication explicitly before cloning and pushing the tap repository.

## New Features

None.

## Bug Fixes

### Homebrew Tap Sync Authentication

The release workflow now runs `gh auth setup-git` before the tap synchronization step. This ensures the workflow can authenticate git operations consistently when updating `Fanduzi/homebrew-binlogviz`.

## Breaking Changes

None.
