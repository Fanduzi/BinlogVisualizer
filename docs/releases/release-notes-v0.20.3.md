# BinlogViz v0.20.3 Release Notes

Release date: 2026-04-22

## Changes

- **Replace offset-based last-timestamp probing with sequential inference**: The previous `probeLastTimestamp` function parsed from the last 256KB of each binlog file to find `LastEventAt`. On large files the offset lands mid-event, causing parse errors that leave `LastEventAt` zero — making the probing useless for the files that need it most. The new approach probes only `FirstEventAt` per file (reliable, starts from offset 0) and then infers `LastEventAt[N]` from `FirstEventAt[N+1]`. Since binlog files are written sequentially with monotonically increasing timestamps, this inference is always reliable. The last file in a sequence keeps zero `LastEventAt` (conservative inclusion by the planner). The fallback full-parse path remains unchanged and still sets exact `LastEventAt`.

## Bug Fixes

None.

## Breaking Changes

None.
