# Security Policy

## Supported Versions

Security fixes are only guaranteed for the latest tagged release.

Current supported line:

- `v0.2.1`

Older tags may remain visible for historical reasons, but they should not be treated as supported production releases.

## Reporting a Vulnerability

If you believe you have found a security issue in BinlogViz:

1. Do not open a public GitHub issue with full exploit details.
2. Contact the maintainer privately first.
3. Include:
   - affected version or commit
   - operating system and architecture
   - reproduction steps
   - impact assessment
   - any suggested mitigation, if available

If no dedicated security contact channel is published yet, use the repository owner contact path and clearly mark the message as a security report.

## Scope Notes

BinlogViz is a local CLI tool for offline binlog analysis. The main security-sensitive areas are:

- parsing untrusted binlog files
- temporary local DuckDB storage during analysis
- installation and release artifact verification

Users should verify release checksums before running downloaded binaries.
