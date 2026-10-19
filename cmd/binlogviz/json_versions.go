// Package binlogviz: schema versions of the command JSON outputs (ADR 0010).
// input: none.
// output: the schema_version each cmd-level JSON document carries.
// pos: bump one constant, and note it in CHANGELOG as breaking, when that output's shape breaks.
// note: compare and trend own theirs (internal/compare, internal/trend); analyze uses report_version and flashback --plan uses plan_version.
package binlogviz

const (
	snapshotSchemaVersion         = 1 // snapshot list|show --format json
	workflowValidateSchemaVersion = 1
	workflowDescribeSchemaVersion = 1
	workflowStatusSchemaVersion   = 1
	workflowCleanSchemaVersion    = 1
	workflowExportSchemaVersion   = 1
	versionSchemaVersion          = 1 // version --format json
)
