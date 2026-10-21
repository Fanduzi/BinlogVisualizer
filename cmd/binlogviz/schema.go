// Package binlogviz: the schema command prints the JSON Schema of every versioned JSON output.
// input: a schema name, or no name to list them.
// output: one schema name per line, or one JSON Schema document on stdout.
// pos: how an agent discovers the machine-readable contract without reading Go (ADR 0017).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"binlogviz/internal/clierr"
	comparepkg "binlogviz/internal/compare"
	"binlogviz/internal/flashback"
	"binlogviz/internal/jsonschema"
	"binlogviz/internal/report"
	trendpkg "binlogviz/internal/trend"
	"binlogviz/internal/version"
	"binlogviz/internal/workflow"
)

// schemaOutput is one versioned JSON document. The same list drives the
// schema command, the published files, and TestEveryJSONOutputIsVersioned.
type schemaOutput struct {
	Name       string
	VersionKey string
	Version    int
	Schema     func() ([]byte, error)
}

func schemaOutputs() []schemaOutput {
	return []schemaOutput{
		{Name: "analyze", VersionKey: "report_version", Version: report.ReportVersion(), Schema: report.AnalysisJSONSchema},
		{Name: "flashback-plan", VersionKey: "plan_version", Version: flashback.FlashbackPlanVersion, Schema: flashback.PlanJSONSchema},
		{Name: "compare", VersionKey: "schema_version", Version: comparepkg.SchemaVersion, Schema: comparepkg.JSONSchema},
		{Name: "trend", VersionKey: "schema_version", Version: trendpkg.SchemaVersion, Schema: trendpkg.JSONSchema},
		{Name: "snapshot-list", VersionKey: "schema_version", Version: snapshotSchemaVersion, Schema: snapshotListSchema},
		{Name: "snapshot-show", VersionKey: "schema_version", Version: snapshotSchemaVersion, Schema: snapshotShowSchema},
		{Name: "workflow-validate", VersionKey: "schema_version", Version: workflowValidateSchemaVersion, Schema: workflowValidateSchema},
		{Name: "workflow-describe", VersionKey: "schema_version", Version: workflowDescribeSchemaVersion, Schema: func() ([]byte, error) {
			return workflow.DescribeJSONSchema(workflowDescribeSchemaVersion, workflowValidateErrorJSON{})
		}},
		{Name: "workflow-status", VersionKey: "schema_version", Version: workflowStatusSchemaVersion, Schema: func() ([]byte, error) {
			return workflow.StatusJSONSchema(workflowStatusSchemaVersion)
		}},
		{Name: "workflow-clean", VersionKey: "schema_version", Version: workflowCleanSchemaVersion, Schema: func() ([]byte, error) {
			return workflow.CleanJSONSchema(workflowCleanSchemaVersion)
		}},
		{Name: "workflow-export", VersionKey: "schema_version", Version: workflowExportSchemaVersion, Schema: workflowExportSchema},
		{Name: "version", VersionKey: "schema_version", Version: versionSchemaVersion, Schema: versionSchema},
		{Name: "flashback-verify", VersionKey: "schema_version", Version: flashbackVerifySchemaVersion, Schema: flashbackVerifySchema},
	}
}

func snapshotListSchema() ([]byte, error) {
	return jsonschema.Versioned(snapshotListJSON{}, "snapshot-list", "schema_version", snapshotSchemaVersion)
}

func snapshotShowSchema() ([]byte, error) {
	return jsonschema.Versioned(snapshotShowJSON{}, "snapshot-show", "schema_version", snapshotSchemaVersion)
}

func workflowValidateSchema() ([]byte, error) {
	return jsonschema.UnionVersioned("workflow-validate", "schema_version", workflowValidateSchemaVersion, workflowValidateJSON{}, workflowValidateErrorJSON{})
}

func workflowExportSchema() ([]byte, error) {
	return jsonschema.Versioned(workflowExportOutput{}, "workflow-export", "schema_version", workflowExportSchemaVersion)
}

func versionSchema() ([]byte, error) {
	return jsonschema.Versioned(version.Build{}, "version", "schema_version", versionSchemaVersion)
}

func flashbackVerifySchema() ([]byte, error) {
	return jsonschema.Versioned(flashbackVerifyJSON{}, "flashback-verify", "schema_version", flashbackVerifySchemaVersion)
}

func newSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema [name]",
		Short: "Print the JSON Schema of a JSON output, or list the names",
		Long: `Print the JSON Schema for one versioned JSON document, or list every name.

Schemas are derived from the Go types the commands marshal. The same documents are shipped in the release archive under docs/schema/. A name that is not in the list is a usage error.`,
		Example: `  binlogviz schema
  binlogviz schema analyze`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return writeSchemaNames(cmd.OutOrStdout())
			}
			name := args[0]
			for _, out := range schemaOutputs() {
				if out.Name != name {
					continue
				}
				body, err := out.Schema()
				if err != nil {
					return clierr.Errorf(clierr.KindError, "build schema %s: %v", name, err)
				}
				_, err = fmt.Fprint(cmd.OutOrStdout(), string(body))
				return err
			}
			return clierr.Errorf(clierr.KindUsage, "unknown schema %q. Run 'binlogviz schema' for the names", name)
		},
	}
	return cmd
}

func writeSchemaNames(out io.Writer) error {
	names := make([]string, 0, len(schemaOutputs()))
	for _, item := range schemaOutputs() {
		names = append(names, item.Name)
	}
	_, err := fmt.Fprintln(out, strings.Join(names, "\n"))
	return err
}
