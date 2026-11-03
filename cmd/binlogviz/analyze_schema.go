// Package binlogviz wires one --schema-file definition into a run.
// input: the analyze options, the parser, and the report options.
// output: the capture resolver and the report's definition lookup, both built
// from one catalog (ADR 0030, ADR 0031).
// pos: keeps analyze.go inside its file and function ceilings.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"os"
	"strings"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/i18n"
	"binlogviz/internal/report"
	"binlogviz/internal/schemacatalog"
)

// analyzeSchemaCatalog builds the definition catalog once per run. It is both
// the undo capture's resolver (ADR 0030) and the report's secondary source of
// primary-key presence (ADR 0031). Without --schema-file there is no
// definition, and a MINIMAL row keeps its row_metadata refusal.
func analyzeSchemaCatalog(schemaSQL, schemaFileDB string) *schemacatalog.Catalog {
	if strings.TrimSpace(schemaSQL) == "" {
		return nil
	}
	catalog := schemacatalog.New(schemaFileDB)
	catalog.LoadFile(schemaSQL)
	return catalog
}

// definitionKeys turns the catalog into the report's lookup: the key columns a
// definition declares, and whether it covers the table at all.
func definitionKeys(catalog *schemacatalog.Catalog) report.DefinitionKeys {
	if catalog == nil {
		return nil
	}
	return func(schema, table string) ([]string, bool) {
		cols, pk, ok := catalog.FlashColumns(schema, table)
		if !ok {
			return nil, false
		}
		names := make([]string, 0, len(pk))
		for _, i := range pk {
			if i >= 0 && i < len(cols) {
				names = append(names, cols[i].Name)
			}
		}
		return names, true
	}
}

// applySchemaCatalog hands one definition catalog to the undo capture and to the
// report, so both answer from the same file.
func applySchemaCatalog(parser binlog.Parser, reportOpts *report.Options, opts analyzer.Options) {
	catalog := analyzeSchemaCatalog(opts.SchemaSQL, opts.SchemaFileDB)
	if catalog == nil {
		return
	}
	if setter, ok := parser.(binlog.FlashSchemaParser); ok {
		setter.SetFlashSchemaResolver(catalog)
	}
	reportOpts.DefinitionKeys = definitionKeys(catalog)
}

// prepareAnalyzeRun reads --schema-file into the options and warns when a
// snapshot is requested without a workload identity.
func prepareAnalyzeRun(opts *analyzeOptions) error {
	if err := readAnalyzeSchemaFile(opts); err != nil {
		return err
	}
	warnSnapshotWithoutWorkloadID(opts)
	return nil
}

// enableRowImageCapture turns on the bounded row images --show-rows prints,
// unless --sql-context off already suppresses the values.
func enableRowImageCapture(analyzerOpts *analyzer.Options, opts *analyzeOptions, reportOpts report.Options) {
	if opts.showRows && reportOpts.SQLContextMode != report.SQLContextOff {
		analyzerOpts.CaptureRowImages = true
	}
}

// warnSnapshotWithoutWorkloadID says once, at save time, that the snapshot
// cannot be compared causally. It stays a warning: the flag is optional.
func warnSnapshotWithoutWorkloadID(opts *analyzeOptions) {
	if opts.snapshotName == "" || strings.TrimSpace(opts.workloadID) != "" {
		return
	}
	fmt.Fprintf(os.Stderr, "warning: %s\n", i18n.T("warning.snapshotWorkloadID"))
}
