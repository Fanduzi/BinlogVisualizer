// Package binlogviz registers the analyze command's flag surface.
// input: the cobra command under construction and its analyzeOptions target.
// output: every analyze flag bound, including the remote input mode flags (ADR 0021).
// pos: flag table for analyze.go; kept separate so the command body stays under the size ceiling.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/i18n"
	"binlogviz/internal/remote"
	"binlogviz/internal/report"
)

func registerAnalyzeFlags(cmd *cobra.Command, opts *analyzeOptions) {
	cmd.Flags().StringVar(&opts.startTime, "start", "", i18n.T("cmd.analyze.flag.start"))
	cmd.Flags().StringVar(&opts.endTime, "end", "", i18n.T("cmd.analyze.flag.end"))
	cmd.Flags().Int64Var(&opts.startPosition, "start-position", 0, "Start position (inclusive event boundary)")
	cmd.Flags().Int64Var(&opts.stopPosition, "stop-position", 0, "Stop position (exclusive event boundary or EOF)")
	cmd.Flags().StringSliceVar(&opts.includeGTIDs, "include-gtids", nil, "Include complete transaction groups matching this GTID set")
	cmd.Flags().StringSliceVar(&opts.excludeGTIDs, "exclude-gtids", nil, "Exclude complete transaction groups matching this GTID set")
	cmd.Flags().StringSliceVar(&opts.threadIDs, "thread-id", nil, i18n.T("cmd.analyze.flag.threadID"))
	cmd.Flags().StringVar(&opts.fromDir, "from-dir", "", i18n.T("cmd.analyze.flag.fromDir"))
	cmd.Flags().StringVar(&opts.prefix, "prefix", "", i18n.T("cmd.analyze.flag.prefix"))
	cmd.Flags().StringVar(&opts.indexFile, "index-file", "", i18n.T("cmd.analyze.flag.indexFile"))
	cmd.Flags().StringVar(&opts.format, "format", "text", i18n.T("cmd.analyze.flag.format"))
	cmd.Flags().StringVar(&opts.csvDataset, "csv-dataset", "", i18n.T("cmd.analyze.flag.csvDataset"))
	cmd.Flags().StringVarP(&opts.output, "output", "o", "", i18n.T("cmd.analyze.flag.output"))
	cmd.Flags().StringVar(&opts.snapshotName, "snapshot-name", "", "Save JSON analyze output as a named snapshot")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Directory to save analyze snapshots")
	cmd.Flags().StringVar(&opts.workloadID, "workload-id", "", i18n.T("cmd.analyze.flag.workloadID"))
	cmd.Flags().StringVar(&opts.sqlContext, "sql-context", string(report.SQLContextSummary), i18n.T("cmd.analyze.flag.sqlContext"))
	cmd.Flags().StringVar(&opts.diagnostics, "diagnostics", string(report.DiagnosticsFull), i18n.T("cmd.analyze.flag.diagnostics"))
	cmd.Flags().IntVar(&opts.top, "top", report.DefaultTopN, i18n.T("cmd.analyze.flag.top"))
	cmd.Flags().IntVar(&opts.topTables, "top-tables", 10, i18n.T("cmd.analyze.flag.topTables"))
	cmd.Flags().IntVar(&opts.topTransactions, "top-transactions", 10, i18n.T("cmd.analyze.flag.topTransactions"))
	cmd.Flags().IntVar(&opts.topThreads, "top-threads", 10, i18n.T("cmd.analyze.flag.topThreads"))
	cmd.Flags().IntVar(&opts.topRows, "top-rows", 10, i18n.T("cmd.analyze.flag.topRows"))
	cmd.Flags().IntVar(&opts.topFingerprints, "top-fingerprints", 10, i18n.T("cmd.analyze.flag.topFingerprints"))
	cmd.Flags().BoolVar(&opts.details, "details", false, i18n.T("cmd.analyze.flag.details"))
	cmd.Flags().BoolVar(&opts.showMinutes, "show-minutes", false, i18n.T("cmd.analyze.flag.showMinutes"))
	cmd.Flags().BoolVar(&opts.showPatterns, "show-patterns", false, i18n.T("cmd.analyze.flag.showPatterns"))
	cmd.Flags().BoolVar(&opts.showFingerprints, "show-fingerprints", false, i18n.T("cmd.analyze.flag.showFingerprints"))
	cmd.Flags().BoolVar(&opts.detectSpikes, "detect-spikes", false, i18n.T("cmd.analyze.flag.detectSpikes"))
	cmd.Flags().IntVar(&opts.largeTrxRows, "large-trx-rows", 1000, i18n.T("cmd.analyze.flag.largeTrxRows"))
	cmd.Flags().DurationVar(&opts.largeTrxDuration, "large-trx-duration", 30*time.Second, i18n.T("cmd.analyze.flag.largeTrxDuration"))
	cmd.Flags().IntVar(&opts.topMinutes, "top-minutes", 60, i18n.T("cmd.analyze.flag.topMinutes"))
	cmd.Flags().IntVar(&opts.spikeWindow, "spike-window", 5, i18n.T("cmd.analyze.flag.spikeWindow"))
	cmd.Flags().Float64Var(&opts.spikeFactor, "spike-factor", 3.0, i18n.T("cmd.analyze.flag.spikeFactor"))
	cmd.Flags().IntVar(&opts.spikeMinRows, "spike-min-rows", 100, i18n.T("cmd.analyze.flag.spikeMinRows"))
	cmd.Flags().StringSliceVar(&opts.includeSchemas, "include-schema", nil, i18n.T("cmd.analyze.flag.includeSchema"))
	cmd.Flags().StringSliceVar(&opts.excludeSchemas, "exclude-schema", nil, i18n.T("cmd.analyze.flag.excludeSchema"))
	cmd.Flags().StringSliceVar(&opts.includeTables, "include-table", nil, i18n.T("cmd.analyze.flag.includeTable"))
	cmd.Flags().StringSliceVar(&opts.dml, "dml", nil, i18n.T("cmd.analyze.flag.dml"))
	cmd.Flags().BoolVar(&opts.showRows, "show-rows", false, i18n.T("cmd.analyze.flag.showRows"))
	cmd.Flags().StringSliceVar(&opts.excludeTables, "exclude-table", nil, i18n.T("cmd.analyze.flag.excludeTable"))
	cmd.Flags().StringVar(&opts.detailStore, "detail-store", string(analyzer.DetailStoreNone), i18n.T("cmd.analyze.flag.detailStore"))
	cmd.Flags().StringVar(&opts.remoteHost, "host", "", i18n.T("cmd.analyze.flag.host"))
	cmd.Flags().Uint16Var(&opts.remotePort, "port", 3306, i18n.T("cmd.analyze.flag.port"))
	cmd.Flags().StringVar(&opts.remoteUser, "user", "", i18n.T("cmd.analyze.flag.user"))
	cmd.Flags().StringVar(&opts.remotePassword, "password", "", i18n.T("cmd.analyze.flag.password"))
	cmd.Flags().Uint32Var(&opts.remoteServerID, "server-id", 0, i18n.T("cmd.analyze.flag.serverID"))
	cmd.Flags().StringVar(&opts.remoteStartFile, "start-file", "", i18n.T("cmd.analyze.flag.startFile"))
	cmd.Flags().Uint32Var(&opts.remoteStartPos, "start-pos", 4, i18n.T("cmd.analyze.flag.startPos"))
	cmd.Flags().StringVar(&opts.remoteStartGTID, "start-gtid", "", i18n.T("cmd.analyze.flag.startGTID"))
	cmd.Flags().StringVar(&opts.remoteSSLMode, "ssl-mode", string(remote.SSLPreferred), i18n.T("cmd.analyze.flag.sslMode"))
}
