// Package report formats the DDL timeline's transaction identity and the
// stop-before hint a DBA copies for mysqlbinlog or BinlogServer.
// input: one DDL event plus the Format Description server version used to pick mysqlbinlog or mariadb-binlog.
// output: identity text, the transaction-start offset, and copyable stop commands that replay earlier events and exclude this DDL.
// pos: shared by text, Markdown, HTML, and JSON so the four formats name the same GTID and byte.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"fmt"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// ddlTimelineView is the operator-facing identity of one DDL timeline entry.
type ddlTimelineView struct {
	Identity    string
	TxnStart    string
	Explain     string
	Mysqlbinlog string
	StopGTID    string
	GTIDNote    string
}

func ddlTimelineViewFor(event model.DDLEvent, serverVersion string) ddlTimelineView {
	path, pos := ddlTxnAnchor(event)
	view := ddlTimelineView{
		Identity: ddlIdentityLine(event),
		TxnStart: ddlTxnStartText(path, pos),
		StopGTID: event.GTID,
	}
	if event.GTID == "" {
		view.GTIDNote = i18n.T("report.text.ddlGTIDUnavailable")
	}
	view.Mysqlbinlog = ddlMysqlbinlogStop(path, pos, serverVersion)
	if view.Mysqlbinlog != "" || view.StopGTID != "" {
		view.Explain = i18n.T("report.text.ddlStopBefore")
	}
	return view
}

func ddlIdentityLine(event model.DDLEvent) string {
	parts := make([]string, 0, 4)
	if event.ServerID != 0 {
		parts = append(parts, fmt.Sprintf("server_id=%d", event.ServerID))
	}
	if event.ThreadID != 0 {
		parts = append(parts, fmt.Sprintf("thread_id=%d", event.ThreadID))
	}
	if event.GTID != "" {
		parts = append(parts, "gtid="+event.GTID)
	} else {
		parts = append(parts, i18n.T("report.text.ddlGTIDUnavailable"))
	}
	if userHost := formatUserHost(event.ActorUser, event.ActorHost); userHost != "" {
		parts = append(parts, "user@host="+userHost)
	}
	return strings.Join(parts, " ")
}

func ddlTxnAnchor(event model.DDLEvent) (string, int64) {
	if event.TxnStartPos > 0 {
		path := event.TxnStartPath
		if path == "" {
			path = event.BinlogPath
		}
		return path, event.TxnStartPos
	}
	if event.PositionStart > 0 {
		return event.BinlogPath, event.PositionStart
	}
	return "", 0
}

func ddlTxnStartText(path string, pos int64) string {
	if pos <= 0 {
		return ""
	}
	if path == "" {
		return fmt.Sprintf("%d", pos)
	}
	return fmt.Sprintf("%s:%d", path, pos)
}

func ddlMysqlbinlogStop(path string, pos int64, serverVersion string) string {
	if pos <= 0 || path == "" || path == "stdin" {
		return ""
	}
	return fmt.Sprintf("%s --stop-position=%d %s", replayBinlogBinary(serverVersion), pos, path)
}
