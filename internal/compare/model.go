// Package compare defines compare-input contracts and comparison result models.
// input: JSON reports emitted by `binlogviz analyze --format json`.
// output: typed compare input structures and, later, compare result structures.
// pos: compare pipeline boundary between JSON loading and diff/render stages.
package compare

type InputReport struct {
	Summary  InputSummary `json:"summary"`
	Tables   []InputTable `json:"tables"`
	Alerts   []InputAlert `json:"alerts"`
	Warnings int          `json:"warnings"`
}

type InputSummary struct {
	TotalTransactions int    `json:"total_transactions"`
	TotalRows         int    `json:"total_rows"`
	TotalEvents       int    `json:"total_events"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
	Duration          string `json:"duration"`
}

type InputTable struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	TotalRows  int    `json:"total_rows"`
	InsertRows int    `json:"insert_rows"`
	UpdateRows int    `json:"update_rows"`
	DeleteRows int    `json:"delete_rows"`
	TxnCount   int    `json:"txn_count"`
}

type InputAlert struct {
	Type     string         `json:"type"`
	Severity string         `json:"severity"`
	Message  string         `json:"message"`
	TxnKey   string         `json:"txn_key,omitempty"`
	Minute   string         `json:"minute,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
}
