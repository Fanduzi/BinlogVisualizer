package report

import (
	"encoding/json"
	"io"
	"os"
	"time"

	"binlogviz/internal/model"
)

// jsonAnalysisResult is the JSON-serializable representation of AnalysisResult.
// Field names use snake_case for script-friendly output.
type jsonAnalysisResult struct {
	Summary      jsonSummary        `json:"summary"`
	Tables       []jsonTableStats   `json:"tables"`
	Transactions []jsonTransaction  `json:"transactions"`
	Minutes      []jsonMinuteBucket `json:"minutes"`
	Alerts       []jsonAlert        `json:"alerts"`
	Warnings     int                `json:"warnings"`
}

type jsonSummary struct {
	TotalTransactions int    `json:"total_transactions"`
	TotalRows         int    `json:"total_rows"`
	TotalEvents       int    `json:"total_events"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
	Duration          string `json:"duration"`
}

type jsonTableStats struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	TotalRows  int    `json:"total_rows"`
	InsertRows int    `json:"insert_rows"`
	UpdateRows int    `json:"update_rows"`
	DeleteRows int    `json:"delete_rows"`
	TxnCount   int    `json:"txn_count"`
}

type jsonTransaction struct {
	TxnKey             string         `json:"txn_key"`
	StartTime        string         `json:"start_time"`
	EndTime          string         `json:"end_time"`
	Duration         string         `json:"duration"`
	TotalRows         int            `json:"total_rows"`
	EventCount       int            `json:"event_count"`
	Tables           map[string]int `json:"tables,omitempty"`
	Operations      map[string]int `json:"operations,omitempty"`
	QuerySummary     string         `json:"query_summary,omitempty"`
	QueryTruncated    bool           `json:"query_truncated,omitempty"`
	QueryOriginalBytes int            `json:"query_original_bytes,omitempty"`
}

type jsonMinuteBucket struct {
	Minute    string         `json:"minute"`
	TotalRows int            `json:"total_rows"`
	TxnCount  int            `json:"txn_count"`
	TableRows map[string]int `json:"table_rows,omitempty"`
}

type jsonAlert struct {
	Type     string         `json:"type"`
	Severity string         `json:"severity"`
	Message  string         `json:"message"`
	TxnKey   string         `json:"txn_key,omitempty"`
	Minute   string         `json:"minute,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
}

// RenderJSON serializes an AnalysisResult to JSON with stable, script-friendly field names.
func RenderJSON(result model.AnalysisResult) (string, error) {
	jr := convertToJSON(result)

	data, err := json.MarshalIndent(jr, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// RenderJSONTo writes the JSON output to the specified writer.
func RenderJSONTo(result model.AnalysisResult, w io.Writer) error {
	jr := convertToJSON(result)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(jr)
}

// RenderJSONToStdout writes the JSON output to stdout.
func RenderJSONToStdout(result model.AnalysisResult) error {
	return RenderJSONTo(result, os.Stdout)
}

func convertToJSON(result model.AnalysisResult) jsonAnalysisResult {
	return jsonAnalysisResult{
		Summary:      convertSummary(result.Summary),
		Tables:       convertTables(result.Tables),
		Transactions: convertTransactions(result.Transactions),
		Minutes:      convertMinutes(result.Minutes),
		Alerts:       convertAlerts(result.Alerts),
		Warnings:     result.Warnings,
	}
}

func convertSummary(s model.WorkloadSummary) jsonSummary {
	return jsonSummary{
		TotalTransactions: s.TotalTransactions,
		TotalRows:         s.TotalRows,
		TotalEvents:       s.TotalEvents,
		StartTime:         formatJSONTime(s.StartTime),
		EndTime:           formatJSONTime(s.EndTime),
		Duration:          s.Duration.String(),
	}
}

func convertTables(tables []model.TableStats) []jsonTableStats {
	if tables == nil {
		return []jsonTableStats{}
	}
	result := make([]jsonTableStats, len(tables))
	for i, t := range tables {
		result[i] = jsonTableStats{
			Schema:     t.Schema,
			Table:      t.Table,
			TotalRows:  t.TotalRows,
			InsertRows: t.InsertRows,
			UpdateRows: t.UpdateRows,
			DeleteRows: t.DeleteRows,
			TxnCount:   t.TxnCount,
		}
	}
	return result
}

func convertTransactions(txns []model.Transaction) []jsonTransaction {
	if txns == nil {
		return []jsonTransaction{}
	}
	result := make([]jsonTransaction, len(txns))
	for i, t := range txns {
		jt := jsonTransaction{
			TxnKey:      t.TxnKey,
			StartTime:   formatJSONTime(t.StartTime),
			EndTime:     formatJSONTime(t.EndTime),
			Duration:    t.Duration.String(),
			TotalRows:   t.TotalRows,
			EventCount:  t.EventCount,
			Tables:      copyStringIntMap(t.Tables),
			Operations:  copyStringIntMap(t.Operations),
			QuerySummary: t.QuerySummary,
		}
		if t.QueryContext != nil {
			jt.QueryTruncated = t.QueryContext.Truncated
			jt.QueryOriginalBytes = t.QueryContext.OriginalBytes
		}
		result[i] = jt
	}
	return result
}

func convertMinutes(minutes []model.MinuteBucket) []jsonMinuteBucket {
	if minutes == nil {
		return []jsonMinuteBucket{}
	}
	result := make([]jsonMinuteBucket, len(minutes))
	for i, m := range minutes {
		result[i] = jsonMinuteBucket{
			Minute:    m.Minute.Format(time.RFC3339),
			TotalRows: m.TotalRows,
			TxnCount:  m.TxnCount,
			TableRows: copyStringIntMap(m.TableRows),
		}
	}
	return result
}

func convertAlerts(alerts []model.Alert) []jsonAlert {
	if alerts == nil {
		return []jsonAlert{}
	}
	result := make([]jsonAlert, len(alerts))
	for i, a := range alerts {
		result[i] = jsonAlert{
			Type:     a.Type,
			Severity: a.Severity,
			Message:  a.Message,
			TxnKey:   a.TxnKey,
			Minute:   formatJSONTime(a.Minute),
			Details:  copyStringAnyMap(a.Details),
		}
	}
	return result
}

func copyStringIntMap(m map[string]int) map[string]int {
	if m == nil {
		return nil
	}
	result := make(map[string]int, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func copyStringAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func formatJSONTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
