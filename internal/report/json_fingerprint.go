// Package report renders the statement fingerprint ranking for JSON reports.
// input: model.StatementFingerprint groups from the analyzer plus the SQL
// context mode.
// output: the statement_fingerprints array entries: fingerprint text, bounded
// sample (omitted under --sql-context off), statement/row counts, row share,
// and per-table row attribution (ADR 0026).
// pos: JSON projection of the fingerprint ranking beside patterns in json.go.
// note: if this file changes, update this header and module README.md.
package report

import "binlogviz/internal/model"

type jsonFingerprint struct {
	Fingerprint string         `json:"fingerprint"`
	Sample      string         `json:"sample,omitempty"`
	Statements  int            `json:"statements"`
	Rows        int            `json:"rows"`
	ShareOfRows float64        `json:"share_of_rows"`
	Tables      map[string]int `json:"tables,omitempty"`
}

func convertFingerprints(fps []model.StatementFingerprint, mode SQLContextMode) []jsonFingerprint {
	if fps == nil {
		return []jsonFingerprint{}
	}
	result := make([]jsonFingerprint, len(fps))
	for i, f := range fps {
		result[i] = jsonFingerprint{
			Fingerprint: f.Fingerprint,
			Statements:  f.Statements,
			Rows:        f.Rows,
			ShareOfRows: f.ShareOfRows,
			Tables:      copyStringIntMap(f.Tables),
		}
		if mode != SQLContextOff {
			result[i].Sample = f.Sample
		}
	}
	return result
}
