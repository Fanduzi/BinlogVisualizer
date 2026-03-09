package model

import "time"

// Transaction represents a reconstructed database transaction.
type Transaction struct {
	TxnKey     string
	StartTime  time.Time
	EndTime    time.Time
	Duration   time.Duration
	TotalRows  int
	EventCount int
	Tables     map[string]int
	Operations map[string]int
}
