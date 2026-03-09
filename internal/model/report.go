package model

import "time"

// WorkloadSummary provides high-level analysis statistics.
type WorkloadSummary struct {
	TotalTransactions int
	TotalRows         int
	TotalEvents       int
	StartTime         time.Time
	EndTime           time.Time
	Duration          time.Duration
}

// Alert represents a detected anomaly or threshold breach.
type Alert struct {
	Type        string // "large_transaction", "spike"
	Severity    string // "info", "warning", "critical"
	Message     string
	TxnKey      string
	Minute      time.Time
	Details     map[string]int
}

// AnalysisResult is the complete output of binlog analysis.
type AnalysisResult struct {
	Summary    WorkloadSummary
	Tables     []TableStats
	Txn        []Transaction
	Minutes    []MinuteBucket
	Alerts     []Alert
	Warnings   int
}
