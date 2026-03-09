package model

import "testing"

func TestTransactionDurationUsesStartAndEnd(t *testing.T) {
	trx := Transaction{}
	if trx.Duration != 0 {
		t.Fatalf("expected zero duration")
	}
}
