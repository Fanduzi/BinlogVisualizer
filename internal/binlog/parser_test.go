package binlog

import "testing"

func TestNewParserReturnsImplementation(t *testing.T) {
	if NewParser() == nil {
		t.Fatal("expected parser")
	}
}
