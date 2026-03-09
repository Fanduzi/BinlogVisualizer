package binlog

import (
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// parser implements Parser using go-mysql-org/go-mysql/replication.
type parser struct{}

// NewParser creates a new binlog parser.
func NewParser() Parser {
	return &parser{}
}

// ParseFiles reads binlog files and calls handler for each event.
func (p *parser) ParseFiles(paths []string, handler func(RawEvent) error) error {
	bp := replication.NewBinlogParser()

	for _, path := range paths {
		if err := bp.ParseFile(path, 0, func(ev *replication.BinlogEvent) error {
			if ev == nil {
				return nil
			}

			raw := RawEvent{
				Timestamp: time.Unix(int64(ev.Header.Timestamp), 0),
				EventType: ev.Header.EventType.String(),
				Position:  ev.Header.LogPos,
			}

			// Extract schema and table from row events
			switch e := ev.Event.(type) {
			case *replication.TableMapEvent:
				raw.Schema = string(e.Schema)
				raw.Table = string(e.Table)
			case *replication.RowsEvent:
				if e.Table != nil {
					raw.Schema = string(e.Table.Schema)
					raw.Table = string(e.Table.Table)
				}
				raw.RowCount = len(e.Rows)
			}

			return handler(raw)
		}); err != nil {
			return err
		}
	}
	return nil
}
