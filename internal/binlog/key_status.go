package binlog

import (
	"github.com/go-mysql-org/go-mysql/replication"

	"binlogviz/internal/model"
)

// tableKeyStatus reads MySQL 8 FULL TABLE_MAP optional metadata.
// Column names are the FULL signal. A primary-key field (SIMPLE_PRIMARY_KEY
// or PRIMARY_KEY_WITH_PREFIX) means the table has a key. FULL metadata with
// no primary-key field means the table has none. Anything else is unknown.
func tableKeyStatus(table *replication.TableMapEvent) string {
	if table == nil || table.ColumnCount == 0 || len(table.ColumnName) != int(table.ColumnCount) {
		return model.KeyStatusUnknown
	}
	if len(table.PrimaryKey) > 0 {
		return model.KeyStatusHasPK
	}
	return model.KeyStatusNoPK
}

func rowsKeyStatus(event *replication.RowsEvent, tableNames map[uint64]cachedTableName) string {
	if event == nil {
		return model.KeyStatusUnknown
	}
	if event.Table != nil {
		return tableKeyStatus(event.Table)
	}
	if tableNames != nil {
		if name, ok := tableNames[event.TableID]; ok && name.keyStatus != "" {
			return name.keyStatus
		}
	}
	return model.KeyStatusUnknown
}
