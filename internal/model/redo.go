package model

// RedoRow is one logical INSERT, UPDATE, or DELETE kept for forward SQL.
// Unlike FlashRow it keeps the raw decoded images: names and type facts are
// resolved later — from FULL table-map metadata or merged with a schema
// definition under binlog_row_metadata=MINIMAL (ADR 0022) — so the shared
// literal emitter renders at write time, not capture time. BeforeDocs and
// AfterDocs carry the binary JSON documents aligned with each image; a nil
// cell means the column is not JSON or its bytes were not captured. A
// non-empty ProblemKind means this row must not be rendered.
type RedoRow struct {
	Schema        string
	Table         string
	Op            string
	Columns       []string
	Names         string // RowNamesFull or RowNamesPositional
	Cols          []FlashCol
	Before        []any
	After         []any
	BeforeDocs    [][]byte
	AfterDocs     [][]byte
	PK            []int
	NoPK          bool
	NonStrict     bool
	ZeroDate      bool
	ProblemKind   string
	ProblemColumn string
	ProblemType   string
}
