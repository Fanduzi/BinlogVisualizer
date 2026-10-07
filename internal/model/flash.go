package model

const (
	// FlashProblemNames means binlog_row_metadata was not FULL, so column names are missing.
	FlashProblemNames = "names"
	// FlashProblemImage means a before- or after-image skipped columns (row image is not FULL).
	FlashProblemImage = "image"
	// FlashProblemType means one column cannot be rendered as an exact SQL literal.
	FlashProblemType = "type"
	// FlashProblemCapture means a selected row event had no flashback image.
	FlashProblemCapture = "capture"
)

// FlashCol is one TABLE_MAP column, aligned with FlashRow.Columns.
// An empty Base means the type was not available to compare.
type FlashCol struct {
	Base     string
	Unsigned bool
	HasSign  bool
	Charset  string
	Members  []string
	Prec     int
	Scale    int
	HasPrec  bool
	FSP      int
	HasFSP   bool
}

// FlashRow is one logical INSERT, UPDATE, or DELETE kept for undo SQL.
// Before and After hold SQL literals aligned with Columns. A non-empty
// ProblemKind means this row must not be rendered. Cols carries TABLE_MAP
// metadata used to check a schema file. NonStrict is set when an ENUM value
// is the error member (index 0), which strict sql_mode rejects.
type FlashRow struct {
	Schema        string
	Table         string
	Op            string
	Columns       []string
	Cols          []FlashCol
	Before        []string
	After         []string
	PK            []int
	NoPK          bool
	NonStrict     bool
	ProblemKind   string
	ProblemColumn string
	ProblemType   string
}
