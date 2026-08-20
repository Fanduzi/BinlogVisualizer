package binlog

import "testing"

func TestIsQueryDMLDetectsCommonDML(t *testing.T) {
	for _, query := range []string{
		"INSERT INTO users VALUES (1)",
		"update users set name='x'",
		"DELETE FROM users WHERE id=1",
		"REPLACE INTO users VALUES (1)",
		"INSERT(users)",
	} {
		if !IsQueryDML(query) {
			t.Fatalf("expected Query-DML for %q", query)
		}
	}
}

func TestIsQueryDMLIgnoresBeginCommitAndDDL(t *testing.T) {
	for _, query := range []string{
		"BEGIN",
		"COMMIT",
		"CREATE TABLE users (id INT)",
		"ALTER TABLE users ADD COLUMN n INT",
		"",
	} {
		if IsQueryDML(query) {
			t.Fatalf("did not expect Query-DML for %q", query)
		}
	}
}

func TestGuessInputFormat(t *testing.T) {
	cases := []struct {
		queryDML, rowImages int
		want                string
	}{
		{0, 3, InputFormatROW},
		{5, 0, InputFormatStatement},
		{5, 1, InputFormatMixed},
		{0, 0, ""},
	}
	for _, tc := range cases {
		if got := GuessInputFormat(tc.queryDML, tc.rowImages); got != tc.want {
			t.Fatalf("GuessInputFormat(%d,%d)=%q, want %q", tc.queryDML, tc.rowImages, got, tc.want)
		}
	}
}

func TestFormatObserverCountsQueryDMLAndRowImages(t *testing.T) {
	var observer FormatObserver
	observer.Observe(RawEvent{EventType: "QueryEvent", Query: "INSERT INTO users VALUES (1)"})
	observer.Observe(RawEvent{EventType: "QUERY_EVENT", Query: "UPDATE users SET name='x'"})
	observer.Observe(RawEvent{EventType: "QUERY_EVENT", Query: "BEGIN"})
	observer.Observe(RawEvent{EventType: "UpdateRowsEventV2", RowCount: 1})
	observer.Observe(RawEvent{EventType: "RowsQueryEvent", QuerySQL: "UPDATE users SET name='x'"})

	if observer.QueryDMLEvents != 2 {
		t.Fatalf("QueryDMLEvents=%d, want 2", observer.QueryDMLEvents)
	}
	if observer.RowImageEvents != 1 {
		t.Fatalf("RowImageEvents=%d, want 1", observer.RowImageEvents)
	}
	if observer.Guess() != InputFormatMixed {
		t.Fatalf("guess=%q, want MIXED", observer.Guess())
	}
}
