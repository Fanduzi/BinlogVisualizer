package binlog

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// Every collation MariaDB 11.4 ships maps to its own charset (#269).
func TestCollationCharsetCoversMariaDB114(t *testing.T) {
	f, err := os.Open("testdata/mariadb-11.4-collations.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		fields := strings.Fields(line)
		id, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			t.Fatalf("bad line %q", line)
		}
		want := fields[1]
		if want == "binary" {
			continue
		}
		got, ok := charsetOfCollation(id)
		if !ok || got != want {
			t.Errorf("collation %d: got %q (known=%v), want %q", id, got, ok, want)
		}
		n++
	}
	if n < 1200 {
		t.Fatalf("read only %d collations", n)
	}
}

func TestCollationCharsetRefusesIdsNoServerUses(t *testing.T) {
	for _, id := range []uint64{0, 99999, 400, 1023, 1024, 1024 + 255, 3328, 4095} {
		if cs, ok := charsetOfCollation(id); ok {
			t.Errorf("collation %d: got %q, want unknown", id, cs)
		}
	}
}

// MariaDB 11.4's default utf8mb4_uca1400_ai_ci (2304) renders as a plain string.
func TestSQLCharacterMariaDBDefaultCollation(t *testing.T) {
	lit, problem := sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte("雪"), map[int]uint64{0: 2304}, 0)
	if problem != "" || lit != "'雪'" {
		t.Fatalf("uca1400: %s %s", lit, problem)
	}
	lit, problem = sqlCharacter(mysql.MYSQL_TYPE_VARCHAR, 0, []byte{0xE9}, map[int]uint64{0: 1032}, 0)
	if problem != "" || lit != "_latin1 0xE9" {
		t.Fatalf("latin1 nopad: %s %s", lit, problem)
	}
}
