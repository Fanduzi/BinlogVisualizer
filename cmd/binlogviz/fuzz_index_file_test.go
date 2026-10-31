// Run: go test -fuzz=FuzzReadIndexFilePaths -fuzztime=20s ./cmd/binlogviz
package binlogviz

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzReadIndexFilePaths(f *testing.F) {
	for _, manifest := range []string{
		"binlog.000001\nbinlog.000002\n",
		"binlog.000001",
		"./binlog.000001\n./binlog.000002\n",
		" binlog.000001 \n\tbinlog.000002\t\n",
		"binlog.000001\r\nbinlog.000002\r\n",
		"binlog.000001\n\nbinlog.000002\n\n",
		"binlog.index\n",
		"binlog.000001\nbinlog.000001\n",
		"binlog.000001\n./binlog.000001\n",
		"binlog.000001/binlog.000002\n",
		"../escape.000001\n",
		"/binlog.000001\n",
		"missing.000999\n",
		"binlog.000001\x00junk\n",
		"bínlog.000001\n",
		"\n", "",
		"a\nb\nc\nd\ne\n",
		"binlog.000001 \x00 \n",
	} {
		f.Add(manifest)
	}
	f.Fuzz(func(t *testing.T, manifest string) {
		dir := t.TempDir()
		for _, name := range []string{"binlog.000001", "binlog.000002", "mysql-bin.000010"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte{0xfe, 'b', 'i', 'n'}, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "binlog.index"), []byte(manifest), 0o600); err != nil {
			t.Fatal(err)
		}
		_, _ = readIndexFilePaths(dir, "binlog.index")
	})
}
