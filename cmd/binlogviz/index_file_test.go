// Package binlogviz pins --from-dir --index-file discovery behavior.
// input: temp dirs with binlog files and binlog.index manifests in various shapes.
// output: resolved path order, and usage/input errors for each invalid combination.
// pos: regression coverage for the server index-file discovery mode.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveAnalyzePathsIndexFileOrder(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000010"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The index order wins over lexicographic or numeric suffix order.
	index := "./mysql-bin.000010\n./mysql-bin.000002\nmysql-bin.000001\n\n"
	if err := os.WriteFile(filepath.Join(dir, "binlog.index"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	paths, discovery, _, err := resolveAnalyzePaths(nil, &analyzeOptions{fromDir: dir, indexFile: "binlog.index"})
	if err != nil {
		t.Fatalf("resolveAnalyzePaths: %v", err)
	}
	if !discovery || len(paths) != 3 {
		t.Fatalf("paths=%v discovery=%v", paths, discovery)
	}
	for i, name := range []string{"mysql-bin.000010", "mysql-bin.000002", "mysql-bin.000001"} {
		if filepath.Base(paths[i]) != name {
			t.Fatalf("paths[%d] = %v, want %s", i, paths[i], name)
		}
	}
}

func TestResolveAnalyzePathsIndexFileFailures(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		opts    analyzeOptions
		args    []string
		index   string
		wantErr string
	}{
		{name: "index without from-dir", opts: analyzeOptions{indexFile: "binlog.index"}, wantErr: "--from-dir"},
		{name: "index with prefix", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index", prefix: "mysql-bin."}, wantErr: "--prefix or --index-file"},
		{name: "index with args", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, args: []string{"mysql-bin.000001"}, wantErr: "cannot combine"},
		{name: "index unreadable", opts: analyzeOptions{fromDir: dir, indexFile: "missing.index"}, wantErr: "cannot read binlog index"},
		{name: "index entry missing", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "./mysql-bin.000001\n./mysql-bin.000099\n", wantErr: "mysql-bin.000099"},
		{name: "index empty", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "\n\n", wantErr: "no binlog files"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.index != "" {
				if err := os.WriteFile(filepath.Join(dir, "binlog.index"), []byte(tc.index), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			_, _, _, err := resolveAnalyzePaths(tc.args, &tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
