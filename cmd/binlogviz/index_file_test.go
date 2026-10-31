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
	// A sibling of --from-dir that traversal and absolute entries point at.
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "mysql-bin.000001")
	if err := os.WriteFile(outsidePath, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		opts    analyzeOptions
		args    []string
		index   string
		setup   func(t *testing.T)
		wantErr string
	}{
		{name: "index without from-dir", opts: analyzeOptions{indexFile: "binlog.index"}, wantErr: "--from-dir"},
		{name: "index with prefix", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index", prefix: "mysql-bin."}, wantErr: "--prefix or --index-file"},
		{name: "index with args", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, args: []string{"mysql-bin.000001"}, wantErr: "cannot combine"},
		{name: "index unreadable", opts: analyzeOptions{fromDir: dir, indexFile: "missing.index"}, wantErr: "cannot read binlog index"},
		{name: "index entry missing", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "./mysql-bin.000001\n./mysql-bin.000099\n", wantErr: "mysql-bin.000099"},
		{name: "index empty", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "\n\n", wantErr: "no binlog files"},
		// ADR 0028: each malformed-manifest shape refuses with its own reason.
		{name: "traversal escape", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: filepath.Join("..", filepath.Base(outside), "mysql-bin.000001") + "\n", wantErr: "resolves outside"},
		{name: "absolute escape", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: outsidePath + "\n", wantErr: "resolves outside"},
		{name: "nested traversal escape", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "sub/../../" + filepath.Base(outside) + "/mysql-bin.000001\n", wantErr: "resolves outside"},
		{name: "index lists itself", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "mysql-bin.000001\n./binlog.index\n", wantErr: "lists itself"},
		{name: "duplicate entry spellings", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "mysql-bin.000001\n./mysql-bin.000001\n", wantErr: "already listed"},
		{name: "directory entry", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "mysql-bin.000001\nlogs\n",
			setup: func(t *testing.T) {
				if err := os.Mkdir(filepath.Join(dir, "logs"), 0o755); err != nil {
					t.Fatal(err)
				}
			}, wantErr: "missing or not a file"},
		{name: "symlink escape", opts: analyzeOptions{fromDir: dir, indexFile: "binlog.index"}, index: "mysql-bin.000002\n",
			setup: func(t *testing.T) {
				if err := os.Symlink(outsidePath, filepath.Join(dir, "mysql-bin.000002")); err != nil {
					t.Skipf("symlinks unsupported: %v", err)
				}
			}, wantErr: "resolves outside"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				tc.setup(t)
			}
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

// ADR 0028: the manifest shapes a real binlog.index produces stay accepted —
// CRLF endings, ./ prefixes, absolute paths inside --from-dir, and in-dir
// symlinks whose targets stay in-dir.
func TestResolveAnalyzePathsIndexFileAcceptsServerShapes(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000009"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// mysql-bin.000003 aliases the unlisted mysql-bin.000009: an in-dir
	// symlink to an in-dir target is a distinct file, not a duplicate.
	if err := os.Symlink("mysql-bin.000009", filepath.Join(dir, "mysql-bin.000003")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	index := "./mysql-bin.000001\r\n" +
		filepath.Join(dir, "mysql-bin.000002") + "\r\n" + // absolute inside --from-dir
		"mysql-bin.000003\n" + // in-dir symlink to an in-dir target
		"\n"
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
	for i, name := range []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"} {
		if filepath.Base(paths[i]) != name {
			t.Fatalf("paths[%d] = %v, want %s", i, paths[i], name)
		}
	}
}
