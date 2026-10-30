package splitout

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
)

// usageError reports whether err is the usage kind (exit 3): directory
// problems the operator fixes by choosing a different --output-dir.
func usageError(t *testing.T, err error) {
	t.Helper()
	var kerr *clierr.Error
	if !errors.As(err, &kerr) || kerr.Kind != clierr.KindUsage {
		t.Fatalf("err = %v (%T), want usage kind", err, err)
	}
}

func TestCheckDirMissingParentIsUsage(t *testing.T) {
	// A typo in a path must fail as usage, not as an output failure —
	// creating parents silently is how dirs end up in the wrong place.
	err := CheckDir(filepath.Join(t.TempDir(), "missing", "out"))
	usageError(t, err)
}

func TestCheckDirFileIsUsage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	usageError(t, CheckDir(path))
}

func TestCheckDirNonEmptyIsUsage(t *testing.T) {
	// A previous run's output must never be merged or overwritten; the
	// operator decides whether to move it aside (ADR 0025).
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "part-000001.sql"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	usageError(t, CheckDir(dir))
}

func TestCheckDirAcceptsNewAndEmpty(t *testing.T) {
	root := t.TempDir()
	if err := CheckDir(filepath.Join(root, "new-dir")); err != nil {
		t.Fatalf("new dir: %v", err)
	}
	empty := filepath.Join(root, "empty")
	if err := os.Mkdir(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := CheckDir(empty); err != nil {
		t.Fatalf("empty dir: %v", err)
	}
}

func TestCheckDirRejectsEmptyString(t *testing.T) {
	usageError(t, CheckDir(""))
}

func TestTableFileBaseEscapes(t *testing.T) {
	// The schema/table separator is a dot, so a dot inside either name must
	// escape — `a.b.c` files would otherwise be ambiguous and collision-prone.
	if got := TableFileBase("shop", "orders"); got != "shop.orders" {
		t.Fatalf("plain: %s", got)
	}
	if got := TableFileBase("my.db", "order's"); got != "my%2Edb.order%27s" {
		t.Fatalf("escaped: %s", got)
	}
	if got := TableFileBase("", "t"); got != "_.t" {
		t.Fatalf("empty schema: %s", got)
	}
}

func TestSinkPublishManifestAndFiles(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "out")
	sink, err := Open(Spec{Dir: dir, MaxStatements: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Abort()
	for i, body := range []string{"one\n", "two\n"} {
		f, err := sink.Create([]string{"a.sql", "b.sql"}[i])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(body); err != nil {
			t.Fatal(err)
		}
		if err := sink.Seal(f); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Publish(); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(manifest), "a.sql\nb.sql\n") {
		t.Fatalf("manifest:\n%s", manifest)
	}
	for _, name := range []string{"a.sql", "b.sql"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
	}
}

func TestSinkPublishIntoExistingEmptyDir(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "out")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	sink, err := Open(Spec{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Abort()
	if err := sink.WriteFile("a.sql", func(w io.Writer) error {
		_, err := w.Write([]byte("x\n"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Publish(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.sql")); err != nil {
		t.Fatalf("published file missing: %v", err)
	}
}

func TestSinkAbortLeavesNoTargetAndNoStaging(t *testing.T) {
	// A mid-run failure must leave nothing inspectable-as-finished behind:
	// no target dir, no half-written staging dir (ADR 0025).
	root := t.TempDir()
	dir := filepath.Join(root, "out")
	sink, err := Open(Spec{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteFile("a.sql", func(w io.Writer) error {
		_, err := w.Write([]byte("x\n"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	staging := sink.StagingPath()
	sink.Abort()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("target dir exists after abort: %v", err)
	}
	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Fatalf("staging dir left behind: %v", err)
	}
}

func TestSinkWriteFilePropagatesWriterError(t *testing.T) {
	root := t.TempDir()
	sink, err := Open(Spec{Dir: filepath.Join(root, "out")})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Abort()
	want := errors.New("planner refusal")
	err = sink.WriteFile("a.sql", func(w io.Writer) error {
		if _, err := w.Write([]byte("partial\n")); err != nil {
			t.Fatal(err)
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want the writer error unwrapped", err)
	}
}
