// Package splitout publishes a directory of split SQL output files as one
// staged unit (ADR 0025).
// input: a validated Spec (target directory and mode) plus the file stream
// the flashback and redo writers produce.
// output: the target directory — created by rename only after every file is
// fsynced and MANIFEST written — or nothing at all.
// pos: shared output-directory staging for internal/flashback and
// internal/redo; it knows file names and ordering, not SQL.
// note: if this file changes, update this header and module README.md.
package splitout

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
)

// ManifestName lists the files in apply order inside the output directory.
const ManifestName = "MANIFEST"

// Spec selects how one run's SQL output is partitioned into files.
type Spec struct {
	// Dir is the output directory the sink publishes into.
	Dir string
	// PerTable partitions by (schema, table); flashback only (ADR 0025).
	PerTable bool
	// MaxStatements bounds the statements per file; 0 means unlimited.
	MaxStatements int64
}

// CheckDir validates the publish target before any work: dir must be absent
// or an existing empty directory, and its parent must exist.
func CheckDir(dir string) error {
	if dir == "" {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.splitNeedsOutputDir"))
	}
	info, err := os.Stat(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return clierr.Wrap(clierr.KindOutput, err)
		}
		parent := filepath.Dir(dir)
		pinfo, perr := os.Stat(parent)
		if perr != nil || !pinfo.IsDir() {
			return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.splitDirParent", map[string]any{"Dir": dir}))
		}
		return nil
	}
	if !info.IsDir() {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.splitDirNotDir", map[string]any{"Dir": dir}))
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return clierr.Wrap(clierr.KindOutput, err)
	}
	if len(entries) > 0 {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.splitDirNotEmpty", map[string]any{"Dir": dir}))
	}
	return nil
}

// TableFileBase renders schema.table as a deterministic filesystem-safe
// base name. Bytes outside [A-Za-z0-9_-] become %XX — a dot inside either
// name can never collide with the separator.
func TableFileBase(schema, table string) string {
	if schema == "" {
		schema = "_"
	}
	if table == "" {
		table = "_"
	}
	return escName(schema) + "." + escName(table)
}

func escName(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' || c == '-' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// Sink stages output files next to the target directory and publishes them
// as one rename. Nothing reaches the target before Publish.
type Sink struct {
	dir     string
	staging string
	files   []string
	done    bool
}

// Open creates the staging directory next to spec.Dir. Call Abort on any
// failure before Publish; Publish makes Abort a no-op.
func Open(spec Spec) (*Sink, error) {
	if err := CheckDir(spec.Dir); err != nil {
		return nil, err
	}
	staging, err := os.MkdirTemp(filepath.Dir(spec.Dir), ".binlogviz-split-*")
	if err != nil {
		return nil, clierr.Wrap(clierr.KindOutput, err)
	}
	return &Sink{dir: spec.Dir, staging: staging}, nil
}

// StagingPath is the staging directory, for diagnostics and tests.
func (s *Sink) StagingPath() string { return s.staging }

// Create opens name inside the staging dir and records it in manifest
// order. The caller closes it with Seal.
func (s *Sink) Create(name string) (*os.File, error) {
	s.files = append(s.files, name)
	f, err := os.Create(filepath.Join(s.staging, name))
	if err != nil {
		return nil, clierr.Wrap(clierr.KindOutput, err)
	}
	return f, nil
}

// Seal fsyncs and closes a file opened by Create.
func (s *Sink) Seal(f *os.File) error {
	err := f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return clierr.Wrap(clierr.KindOutput, err)
}

// WriteFile streams fn(w) into a fresh file, then seals it. fn's own errors
// (planner refusals) propagate unwrapped.
func (s *Sink) WriteFile(name string, fn func(w io.Writer) error) error {
	f, err := s.Create(name)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	err = fn(bw)
	if err == nil {
		err = bw.Flush()
	}
	if err == nil {
		return s.Seal(f)
	}
	_ = f.Close()
	return err
}

// Manifest is the apply order: file creation order.
func (s *Sink) Manifest() []string {
	return append([]string(nil), s.files...)
}

// Publish writes MANIFEST last and renames the staging directory onto the
// target — the file set appears atomically.
func (s *Sink) Publish() error {
	var b strings.Builder
	b.WriteString("# binlogviz split output — apply files in the order listed\n")
	for _, name := range s.files {
		b.WriteString(name)
		b.WriteByte('\n')
	}
	manifest := filepath.Join(s.staging, ManifestName)
	if err := os.WriteFile(manifest, []byte(b.String()), 0o644); err != nil {
		return clierr.Wrap(clierr.KindOutput, err)
	}
	if f, err := os.Open(manifest); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if info, err := os.Stat(s.dir); err == nil && info.IsDir() {
		// The target pre-exists only when CheckDir saw it empty.
		if err := os.Remove(s.dir); err != nil {
			return clierr.Wrap(clierr.KindOutput, err)
		}
	}
	if err := os.Rename(s.staging, s.dir); err != nil {
		return clierr.Wrap(clierr.KindOutput, err)
	}
	s.done = true
	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Abort removes the staging directory when Publish has not run.
func (s *Sink) Abort() {
	if s == nil || s.done {
		return
	}
	s.done = true
	_ = os.RemoveAll(s.staging)
}
