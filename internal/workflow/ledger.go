// input: files a workflow run or resume wrote (step artifacts and snapshots).
// output: an append-only <output_dir>/ledger.jsonl with one line per written file (path, kind, step, run id, sha256), and a reader that answers "did this workflow write exactly this file?".
// pos: per-file attribution for workflow clean (ADR 0009 §8, #289).
// note: if this file changes, update this header, ADR 0009 and module README.md.
package workflow

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"binlogviz/internal/clierr"
)

// LedgerFile is the ledger's name inside the workflow output directory.
const LedgerFile = "ledger.jsonl"

// Ledger entry kinds.
const (
	LedgerArtifact = "artifact"
	LedgerSnapshot = "snapshot"
)

// LedgerEntry records one file the workflow wrote. Path is relative to the
// output directory for artifacts and absolute for snapshots.
type LedgerEntry struct {
	Path      string `json:"path"`
	Kind      string `json:"kind"`
	Step      string `json:"step"`
	RunID     string `json:"run_id"`
	SHA256    string `json:"sha256"`
	WrittenAt string `json:"written_at"`
}

// Ledger is the decoded ledger: for each path, every sha256 the workflow wrote there.
type Ledger struct {
	Present bool
	hashes  map[string]map[string]bool
	kinds   map[string]string
	order   []string
}

// NewRunID returns a run id: UTC start time plus random hex.
func NewRunID(startedAt time.Time) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return startedAt.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b)
}

// FileSHA256 is the hex sha256 of the file at path.
func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// AppendLedger appends entries to outputDir/ledger.jsonl and syncs it. It
// never rewrites earlier lines.
func AppendLedger(outputDir string, entries []LedgerEntry) error {
	if len(entries) == 0 {
		return nil
	}
	f, err := os.OpenFile(filepath.Join(outputDir, LedgerFile), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return clierr.Wrap(clierr.KindOutput, err)
	}
	w := bufio.NewWriter(f)
	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			_ = f.Close()
			return clierr.Wrap(clierr.KindOutput, err)
		}
		_, _ = w.Write(append(line, '\n'))
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return clierr.Wrap(clierr.KindOutput, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return clierr.Wrap(clierr.KindOutput, err)
	}
	return clierr.Wrap(clierr.KindOutput, f.Close())
}

// ReadLedger loads outputDir/ledger.jsonl. A missing ledger is not an
// error: Present is false. An undecodable line is corrupt (5): a ledger that
// cannot be read in full cannot prove anything.
func ReadLedger(outputDir string) (Ledger, error) {
	l := Ledger{hashes: map[string]map[string]bool{}, kinds: map[string]string{}}
	f, err := os.Open(filepath.Join(outputDir, LedgerFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return l, nil
		}
		return l, clierr.Errorf(clierr.KindInput, "read workflow ledger: %w", err)
	}
	defer f.Close()
	l.Present = true
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	n := 0
	for sc.Scan() {
		n++
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e LedgerEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil || e.Path == "" || e.SHA256 == "" {
			return l, clierr.Reasonf(clierr.KindCorrupt, "untrusted_ledger", "workflow ledger line %d is corrupt", n)
		}
		if err := validateLedgerEntry(e); err != nil {
			return l, clierr.Reasonf(clierr.KindCorrupt, "untrusted_ledger", "workflow ledger line %d: %v", n, err)
		}
		key := ledgerKey(e.Kind, e.Path)
		if l.hashes[key] == nil {
			l.hashes[key] = map[string]bool{}
			l.order = append(l.order, key)
		}
		l.hashes[key][e.SHA256] = true
		l.kinds[key] = e.Kind
	}
	if err := sc.Err(); err != nil {
		return l, clierr.Errorf(clierr.KindInput, "read workflow ledger: %w", err)
	}
	return l, nil
}

func ledgerKey(kind, path string) string {
	return kind + "\x00" + filepath.ToSlash(filepath.Clean(path))
}

// Attributed reports whether the workflow wrote exactly these bytes at path:
// the path is in the ledger and the file's current sha256 is one it recorded.
func (l Ledger) Attributed(kind, path, sha string) bool {
	return l.hashes[ledgerKey(kind, path)][sha]
}

// validateLedgerEntry enforces the only path shapes clean will act on:
// artifacts are "<analyze|compare|trend>/<file>", snapshots are a bare
// "<file>.json" inside the configured snapshot dir. Absolute paths, "..",
// backslashes, and hidden names make the whole ledger untrusted.
func validateLedgerEntry(e LedgerEntry) error {
	if err := validateRelPath(e.Path); err != nil {
		return err
	}
	parts := strings.Split(e.Path, "/")
	switch e.Kind {
	case LedgerArtifact:
		if len(parts) != 2 || !isArtifactKind(parts[0]) {
			return errUntrustedPath(e.Path, "artifact paths must be <analyze|compare|trend>/<file>")
		}
	case LedgerSnapshot:
		if len(parts) != 1 || !strings.HasSuffix(e.Path, ".json") || e.Step != "analyze:"+strings.TrimSuffix(e.Path, ".json") {
			return errUntrustedPath(e.Path, "snapshot paths must be a bare <name>.json recorded by step analyze:<name>")
		}
	default:
		return errUntrustedPath(e.Path, "unknown kind "+strconv.Quote(e.Kind))
	}
	return nil
}

func isArtifactKind(kind string) bool {
	for _, k := range artifactKinds {
		if k == kind {
			return true
		}
	}
	return false
}

// Snapshots lists the snapshot file names (relative to the snapshot dir) the ledger records the ledger records, in first-written order.
func (l Ledger) Snapshots() []string {
	var out []string
	for _, key := range l.order {
		if l.kinds[key] == LedgerSnapshot {
			out = append(out, key[len(LedgerSnapshot)+1:])
		}
	}
	return out
}

// LedgerEntriesForRun builds entries for the files this run's executed steps
// wrote. Reused steps were recorded by the run that wrote them. Snapshots are
// recorded only for successful analyze steps that saved one.
func LedgerEntriesForRun(outputDir string, m Manifest, runID string, now time.Time) ([]LedgerEntry, []error) {
	var entries []LedgerEntry
	var errs []error
	ts := now.UTC().Format(time.RFC3339)
	for _, step := range m.Steps {
		if step.Execution != "executed" {
			continue
		}
		stepID := step.Kind + ":" + step.Name
		for _, rel := range step.Artifacts {
			sum, err := FileSHA256(filepath.Join(outputDir, filepath.FromSlash(rel)))
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					errs = append(errs, err)
				}
				continue
			}
			entries = append(entries, LedgerEntry{Path: filepath.ToSlash(filepath.Clean(rel)), Kind: LedgerArtifact, Step: stepID, RunID: runID, SHA256: sum, WrittenAt: ts})
		}
		if step.Kind == "analyze" && step.Status == "success" && step.SnapshotName != "" && m.SnapshotDir != "" {
			name := step.SnapshotName + ".json"
			if validateRelPath(name) != nil || strings.Contains(name, "/") {
				errs = append(errs, clierr.Errorf(clierr.KindInput, "snapshot name %q is not a plain file name", step.SnapshotName))
				continue
			}
			abs := filepath.Join(m.SnapshotDir, name)
			sum, err := FileSHA256(abs)
			if err != nil {
				errs = append(errs, clierr.Errorf(clierr.KindInput, "hash snapshot %s: %w", abs, err))
				continue
			}
			entries = append(entries, LedgerEntry{Path: name, Kind: LedgerSnapshot, Step: stepID, RunID: runID, SHA256: sum, WrittenAt: ts})
		}
	}
	return entries, errs
}
