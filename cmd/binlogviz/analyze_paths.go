// Package binlogviz resolves analyze's input paths: explicit files, directory
// discovery with a shared prefix, position/time windows, and readability.
// input: cobra args plus --from-dir/--prefix/time flags.
// output: sorted binlog paths, file coverage, or a usage error naming the miss.
// pos: CLI orchestration helper for analyze.go.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

func validateAnalyzeSelectionInput(args []string, opts *analyzeOptions) error {
	if opts == nil || (!opts.startPositionSet && !opts.stopPositionSet) {
		return nil
	}
	if opts.fromDir != "" || opts.prefix != "" || len(args) != 1 {
		return clierr.Errorf(clierr.KindUsage, "position selectors require exactly one explicit binlog file")
	}
	if opts.startPositionSet && opts.startPosition < 0 {
		return clierr.Errorf(clierr.KindUsage, "--start-position must not be negative")
	}
	if opts.stopPositionSet && opts.stopPosition < 0 {
		return clierr.Errorf(clierr.KindUsage, "--stop-position must not be negative")
	}
	if opts.startPositionSet && opts.stopPositionSet && opts.stopPosition <= opts.startPosition {
		return clierr.Errorf(clierr.KindUsage, "--stop-position must be greater than --start-position")
	}
	return nil
}

func resolveAnalyzePaths(args []string, opts *analyzeOptions) ([]string, bool, model.FileCoverage, error) {
	hasArgs := len(args) > 0
	hasFromDir := opts.fromDir != ""
	hasPrefix := opts.prefix != ""
	hasIndex := opts.indexFile != ""

	if hasArgs && (hasFromDir || hasPrefix || hasIndex) {
		return nil, false, model.FileCoverage{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.combineArgsWithDir"))
	}
	if (hasFromDir || hasPrefix || hasIndex) && (!hasFromDir || hasPrefix == hasIndex) {
		return nil, false, model.FileCoverage{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.fromDirPrefixOrIndex"))
	}
	if hasArgs {
		return args, false, fileCoverageForPaths(args), nil
	}
	if hasIndex {
		paths, err := readIndexFilePaths(opts.fromDir, opts.indexFile)
		if err != nil {
			return nil, false, model.FileCoverage{}, err
		}
		return paths, true, fileCoverageForPaths(paths), nil
	}
	if hasFromDir {
		startTime, endTime, err := parseTimeRange(opts.startTime, opts.endTime, opts.display)
		if err != nil {
			return nil, false, model.FileCoverage{}, err
		}
		plan, err := discoverBinlogPlanInWindow(opts.fromDir, opts.prefix, startTime, endTime)
		if err != nil {
			return nil, false, model.FileCoverage{}, err
		}
		return plan.Paths, true, plan.FileCoverage, nil
	}
	return nil, false, model.FileCoverage{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.requiresBinlogOrDir"))
}

// readIndexFilePaths reads a server's binlog.index manifest: one entry per line
// in write order. Relative entries resolve against --from-dir; absolute entries
// are allowed only inside it. Every entry must be a distinct existing file
// inside --from-dir: escapes (.., absolute-to-elsewhere, symlink targets),
// self-listing, and duplicates refuse instead of clamping or double-reading
// (ADR 0028). A rotated-away entry fails instead of silently dropping part of
// the history.
func readIndexFilePaths(dir, indexName string) ([]string, error) {
	indexPath := indexName
	if !filepath.IsAbs(indexPath) {
		indexPath = filepath.Join(dir, indexName)
	}
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.readIndexFile", map[string]any{"Path": indexPath, "Error": err.Error()}))
	}
	indexInfo, err := os.Stat(indexPath)
	if err != nil {
		return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.readIndexFile", map[string]any{"Path": indexPath, "Error": err.Error()}))
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.readBinlogDir", map[string]any{"Error": err.Error()}))
	}
	evalDir, err := filepath.EvalSymlinks(absDir)
	if err != nil {
		return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.readBinlogDir", map[string]any{"Error": err.Error()}))
	}
	var paths []string
	var seen []indexEntry
	for _, line := range strings.Split(string(data), "\n") {
		entry := strings.TrimSpace(line)
		if entry == "" {
			continue
		}
		path := entry
		if !filepath.IsAbs(path) {
			path = filepath.Join(dir, entry)
		}
		// Containment is judged twice: once lexically (a ../ or absolute
		// escape names itself even when nothing exists there) and once on
		// the resolved target (an in-dir symlink is the same escape one
		// indirection down).
		if resolvedOutside(absDir, path) {
			return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.indexFileOutside", map[string]any{"Index": indexPath, "Entry": entry}))
		}
		info, statErr := os.Stat(path)
		if statErr != nil || info.IsDir() {
			return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.indexFileEntry", map[string]any{"Index": indexPath, "Entry": entry}))
		}
		if os.SameFile(indexInfo, info) {
			return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.indexFileSelf", map[string]any{"Index": indexPath, "Entry": entry}))
		}
		for _, prior := range seen {
			if os.SameFile(prior.info, info) {
				return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.indexFileDuplicate", map[string]any{"Index": indexPath, "Entry": entry, "First": prior.entry}))
			}
		}
		evalPath, evalErr := filepath.EvalSymlinks(path)
		if evalErr != nil {
			return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.indexFileEntry", map[string]any{"Index": indexPath, "Entry": entry}))
		}
		if resolvedOutside(evalDir, evalPath) {
			return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.indexFileOutside", map[string]any{"Index": indexPath, "Entry": entry}))
		}
		paths = append(paths, path)
		seen = append(seen, indexEntry{entry: entry, info: info})
	}
	if len(paths) == 0 {
		return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.indexFileEmpty", map[string]any{"Index": indexPath}))
	}
	return paths, nil
}

// indexEntry is one accepted manifest entry, kept so a later spelling of the
// same file can be refused by identity rather than by name.
type indexEntry struct {
	entry string
	info  os.FileInfo
}

// resolvedOutside reports whether path escapes dir after Rel resolution.
// Both arguments are absolute forms by the caller.
func resolvedOutside(dir, path string) bool {
	rel, err := filepath.Rel(dir, filepath.Clean(path))
	if err != nil {
		return true
	}
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func fileCoverageForPaths(paths []string) model.FileCoverage {
	selected := make([]model.FileCoverageItem, 0, len(paths))
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		selected = append(selected, model.FileCoverageItem{
			BinlogPath: path,
			Reason:     "selected",
			Size:       info.Size(),
		})
	}
	return analyzer.BuildFileCoverage(selected, nil)
}

func discoverBinlogPaths(dir, prefix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.readBinlogDir", map[string]any{"Error": err.Error()}))
	}

	candidates := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		if _, ok := binlogNumericSuffix(entry.Name(), prefix); !ok {
			continue
		}
		candidates = append(candidates, filepath.Join(dir, entry.Name()))
	}

	sortBinlogPaths(candidates, prefix)
	if len(candidates) == 0 {
		return nil, clierr.Errorf(clierr.KindInput, "%s", noMatchingBinlogFilesError(dir, prefix))
	}
	return candidates, nil
}

func discoverBinlogPathsInWindow(dir, prefix string, startTime, endTime time.Time) ([]string, error) {
	plan, err := discoverBinlogPlanInWindow(dir, prefix, startTime, endTime)
	if err != nil {
		return nil, err
	}
	return plan.Paths, nil
}

func discoverBinlogPlanInWindow(dir, prefix string, startTime, endTime time.Time) (analyzePlan, error) {
	paths, err := discoverBinlogPaths(dir, prefix)
	if err != nil {
		return analyzePlan{}, err
	}
	coarsePaths, err := coarseFilterPathsByModTime(paths, startTime, endTime)
	if err != nil {
		return analyzePlan{}, err
	}
	if len(coarsePaths) == 0 {
		return analyzePlan{Paths: coarsePaths, WorkerCount: 1}, nil
	}

	workers := defaultAnalyzeProbeWorkers(len(coarsePaths))
	probes, err := probeAnalyzePaths(coarsePaths, workers)
	if err != nil {
		return analyzePlan{}, err
	}
	return buildAnalyzePlan(probes, startTime, endTime, workers), nil
}

func sortBinlogPaths(paths []string, prefix string) {
	sort.SliceStable(paths, func(i, j int) bool {
		leftBase := filepath.Base(paths[i])
		rightBase := filepath.Base(paths[j])
		leftSuffix, leftOK := binlogNumericSuffix(leftBase, prefix)
		rightSuffix, rightOK := binlogNumericSuffix(rightBase, prefix)
		if leftOK && rightOK {
			leftValue, leftErr := strconv.ParseInt(leftSuffix, 10, 64)
			rightValue, rightErr := strconv.ParseInt(rightSuffix, 10, 64)
			if leftErr == nil && rightErr == nil && leftValue != rightValue {
				return leftValue < rightValue
			}
		}
		return leftBase < rightBase
	})
}

// binlogNumericSuffix returns the numeric sequence after prefix.
// An optional '.' between the prefix and the digits is accepted so
// --prefix mysql-bin matches mysql-bin.000008 (log_bin_basename style).
// A prefix that equals the filename selects that file only.
func binlogNumericSuffix(name, prefix string) (string, bool) {
	if name == prefix {
		return "0", true
	}
	if !strings.HasPrefix(name, prefix) {
		return "", false
	}
	suffix := strings.TrimPrefix(name, prefix)
	if suffix == "" {
		return "", false
	}
	if suffix[0] == '.' {
		suffix = suffix[1:]
	}
	if !isDigits(suffix) {
		return "", false
	}
	return suffix, true
}

func isDigits(value string) bool {
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return value != ""
}

func noMatchingBinlogFilesError(dir, prefix string) string {
	msg := i18n.Tf("error.noMatchingFiles", map[string]any{"Dir": dir, "Prefix": prefix})
	if shared := sharedBinlogPrefix(prefix); shared != "" {
		msg += "; " + i18n.Tf("error.prefixExactFileHint", map[string]any{
			"Prefix": shared,
			"Path":   filepath.Join(dir, prefix),
		})
		return msg
	}
	if prefix != "" && !strings.HasSuffix(prefix, ".") {
		msg += "; " + i18n.Tf("error.prefixDotHint", map[string]any{"Prefix": prefix + "."})
	}
	return msg
}

// sharedBinlogPrefix strips a trailing numeric sequence so mysql-bin.000008
// yields mysql-bin. when the operator passed a complete filename as --prefix.
func sharedBinlogPrefix(prefix string) string {
	end := len(prefix)
	for end > 0 && prefix[end-1] >= '0' && prefix[end-1] <= '9' {
		end--
	}
	if end == 0 || end == len(prefix) {
		return ""
	}
	return prefix[:end]
}

func readFirstBinlogTimestamp(path string) (time.Time, error) {
	probe, err := binlog.ProbeFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return probe.FirstEventAt, nil
}

func printResolvedPaths(out io.Writer, paths []string) {
	_, _ = fmt.Fprintln(out, i18n.T("progress.resolvedFiles"))
	for _, path := range paths {
		_, _ = fmt.Fprintf(out, "- %s\n", path)
	}
}

// validateFiles checks that every input is a file this process can open.
// A missing, unreadable or directory path is an input error (exit 4), found
// here before the parser would report it as a corrupt binlog (#268).
func validateFiles(paths []string) error {
	for _, path := range paths {
		if err := checkReadableFile(path); err != nil {
			if os.IsNotExist(err) {
				return clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.fileNotFound", map[string]any{"Path": path}))
			}
			return clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.cannotAccessFile", map[string]any{"Path": path, "Error": err.Error()}))
		}
	}
	return nil
}

var errIsDirectory = errors.New("is a directory")

func checkReadableFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return &fs.PathError{Op: "read", Path: path, Err: errIsDirectory}
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return f.Close()
}
