package workflow

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var exportArchiveTimestamp = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// ExportOptions controls optional workflow bundle export behavior.
type ExportOptions struct {
	IncludeSnapshots bool
}

// ExportResult is the deterministic, read-only workflow bundle representation.
type ExportResult struct {
	OutputDir string
	Root      string
	Warnings  []string
	Entries   []ExportEntry
}

// ExportEntry is one file that will be written into the export archive.
type ExportEntry struct {
	ArchivePath string
	SourcePath  string
	Body        []byte
}

// BuildExport assembles a read-only export bundle from manifest-declared files.
func BuildExport(outputDir string, manifest Manifest, opts ExportOptions) (ExportResult, error) {
	root := filepath.Base(filepath.Clean(outputDir))
	result := ExportResult{
		OutputDir: outputDir,
		Root:      root,
		Warnings:  []string{},
		Entries:   []ExportEntry{},
	}

	manifestPath := filepath.Join(outputDir, "manifest.json")
	insideManifest, err := pathWithinRoot(manifestPath, outputDir)
	if err != nil {
		return ExportResult{}, fmt.Errorf("validate manifest.json: %w", err)
	}
	if !insideManifest {
		return ExportResult{}, fmt.Errorf("manifest.json resolves outside workflow root")
	}
	manifestEntry, err := readExportEntry(root, manifestPath, "manifest.json")
	if err != nil {
		return ExportResult{}, fmt.Errorf("read manifest.json: %w", err)
	}
	result.Entries = append(result.Entries, manifestEntry)

	indexPath := filepath.Join(outputDir, "index.html")
	insideIndex, err := pathWithinRoot(indexPath, outputDir)
	if err != nil {
		return ExportResult{}, fmt.Errorf("validate index.html: %w", err)
	}
	if !insideIndex {
		result.Warnings = append(result.Warnings, "skipping index.html because it resolves outside workflow root")
	} else {
		indexEntry, err := readExportEntry(root, indexPath, "index.html")
		if err != nil {
			if os.IsNotExist(err) {
				result.Warnings = append(result.Warnings, "missing index.html")
			} else {
				result.Warnings = append(result.Warnings, fmt.Sprintf("missing index.html: %v", err))
			}
		} else {
			result.Entries = append(result.Entries, indexEntry)
		}
	}

	for _, step := range manifest.Steps {
		for _, artifact := range step.Artifacts {
			normalizedArtifact, ok := normalizeExportRelativePath(artifact)
			if !ok {
				result.Warnings = append(result.Warnings, "skipping manifest artifact with non-normalized archive path: "+filepath.ToSlash(artifact))
				continue
			}
			sourcePath := filepath.Join(outputDir, filepath.FromSlash(normalizedArtifact))
			inside, err := pathWithinRoot(sourcePath, outputDir)
			if err != nil {
				return ExportResult{}, fmt.Errorf("validate artifact %q: %w", artifact, err)
			}
			if !inside {
				result.Warnings = append(result.Warnings, "skipping manifest artifact outside workflow root: "+filepath.ToSlash(artifact))
				continue
			}
			entry, err := readExportEntry(root, sourcePath, normalizedArtifact)
			if err != nil {
				if os.IsNotExist(err) {
					result.Warnings = append(result.Warnings, "missing manifest artifact: "+filepath.ToSlash(artifact))
					continue
				}
				return ExportResult{}, fmt.Errorf("read artifact %q: %w", artifact, err)
			}
			result.Entries = append(result.Entries, entry)
		}
	}

	if manifest.PlanPath != "" {
		planEntry, warning, err := buildPlanExportEntry(root, outputDir, manifest)
		if err != nil {
			return ExportResult{}, err
		}
		if warning != "" {
			result.Warnings = append(result.Warnings, warning)
		} else {
			result.Entries = append(result.Entries, planEntry)
		}
	}

	if opts.IncludeSnapshots {
		if manifest.SnapshotDir == "" {
			result.Warnings = append(result.Warnings, "skipping snapshots: manifest snapshot dir is empty")
		} else {
			for _, snapshotName := range referencedSnapshotNames(manifest) {
				normalizedSnapshotName, ok := normalizeExportRelativePath(snapshotName)
				if !ok {
					result.Warnings = append(result.Warnings, "skipping snapshot with non-normalized archive path: "+filepath.ToSlash(snapshotName))
					continue
				}
				sourcePath := filepath.Join(manifest.SnapshotDir, filepath.FromSlash(normalizedSnapshotName)+".json")
				inside, err := pathWithinRoot(sourcePath, manifest.SnapshotDir)
				if err != nil {
					return ExportResult{}, fmt.Errorf("validate snapshot %q: %w", snapshotName, err)
				}
				if !inside {
					result.Warnings = append(result.Warnings, "skipping snapshot outside snapshot dir: "+filepath.ToSlash(snapshotName))
					continue
				}
				relPath := filepath.ToSlash(filepath.Join("snapshots", filepath.FromSlash(normalizedSnapshotName)+".json"))
				entry, err := readExportEntry(root, sourcePath, relPath)
				if err != nil {
					if os.IsNotExist(err) {
						result.Warnings = append(result.Warnings, "missing snapshot: "+snapshotName)
						continue
					}
					return ExportResult{}, fmt.Errorf("read snapshot %q: %w", snapshotName, err)
				}
				result.Entries = append(result.Entries, entry)
			}
		}
	}

	result.Entries = stableUniqueExportEntries(result.Entries)
	return result, nil
}

func stableUniqueExportEntries(entries []ExportEntry) []ExportEntry {
	sorted := append([]ExportEntry(nil), entries...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].ArchivePath < sorted[j].ArchivePath
	})

	unique := make([]ExportEntry, 0, len(sorted))
	seenArchivePaths := make(map[string]struct{}, len(sorted))
	for _, entry := range sorted {
		if _, ok := seenArchivePaths[entry.ArchivePath]; ok {
			continue
		}
		seenArchivePaths[entry.ArchivePath] = struct{}{}
		unique = append(unique, entry)
	}
	return unique
}

func referencedSnapshotNames(manifest Manifest) []string {
	seen := make(map[string]struct{})
	names := make([]string, 0)
	for _, step := range manifest.Steps {
		if step.SnapshotName == "" {
			continue
		}
		if _, ok := seen[step.SnapshotName]; ok {
			continue
		}
		seen[step.SnapshotName] = struct{}{}
		names = append(names, step.SnapshotName)
	}
	sort.Strings(names)
	return names
}

func buildPlanExportEntry(root string, outputDir string, manifest Manifest) (ExportEntry, string, error) {
	if manifest.PlanSHA256 == "" {
		return ExportEntry{}, "missing plan.yaml from manifest plan path: "+manifest.PlanPath, nil
	}

	inside, err := pathWithinRoot(manifest.PlanPath, outputDir)
	if err != nil {
		return ExportEntry{}, fmt.Sprintf("skipping plan.yaml because manifest plan path could not be validated against workflow root: %s", manifest.PlanPath), nil
	}
	if !inside {
		return ExportEntry{}, "skipping plan.yaml because manifest plan path is outside workflow root: "+manifest.PlanPath, nil
	}

	data, err := os.ReadFile(manifest.PlanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ExportEntry{}, "missing plan.yaml from manifest plan path: "+manifest.PlanPath, nil
		}
		return ExportEntry{}, fmt.Sprintf("missing plan.yaml from manifest plan path: %s", manifest.PlanPath), nil
	}

	sum := sha256.Sum256(data)
	if fmt.Sprintf("%x", sum[:]) != manifest.PlanSHA256 {
		return ExportEntry{}, "skipping plan.yaml because manifest plan path no longer matches manifest plan_sha256: "+manifest.PlanPath, nil
	}

	plan, err := LoadPlan(bytes.NewReader(data))
	if err != nil {
		return ExportEntry{}, "skipping plan.yaml because manifest plan path is not a valid workflow plan: "+manifest.PlanPath, nil
	}
	if manifest.WorkflowName != "" && plan.Workflow.Name != manifest.WorkflowName {
		return ExportEntry{}, "skipping plan.yaml because workflow name does not match manifest: "+manifest.PlanPath, nil
	}
	if manifest.WorkflowPlanVersion != 0 && plan.Version != manifest.WorkflowPlanVersion {
		return ExportEntry{}, "skipping plan.yaml because workflow plan version does not match manifest: "+manifest.PlanPath, nil
	}

	return ExportEntry{
		ArchivePath: filepath.ToSlash(filepath.Join(root, "plan.yaml")),
		SourcePath:  manifest.PlanPath,
		Body:        append([]byte(nil), data...),
	}, "", nil
}

func readExportEntry(root string, sourcePath string, relativeArchivePath string) (ExportEntry, error) {
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return ExportEntry{}, err
	}
	return ExportEntry{
		ArchivePath: filepath.ToSlash(filepath.Join(root, relativeArchivePath)),
		SourcePath:  sourcePath,
		Body:        append([]byte(nil), data...),
	}, nil
}

func normalizeExportRelativePath(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	normalizedRaw := strings.ReplaceAll(raw, "\\", "/")
	if filepath.IsAbs(normalizedRaw) {
		return "", false
	}
	if hasDotDotPathSegment(normalizedRaw) {
		return "", false
	}
	normalized := pathCleanSlash(normalizedRaw)
	if normalized == "." || normalized == "" {
		return "", false
	}
	if normalized == ".." || strings.HasPrefix(normalized, "../") {
		return "", false
	}
	if hasDotDotPathSegment(normalized) {
		return "", false
	}
	return normalized, true
}

func pathCleanSlash(value string) string {
	return strings.TrimPrefix(filepath.ToSlash(filepath.Clean(filepath.FromSlash(value))), "./")
}

func hasDotDotPathSegment(value string) bool {
	for _, segment := range strings.Split(strings.ReplaceAll(value, "\\", "/"), "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

// WriteExportArchive writes the deterministic bundle to a zip archive.
func WriteExportArchive(path string, result ExportResult) error {
	inside, err := pathWithinRoot(path, result.OutputDir)
	if err != nil {
		return err
	}
	if inside {
		return fmt.Errorf("archive output path %q is inside workflow root %q", path, result.OutputDir)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create export archive: %w", err)
	}
	defer file.Close()

	writer := zip.NewWriter(file)
	entries := stableUniqueExportEntries(result.Entries)
	for _, entry := range entries {
		header := &zip.FileHeader{
			Name:   entry.ArchivePath,
			Method: zip.Deflate,
		}
		header.Modified = exportArchiveTimestamp
		header.SetMode(0o644)

		archiveEntry, err := writer.CreateHeader(header)
		if err != nil {
			writer.Close()
			return fmt.Errorf("create archive entry %q: %w", entry.ArchivePath, err)
		}
		if _, err := archiveEntry.Write(entry.Body); err != nil {
			writer.Close()
			return fmt.Errorf("write archive entry %q: %w", entry.ArchivePath, err)
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close export archive: %w", err)
	}
	return nil
}

func pathWithinRoot(targetPath string, root string) (bool, error) {
	canonicalRoot, err := canonicalizePath(root)
	if err != nil {
		return false, fmt.Errorf("resolve workflow root: %w", err)
	}
	canonicalTarget, err := canonicalizePath(targetPath)
	if err != nil {
		return false, fmt.Errorf("resolve candidate path: %w", err)
	}
	rel, err := filepath.Rel(canonicalRoot, canonicalTarget)
	if err != nil {
		return false, fmt.Errorf("compare candidate path to workflow root: %w", err)
	}
	if rel == "." {
		return true, nil
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)), nil
}

func canonicalizePath(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}

	existingPath := absPath
	missingParts := make([]string, 0)
	for {
		resolvedPath, err := filepath.EvalSymlinks(existingPath)
		if err == nil {
			canonicalPath := resolvedPath
			for index := len(missingParts) - 1; index >= 0; index-- {
				canonicalPath = filepath.Join(canonicalPath, missingParts[index])
			}
			return filepath.Clean(canonicalPath), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}

		parentPath := filepath.Dir(existingPath)
		if parentPath == existingPath {
			return "", err
		}
		missingParts = append(missingParts, filepath.Base(existingPath))
		existingPath = parentPath
	}
}
