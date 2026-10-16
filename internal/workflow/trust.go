// input: a decoded workflow manifest and its output directory.
// output: an error when the manifest cannot be trusted to say which files a workflow produced.
// pos: shared gate for destructive (clean) and archival (export) commands; see ADR 0009 §8.
// note: if this file changes, update this header, ADR 0009 and module README.md.
package workflow

import (
	"os"
	"path/filepath"

	"binlogviz/internal/clierr"
)

// ReasonUntrustedManifest is details.reason for a manifest that cannot
// attribute files to recorded steps.
const ReasonUntrustedManifest = "untrusted_manifest"

// artifactKinds are the output subdirectories whose files a step records.
var artifactKinds = []string{"analyze", "compare", "trend"}

// CheckManifestIntegrity fails when the manifest lacks the fields every
// manifest written by binlogviz has. That is damaged content: corrupt (5).
// A manifest_version newer than this build is unsupported (9).
func CheckManifestIntegrity(m Manifest) error {
	if m.ManifestVersion > 2 {
		return clierr.Errorf(clierr.KindUnsupported, "manifest_version %d is not supported (only versions up to 2)", m.ManifestVersion)
	}
	if m.WorkflowName == "" {
		return clierr.Reasonf(clierr.KindCorrupt, ReasonUntrustedManifest,
			"manifest is missing required fields (workflow_name); it cannot say which files the workflow produced")
	}
	return nil
}

// CheckCleanTrust is the gate before clean decides anything. Besides
// CheckManifestIntegrity, a well-formed manifest that records no steps while
// step artifacts exist on disk cannot attribute any of them, so clean
// refuses (6) instead of treating "lists nothing" as "everything is orphaned".
func CheckCleanTrust(outputDir string, m Manifest) error {
	if err := CheckManifestIntegrity(m); err != nil {
		return err
	}
	if len(m.Steps) == 0 && hasStepArtifacts(outputDir) {
		return clierr.Reasonf(clierr.KindRefused, ReasonUntrustedManifest,
			"refusing to clean: manifest records no steps but %s has workflow artifacts; nothing can be attributed to a recorded step", outputDir)
	}
	return nil
}

func hasStepArtifacts(outputDir string) bool {
	for _, kind := range artifactKinds {
		entries, err := os.ReadDir(filepath.Join(outputDir, kind))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				return true
			}
		}
	}
	return false
}

// recordedKinds returns the step kinds the manifest recorded.
func recordedKinds(m Manifest) map[string]bool {
	kinds := map[string]bool{}
	for _, s := range m.Steps {
		kinds[s.Kind] = true
	}
	return kinds
}
