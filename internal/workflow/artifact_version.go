// input: a workflow output dir and its manifest.
// output: an unsupported (9) error when a recorded compare/trend JSON artifact carries a schema_version this build does not know.
// pos: ADR 0010 consumer rule for workflow status, resume and clean; the artifact counterpart of manifest_version and report_version checks.
// note: if this file changes, update this header, ADR 0010, docs/reference/output-format(.zh-CN).md and module README.md.
package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"binlogviz/internal/clierr"
	"binlogviz/internal/compare"
	"binlogviz/internal/trend"
)

// ReasonUnsupportedArtifactVersion is details.reason for a workflow artifact
// written with a schema_version this build does not know.
const ReasonUnsupportedArtifactVersion = "unsupported_artifact_version"

// supportedArtifactSchemaVersions is the newest schema_version this build
// reads per step kind. compare and trend JSON started at 1 (ADR 0010).
var supportedArtifactSchemaVersions = map[string]int{
	"compare": compare.SchemaVersion,
	"trend":   trend.SchemaVersion,
}

// CheckArtifactVersions fails with unsupported (9) when a compare or trend
// JSON artifact the manifest records has a schema_version outside
// 1..supported, or one that is not an integer. An artifact without the key
// was written before ADR 0010 and is accepted. Missing, unreadable or
// malformed files are left to the callers, which already report them.
func CheckArtifactVersions(outputDir string, m Manifest) error {
	for _, step := range m.Steps {
		supported, ok := supportedArtifactSchemaVersions[step.Kind]
		if !ok {
			continue
		}
		for _, artifact := range step.Artifacts {
			if !strings.EqualFold(filepath.Ext(artifact), ".json") {
				continue
			}
			data, err := os.ReadFile(resolveArtifactPath(outputDir, filepath.FromSlash(artifact)))
			if err != nil {
				continue
			}
			var head struct {
				SchemaVersion json.RawMessage `json:"schema_version"`
			}
			if err := json.Unmarshal(data, &head); err != nil || head.SchemaVersion == nil {
				continue
			}
			raw := bytes.TrimSpace(head.SchemaVersion)
			var version int
			if err := json.Unmarshal(raw, &version); err != nil || version < 1 || version > supported {
				msg := fmt.Sprintf("%s artifact %s has schema_version %s; this BinlogViz build supports %s schema_version up to %d. Use a newer binlogviz, or re-run the workflow into a fresh output dir",
					step.Kind, artifact, raw, step.Kind, supported)
				unsupported := clierr.New(clierr.KindUnsupported, msg).(*clierr.Error)
				unsupported.Details = map[string]any{
					"reason":            ReasonUnsupportedArtifactVersion,
					"artifact":          artifact,
					"step_kind":         step.Kind,
					"step_name":         step.Name,
					"schema_version":    json.RawMessage(raw),
					"supported_version": supported,
				}
				return unsupported
			}
		}
	}
	return nil
}
