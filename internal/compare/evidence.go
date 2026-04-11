// Package compare builds evidence refs that link findings back to report sections.
// input: a CompareResult with populated findings and section data.
// output: deterministic evidence_refs attached to each CompareFinding.
// pos: evidence ref builder called after key findings are constructed.
package compare

// buildCompareEvidenceRefs attaches evidence refs to each finding in the result.
// It only emits refs when the finding's evidence data matches an existing item.
func buildCompareEvidenceRefs(result *CompareResult) {
	for i := range result.KeyFindings {
		f := &result.KeyFindings[i]
		f.EvidenceRefs = buildFindingRefs(*f, result)
	}
}

func buildFindingRefs(f CompareFinding, result *CompareResult) []EvidenceRef {
	var refs []EvidenceRef

	switch f.Kind {
	case "volume_change":
		// Volume change is general — the summary cards show it directly.
		// No section-level anchor to link to.

	case "pattern_driver":
		if key, ok := f.Evidence["pattern_key"].(string); ok {
			idx := findPatternChangeIndex(result.PatternChanges, key)
			if idx >= 0 {
				refs = append(refs, EvidenceRef{
					Section: "pattern_changes",
					Key:     key,
					Label:   patternLabel(result.PatternChanges[idx]),
					Anchor:  "section-pattern-changes",
				})
			}
		}

	case "table_driver":
		if table, ok := f.Evidence["table"].(string); ok {
			idx := findTableChangeIndex(result.TableChanges, table)
			if idx >= 0 {
				refs = append(refs, EvidenceRef{
					Section: "table_changes",
					Key:     table,
					Label:   table,
					Anchor:  "section-table-changes",
				})
			}
		}

	case "operation_mix_drift":
		if _, ok := f.Evidence["operation"]; ok {
			refs = append(refs, EvidenceRef{
				Section: "operation_mix",
				Label:   "Operation Mix",
				Anchor:  "section-operation-mix",
			})
		}

	case "new_pattern":
		if key, ok := f.Evidence["pattern_key"].(string); ok {
			idx := findPatternChangeIndex(result.PatternChanges, key)
			if idx >= 0 {
				refs = append(refs, EvidenceRef{
					Section: "pattern_changes",
					Key:     key,
					Label:   patternLabel(result.PatternChanges[idx]),
					Anchor:  "section-pattern-changes",
				})
			}
		}
	}

	if len(refs) == 0 {
		return nil
	}
	return refs
}

func patternLabel(p PatternChange) string {
	if p.Label != "" {
		return p.Label
	}
	return p.PatternKey
}

func findPatternChangeIndex(changes []PatternChange, key string) int {
	for i, p := range changes {
		if p.PatternKey == key {
			return i
		}
	}
	return -1
}

func findTableChangeIndex(changes []TableChange, table string) int {
	for i, t := range changes {
		if t.Schema+"."+t.Table == table {
			return i
		}
	}
	return -1
}
