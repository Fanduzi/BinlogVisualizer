// Package compare evaluates explicit workload comparability for compare and trend narratives.
// input: named report-v0-v3 inputs with workload IDs, object/selector scopes, producer provenance, schema evidence, and transaction completeness counts.
// output: deterministic comparable/not_comparable/unknown verdicts, stable reason codes, selector-visible evidence, and one guard finding for unsafe narratives.
// pos: shared safety gate between report decoding and compare/trend causal finding construction.
// note: if this file changes, update this header and internal/compare/README.md.
package compare

import (
	"encoding/json"
	"sort"
	"strings"

	"binlogviz/internal/i18n"
)

var comparabilityReasonOrder = []string{
	ReasonWorkloadIdentityMismatch,
	ReasonMixedProducers,
	ReasonProducerFlavorConflict,
	ReasonIncompatibleScope,
	ReasonMissingWorkloadIdentity,
	ReasonMissingProducerProvenance,
	ReasonLegacyReportMetadata,
	ReasonMissingScope,
	ReasonInsufficientCompleteness,
	ReasonPartialOrUnknownTransactions,
}

// AssessComparability evaluates every input as one workload series.
func AssessComparability(inputs []ComparabilityInput) Comparability {
	reasons := make(map[string]bool)
	workloadIDs := make(map[string]bool)
	flavors := make(map[string]bool)
	objectScopes := make(map[string]bool)
	selectorScopes := make(map[string]bool)
	evidence := make([]ComparabilityEvidence, 0, len(inputs))

	for _, input := range inputs {
		report := input.Report
		evidence = append(evidence, buildComparabilityEvidence(input.Role, report))

		if report.ReportVersion == nil || *report.ReportVersion < currentSupportedReportVersion {
			reasons[ReasonLegacyReportMetadata] = true
		}
		if workloadID := strings.TrimSpace(report.WorkloadID); workloadID == "" {
			reasons[ReasonMissingWorkloadIdentity] = true
		} else {
			workloadIDs[workloadID] = true
		}

		reportFlavors := normalizedStrings(nil)
		if report.Provenance != nil {
			reportFlavors = normalizedStrings(report.Provenance.ServerFlavors)
		}
		if report.Provenance == nil || len(reportFlavors) == 0 {
			reasons[ReasonMissingProducerProvenance] = true
		} else {
			for _, flavor := range reportFlavors {
				flavors[flavor] = true
			}
			if report.Provenance.MixedProducers || len(reportFlavors) > 1 {
				reasons[ReasonMixedProducers] = true
			}
		}

		if report.Scope == nil {
			reasons[ReasonMissingScope] = true
		} else {
			objectScopes[canonicalScopeKey(*report.Scope, nil)] = true
		}
		if report.ReportVersion != nil && *report.ReportVersion >= currentSupportedReportVersion {
			selectorScopes[canonicalScopeKey(InputSnapshotFilters{}, report.Selection)] = true
		}

		if report.Summary.PartialTransactions == nil || report.Summary.UnknownTransactions == nil {
			reasons[ReasonInsufficientCompleteness] = true
		} else if *report.Summary.PartialTransactions > 0 || *report.Summary.UnknownTransactions > 0 {
			reasons[ReasonPartialOrUnknownTransactions] = true
		}
	}

	if len(workloadIDs) > 1 {
		reasons[ReasonWorkloadIdentityMismatch] = true
	}
	if len(flavors) > 1 {
		reasons[ReasonProducerFlavorConflict] = true
	}
	if len(objectScopes) > 1 || len(selectorScopes) > 1 {
		reasons[ReasonIncompatibleScope] = true
	}

	verdict := VerdictComparable
	if reasons[ReasonWorkloadIdentityMismatch] || reasons[ReasonMixedProducers] || reasons[ReasonProducerFlavorConflict] || reasons[ReasonIncompatibleScope] {
		verdict = VerdictNotComparable
	} else if len(reasons) > 0 {
		verdict = VerdictUnknown
	}

	return Comparability{Verdict: verdict, ReasonCodes: orderedReasonCodes(reasons), Evidence: evidence}
}

func buildComparabilityEvidence(role string, report InputReport) ComparabilityEvidence {
	evidence := ComparabilityEvidence{
		Role:                role,
		ReportVersion:       report.ReportVersion,
		WorkloadID:          strings.TrimSpace(report.WorkloadID),
		Scope:               cloneScope(report.Scope),
		Selection:           cloneSelection(report.Selection),
		Schemas:             reportSchemas(report),
		TotalTransactions:   report.Summary.TotalTransactions,
		PartialTransactions: report.Summary.PartialTransactions,
		UnknownTransactions: report.Summary.UnknownTransactions,
	}
	if report.Snapshot != nil {
		evidence.Name = report.Snapshot.Name
	}
	if report.Provenance != nil {
		evidence.ServerIDs = append([]uint32(nil), report.Provenance.ServerIDs...)
		evidence.ServerVersions = normalizedStrings(report.Provenance.ServerVersions)
		evidence.ServerFlavors = normalizedStrings(report.Provenance.ServerFlavors)
		evidence.MixedProducers = report.Provenance.MixedProducers
		sort.Slice(evidence.ServerIDs, func(i, j int) bool { return evidence.ServerIDs[i] < evidence.ServerIDs[j] })
	}
	return evidence
}

func reportSchemas(report InputReport) []string {
	seen := make(map[string]bool)
	for _, table := range report.Tables {
		if schema := strings.TrimSpace(table.Schema); schema != "" {
			seen[schema] = true
		}
	}
	out := make([]string, 0, len(seen))
	for schema := range seen {
		out = append(out, schema)
	}
	sort.Strings(out)
	return out
}

func normalizedStrings(values []string) []string {
	seen := make(map[string]bool)
	for _, value := range values {
		if normalized := strings.ToLower(strings.TrimSpace(value)); normalized != "" {
			seen[normalized] = true
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

type canonicalComparabilityScope struct {
	Filters   InputSnapshotFilters `json:"filters"`
	Selection *InputSelection      `json:"selection,omitempty"`
}

func canonicalScopeKey(scope InputSnapshotFilters, selection *InputSelection) string {
	canonical := canonicalComparabilityScope{
		Filters: InputSnapshotFilters{
			IncludeSchemas: normalizedScopeValues(scope.IncludeSchemas),
			ExcludeSchemas: normalizedScopeValues(scope.ExcludeSchemas),
			IncludeTables:  normalizedScopeValues(scope.IncludeTables),
			ExcludeTables:  normalizedScopeValues(scope.ExcludeTables),
		},
		Selection: canonicalSelection(selection),
	}
	encoded, _ := json.Marshal(canonical)
	return string(encoded)
}

func canonicalSelection(selection *InputSelection) *InputSelection {
	if selection == nil {
		return nil
	}
	canonical := cloneSelection(selection)
	canonical.IncludeGTIDs = normalizedScopeValues(canonical.IncludeGTIDs)
	canonical.ExcludeGTIDs = normalizedScopeValues(canonical.ExcludeGTIDs)
	// MatchedGTIDs is retained result evidence, not a requested selector.
	canonical.MatchedGTIDs = nil
	canonical.ResolvedGTIDFlavor = strings.ToLower(strings.TrimSpace(canonical.ResolvedGTIDFlavor))
	return canonical
}

func normalizedScopeValues(values []string) []string {
	seen := make(map[string]bool)
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			seen[value] = true
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func cloneScope(scope *InputSnapshotFilters) *InputSnapshotFilters {
	if scope == nil {
		return nil
	}
	return &InputSnapshotFilters{
		IncludeSchemas: append([]string(nil), scope.IncludeSchemas...),
		ExcludeSchemas: append([]string(nil), scope.ExcludeSchemas...),
		IncludeTables:  append([]string(nil), scope.IncludeTables...),
		ExcludeTables:  append([]string(nil), scope.ExcludeTables...),
	}
}

func cloneSelection(selection *InputSelection) *InputSelection {
	if selection == nil {
		return nil
	}
	return &InputSelection{
		RequestedStartPosition: cloneInt64(selection.RequestedStartPosition),
		RequestedStopPosition:  cloneInt64(selection.RequestedStopPosition),
		EffectiveStartPosition: cloneInt64(selection.EffectiveStartPosition),
		EffectiveStopPosition:  cloneInt64(selection.EffectiveStopPosition),
		IncludeGTIDs:           append([]string(nil), selection.IncludeGTIDs...),
		ExcludeGTIDs:           append([]string(nil), selection.ExcludeGTIDs...),
		ResolvedGTIDFlavor:     selection.ResolvedGTIDFlavor,
		MatchedGTIDs:           append([]string(nil), selection.MatchedGTIDs...),
	}
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func orderedReasonCodes(reasons map[string]bool) []string {
	out := make([]string, 0, len(reasons))
	for _, reason := range comparabilityReasonOrder {
		if reasons[reason] {
			out = append(out, reason)
		}
	}
	return out
}

func comparabilityGuardFinding(comparability Comparability) CompareFinding {
	return CompareFinding{
		Kind:    "comparability_guard",
		Title:   ComparabilityGuardTitle(),
		Summary: ComparabilityGuardSummary(comparability.Verdict),
		Evidence: map[string]any{
			"verdict":      comparability.Verdict,
			"reason_codes": append([]string(nil), comparability.ReasonCodes...),
		},
	}
}

// ComparabilityGuardTitle returns the localized safety-guard title shared by compare and trend.
func ComparabilityGuardTitle() string {
	return i18n.T("report.comparability.guardTitle")
}

// ComparabilityGuardSummary returns the localized guard summary for a verdict.
func ComparabilityGuardSummary(verdict ComparabilityVerdict) string {
	if verdict == VerdictNotComparable {
		return i18n.T("report.comparability.notComparableSummary")
	}
	return i18n.T("report.comparability.unknownSummary")
}

// ComparabilityReasonCodesLabel returns the localized reason-code label.
func ComparabilityReasonCodesLabel() string {
	return i18n.T("report.comparability.reasonCodes")
}
