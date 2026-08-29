// Package analyzer tests canonical GTID selector parsing and precedence.
// input: MySQL UUID ranges, MariaDB exact identities, anonymous IDs, and mixed-flavor selectors.
// output: deterministic canonical sets, explicit flavor resolution, and include/exclude match decisions.
// pos: public selector grammar contract ahead of transaction-group filtering.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseMySQLGTIDSelectorNormalizesRangesAndExcludeWins(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	selector, err := ParseGTIDSelector(
		[]string{strings.ToUpper(sid) + ":3:1-2", sid + ":2-4"},
		[]string{sid + ":4"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if selector.Flavor() != "mysql" {
		t.Fatalf("flavor = %q", selector.Flavor())
	}
	if got, want := selector.Include(), []string{sid + ":1-4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("include = %v, want %v", got, want)
	}
	if matched, err := selector.Match(sid + ":3"); err != nil || !matched {
		t.Fatalf("sequence 3 match = %v, %v", matched, err)
	}
	if matched, err := selector.Match(sid + ":4"); err != nil || matched {
		t.Fatalf("excluded sequence 4 match = %v, %v", matched, err)
	}
	if matched, err := selector.Match(""); err != nil || matched {
		t.Fatalf("anonymous match = %v, %v", matched, err)
	}
}

func TestParseMariaDBGTIDSelectorUsesExactIdentities(t *testing.T) {
	selector, err := ParseGTIDSelector([]string{"0-7-1857,1-2-3"}, []string{"0-7-1857"})
	if err != nil {
		t.Fatal(err)
	}
	if selector.Flavor() != "mariadb" {
		t.Fatalf("flavor = %q", selector.Flavor())
	}
	if got, want := selector.Include(), []string{"0-7-1857", "1-2-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("include = %v, want %v", got, want)
	}
	if matched, err := selector.Match("0-7-1857"); err != nil || matched {
		t.Fatalf("excluded identity match = %v, %v", matched, err)
	}
	if matched, err := selector.Match("1-2-3"); err != nil || !matched {
		t.Fatalf("included identity match = %v, %v", matched, err)
	}
}

func TestParseGTIDSelectorRejectsMixedFlavor(t *testing.T) {
	_, err := ParseGTIDSelector(
		[]string{"24bc7850-2c16-11e6-a073-0242ac110002:1"},
		[]string{"0-7-1857"},
	)
	if err == nil || !strings.Contains(err.Error(), "mixed GTID selector flavors") {
		t.Fatalf("error = %v", err)
	}
}

func TestGTIDExcludeOnlyDoesNotRejectAnonymousGroup(t *testing.T) {
	selector, err := ParseGTIDSelector(nil, []string{"24bc7850-2c16-11e6-a073-0242ac110002:1"})
	if err != nil {
		t.Fatal(err)
	}
	matched, err := selector.Match("")
	if err != nil || !matched {
		t.Fatalf("anonymous exclude-only match = %v, %v", matched, err)
	}
}
