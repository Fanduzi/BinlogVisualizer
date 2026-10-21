package binlogviz

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
)

// TestDraft202012Schemas checks every published schema against the Draft 2020-12
// meta-schema with santhosh-tekuri/jsonschema, then checks real command output
// against those schemas, including a failure document a command prints on
// stdout. A schema that still has "required": null fails here
// and is accepted by the in-repo validator.
func TestDraft202012Schemas(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, "docs", "schema")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	compiled := map[string]*jsonschema.Schema{}
	var saw bool
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		saw = true
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(`"required": null`)) {
			t.Errorf("%s still contains \"required\": null", entry.Name())
		}
		sch, err := compileDraft202012(raw)
		if err != nil {
			t.Errorf("%s is not a Draft 2020-12 schema: %v", entry.Name(), err)
			continue
		}
		compiled[strings.TrimSuffix(entry.Name(), ".json")] = sch
	}
	if !saw {
		t.Fatal("docs/schema has no schema files")
	}

	nullRequired := []byte(`{
	  "$schema": "https://json-schema.org/draft/2020-12/schema",
	  "$id": "binlogviz:schema/required-null",
	  "type": "object",
	  "properties": {
	    "actor": {
	      "type": "object",
	      "properties": {"user": {"type": "string"}, "host": {"type": "string"}},
	      "required": null,
	      "additionalProperties": false
	    }
	  },
	  "required": ["actor"],
	  "additionalProperties": false
	}`)
	if _, err := compileDraft202012(nullRequired); err == nil {
		t.Fatal("a schema with \"required\": null was accepted; the meta-schema check does not fail on the old generator output")
	}

	for _, run := range prepareJSONOutputRuns(t) {
		sch := compiled[run.name]
		if sch == nil {
			t.Errorf("no compiled schema for %s", run.name)
			continue
		}
		code, stdout, stderr := runRoot(t, run.args...)
		if code != run.exit {
			t.Errorf("%s exit %d: %s", run.name, code, stderr)
			continue
		}
		if err := validateDraft202012(sch, stdout); err != nil {
			t.Errorf("%s output does not match its Draft 2020-12 schema: %v", run.name, err)
		}
	}
	for _, run := range prepareJSONFailureRuns(t) {
		if !run.document {
			continue
		}
		sch := compiled[run.name]
		if sch == nil {
			t.Errorf("no compiled schema for failure %s", run.name)
			continue
		}
		code, stdout, stderr := runRoot(t, run.args...)
		if code != run.exit {
			t.Errorf("%s failure exit %d: %s", run.name, code, stderr)
			continue
		}
		if err := validateDraft202012(sch, stdout); err != nil {
			t.Errorf("%s failure output does not match its Draft 2020-12 schema: %v\n%s", run.name, err, stdout)
		}
	}

	analyzeSchema := compiled["analyze"]
	if analyzeSchema == nil {
		t.Fatal("analyze schema did not compile, so real reports were not checked")
	}
	ddlPath := mustFixturePath(t, "mysql-8.4.11-ddl.binlog")
	code, stdout, stderr := runRoot(t, "analyze", ddlPath, "--format", "json", "-q")
	if code != 0 {
		t.Fatalf("ddl analyze exit %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, `"ddl_events"`) {
		t.Fatalf("ddl fixture produced no ddl_events:\n%s", stdout)
	}
	if err := validateDraft202012(analyzeSchema, stdout); err != nil {
		t.Fatalf("real DDL report: %v", err)
	}

	actorReport := ddlActorReport(t)
	if !strings.Contains(actorReport, `"actor"`) {
		t.Fatalf("DDL actor report has no actor object:\n%s", actorReport)
	}
	if err := validateDraft202012(analyzeSchema, actorReport); err != nil {
		t.Fatalf("DDL actor report: %v", err)
	}
}

func ddlActorReport(t *testing.T) string {
	t.Helper()
	ts := time.Date(2026, 10, 20, 8, 41, 18, 0, time.UTC)
	mock := &mockParser{events: []binlog.RawEvent{{
		Timestamp: ts,
		EventType: "QUERY",
		Query:     "DROP TABLE `shop`.`orders`",
		Schema:    "shop",
		ThreadID:  30,
		ActorUser: "root",
		ActorHost: "localhost",
	}}}
	stdout, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParser([]string{"ddl-actor.binlog"}, analyzer.DefaultOptions(), "json", mock)
	})
	if err != nil {
		t.Fatal(err)
	}
	return stdout
}

func compileDraft202012(raw []byte) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	const loc = "https://binlogviz.test/schema.json"
	if err := c.AddResource(loc, doc); err != nil {
		return nil, err
	}
	return c.Compile(loc)
}

func validateDraft202012(sch *jsonschema.Schema, raw string) error {
	inst, err := jsonschema.UnmarshalJSON(strings.NewReader(raw))
	if err != nil {
		return err
	}
	return sch.Validate(inst)
}
