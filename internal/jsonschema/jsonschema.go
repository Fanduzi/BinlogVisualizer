// Package jsonschema builds a JSON Schema document from the Go value encoding/json
// would marshal, and checks a document against that schema.
// input: a value whose type is the JSON object a command prints, plus the version key that object carries.
// output: a Draft 2020-12 schema with additionalProperties false on every struct, and a validator for one instance.
// pos: the schema published for every versioned JSON output (ADR 0017). Commands do not hand-write a second field list. A command that prints two shapes uses Union when they share required keys, or OneOf when a union would drop a shape's required keys.
// note: if this file changes, update docs/adr/0017-json-schema-and-display-timezone.md.
package jsonschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
)

const draft = "https://json-schema.org/draft/2020-12/schema"

// Of is the JSON Schema for the object encoding/json emits for v.
// v may be a typed nil pointer. The schema closes every struct
// (additionalProperties false). A nil slice or map is null, which the
// schema allows, because that is what encoding/json writes.
func Of(v any) (map[string]any, error) {
	t := reflect.TypeOf(v)
	if t == nil {
		return nil, fmt.Errorf("jsonschema: nil type")
	}
	doc, err := schemaOf(t, map[reflect.Type]bool{})
	if err != nil {
		return nil, err
	}
	if doc["type"] != "object" && !typeListHas(doc["type"], "object") {
		return nil, fmt.Errorf("jsonschema: %s is not a JSON object", t)
	}
	return doc, nil
}

// Union is one closed object whose properties are the union of the parts
// and whose required list is the keys every part requires. Two shapes of
// one command (workflow validate success and failure) share one schema.
func Union(parts ...any) (map[string]any, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("jsonschema: union of nothing")
	}
	props := map[string]any{}
	var required []string
	first := true
	for _, part := range parts {
		doc, err := Of(part)
		if err != nil {
			return nil, err
		}
		got, _ := doc["properties"].(map[string]any)
		for name, schema := range got {
			if prev, ok := props[name]; ok && !sameJSON(prev, schema) {
				return nil, fmt.Errorf("jsonschema: union property %s disagrees", name)
			}
			props[name] = schema
		}
		req := stringList(doc["required"])
		if first {
			required = req
			first = false
			continue
		}
		var keep []string
		for _, name := range required {
			if contains(req, name) {
				keep = append(keep, name)
			}
		}
		required = keep
	}
	sort.Strings(required)
	return objectSchema(props, required), nil
}

// AddVersion records key as a required integer const. The key is the
// version field already on the struct, or the key jsonout.Marshal injects.
func AddVersion(doc map[string]any, key string, version int) error {
	if version < 1 {
		return fmt.Errorf("jsonschema: version %d < 1", version)
	}
	props, _ := doc["properties"].(map[string]any)
	if props == nil {
		return fmt.Errorf("jsonschema: schema has no properties")
	}
	prop, _ := props[key].(map[string]any)
	if prop == nil {
		prop = map[string]any{}
		props[key] = prop
	}
	prop["type"] = "integer"
	prop["const"] = version
	req := stringList(doc["required"])
	if !contains(req, key) {
		req = append(req, key)
	}
	sort.Strings(req)
	doc["required"] = req
	return nil
}

// Versioned is Of(v) plus a required integer version key, rendered as a file.
func Versioned(v any, id, key string, version int) ([]byte, error) {
	doc, err := Of(v)
	if err != nil {
		return nil, err
	}
	if err := AddVersion(doc, key, version); err != nil {
		return nil, err
	}
	return Marshal(doc, id)
}

// UnionVersioned is Union plus a required integer version key.
func UnionVersioned(id, key string, version int, parts ...any) ([]byte, error) {
	doc, err := Union(parts...)
	if err != nil {
		return nil, err
	}
	if err := AddVersion(doc, key, version); err != nil {
		return nil, err
	}
	return Marshal(doc, id)
}

// OneOfVersioned is one schema whose oneOf branches are the parts, each with
// the version key required. A property union would keep only the required
// keys every part shares, so a success object and a failure object that
// share none would stop requiring the success keys. Each branch keeps its own.
func OneOfVersioned(id, key string, version int, parts ...any) ([]byte, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("jsonschema: oneOf needs at least two parts")
	}
	branches := make([]any, 0, len(parts))
	for _, part := range parts {
		doc, err := Of(part)
		if err != nil {
			return nil, err
		}
		if err := AddVersion(doc, key, version); err != nil {
			return nil, err
		}
		branches = append(branches, doc)
	}
	out := map[string]any{
		"$schema": draft,
		"$id":     "binlogviz:schema/" + id,
		"title":   id,
		"oneOf":   branches,
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		return nil, err
	}
	pretty.WriteByte('\n')
	return pretty.Bytes(), nil
}

// Marshal renders doc as the published schema file, with $schema and $id.
func Marshal(doc map[string]any, id string) ([]byte, error) {
	out := map[string]any{
		"$schema":              draft,
		"$id":                  "binlogviz:schema/" + id,
		"title":                id,
		"type":                 doc["type"],
		"properties":           doc["properties"],
		"additionalProperties": false,
	}
	if req := stringList(doc["required"]); len(req) > 0 {
		out["required"] = req
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		return nil, err
	}
	pretty.WriteByte('\n')
	return pretty.Bytes(), nil
}

// Validate reports the first place inst disagrees with schema.
// A property missing from a closed object, or a key the schema does not
// name, is an error. A oneOf matches exactly one branch and does not also
// apply sibling keywords on that object. Keywords this package does not
// emit are ignored.
func Validate(schema map[string]any, inst any) error {
	return validateAt(schema, inst, "$")
}

func schemaOf(t reflect.Type, stack map[reflect.Type]bool) (map[string]any, error) {
	if t.Kind() == reflect.Pointer {
		inner, err := schemaOf(t.Elem(), stack)
		if err != nil {
			return nil, err
		}
		// A pointer element (a slice item, a map value) marshals as null
		// when the pointer is nil. Struct fields unwrap the pointer first
		// and decide null from omitempty, so they do not reach this branch.
		return withNull(inner), nil
	}
	if t == reflect.TypeOf(json.RawMessage{}) {
		return map[string]any{}, nil
	}
	switch t.Kind() {
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return map[string]any{"type": "string"}, nil
		}
		if stack[t] {
			return map[string]any{}, nil
		}
		stack[t] = true
		defer delete(stack, t)
		props := map[string]any{}
		var required []string
		for _, f := range collectFields(t) {
			fs, err := fieldSchema(f, stack)
			if err != nil {
				return nil, err
			}
			props[f.name] = fs
			if !f.omit {
				required = append(required, f.name)
			}
		}
		sort.Strings(required)
		return objectSchema(props, required), nil
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return map[string]any{"type": "string"}, nil
		}
		item, err := schemaOf(t.Elem(), stack)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": item}, nil
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("jsonschema: map key %s is not string", t.Key())
		}
		elem, err := schemaOf(t.Elem(), stack)
		if err != nil {
			return nil, err
		}
		if t.Elem().Kind() == reflect.Interface {
			return map[string]any{"type": "object", "additionalProperties": true}, nil
		}
		return map[string]any{"type": "object", "additionalProperties": elem}, nil
	case reflect.Interface:
		return map[string]any{}, nil
	case reflect.String:
		return map[string]any{"type": "string"}, nil
	case reflect.Bool:
		return map[string]any{"type": "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}, nil
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}, nil
	default:
		return nil, fmt.Errorf("jsonschema: unsupported kind %s (%s)", t.Kind(), t)
	}
}

// jsonTime exists only so schemaOf can name time.Time without importing time
// into a comparison that breaks if the std type moves. The string check above
// is the real test; this alias is unused. Kept out: see timeType below.
func fieldSchema(f fieldInfo, stack map[reflect.Type]bool) (map[string]any, error) {
	t := f.typ
	nullable := false
	if t.Kind() == reflect.Pointer {
		nullable = !f.omit
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Map:
		if !f.omit {
			nullable = true
		}
	}
	inner, err := schemaOf(t, stack)
	if err != nil {
		return nil, err
	}
	if !nullable {
		return inner, nil
	}
	return withNull(inner), nil
}

// objectSchema is one closed object. required is omitted when empty:
// Draft 2020-12 says required is an array of at least one string, and a
// nil slice would marshal as null.
func objectSchema(props map[string]any, required []string) map[string]any {
	doc := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		doc["required"] = required
	}
	return doc
}

func withNull(schema map[string]any) map[string]any {
	if len(schema) == 0 {
		return schema
	}
	types := typeNames(schema["type"])
	if len(types) == 0 {
		return schema
	}
	if contains(types, "null") {
		return schema
	}
	types = append(types, "null")
	out := map[string]any{}
	for k, v := range schema {
		out[k] = v
	}
	if len(types) == 1 {
		out["type"] = types[0]
	} else {
		out["type"] = types
	}
	return out
}

type fieldInfo struct {
	name string
	omit bool
	typ  reflect.Type
}

func collectFields(t reflect.Type) []fieldInfo {
	var direct []fieldInfo
	var embeds []reflect.Type
	seen := map[string]bool{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name, omit, skip, embed := jsonField(f)
		if skip {
			continue
		}
		if embed {
			et := f.Type
			if et.Kind() == reflect.Pointer {
				et = et.Elem()
			}
			embeds = append(embeds, et)
			continue
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		direct = append(direct, fieldInfo{name: name, omit: omit, typ: f.Type})
	}
	for _, et := range embeds {
		for _, f := range collectFields(et) {
			if seen[f.name] {
				continue
			}
			seen[f.name] = true
			direct = append(direct, f)
		}
	}
	return direct
}

func jsonField(f reflect.StructField) (name string, omit bool, skip bool, embed bool) {
	if f.PkgPath != "" && !f.Anonymous {
		return "", false, true, false
	}
	tag := f.Tag.Get("json")
	if tag == "-" {
		return "", false, true, false
	}
	name = f.Name
	if tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] == "-" {
			return "", false, true, false
		}
		if parts[0] != "" {
			name = parts[0]
		}
		for _, opt := range parts[1:] {
			if opt == "omitempty" {
				omit = true
			}
		}
	}
	if f.Anonymous && (tag == "" || tag == "omitempty" || strings.HasPrefix(tag, ",omitempty")) {
		et := f.Type
		if et.Kind() == reflect.Pointer {
			et = et.Elem()
		}
		if et.Kind() == reflect.Struct {
			return "", false, false, true
		}
	}
	if f.PkgPath != "" {
		return "", false, true, false
	}
	return name, omit, false, false
}

func validateAt(schema map[string]any, inst any, path string) error {
	if len(schema) == 0 {
		return nil
	}
	if branches, ok := schema["oneOf"]; ok {
		return validateOneOf(branches, inst, path)
	}
	if c, ok := schema["const"]; ok && !sameJSON(c, inst) {
		return fmt.Errorf("%s: const %v, got %v", path, c, inst)
	}
	if err := checkType(schema["type"], inst, path); err != nil {
		return err
	}
	switch inst := inst.(type) {
	case map[string]any:
		props, _ := schema["properties"].(map[string]any)
		for _, name := range stringList(schema["required"]) {
			if _, ok := inst[name]; !ok {
				return fmt.Errorf("%s: missing required property %q", path, name)
			}
		}
		add := schema["additionalProperties"]
		closed := add == nil || add == false
		for name, value := range inst {
			sub, ok := props[name].(map[string]any)
			if ok {
				if err := validateAt(sub, value, path+"."+name); err != nil {
					return err
				}
				continue
			}
			if closed {
				return fmt.Errorf("%s: property %q is not in the schema", path, name)
			}
			if sub, ok := add.(map[string]any); ok {
				if err := validateAt(sub, value, path+"."+name); err != nil {
					return err
				}
			}
		}
	case []any:
		item, _ := schema["items"].(map[string]any)
		if item == nil {
			return nil
		}
		for i, value := range inst {
			if err := validateAt(item, value, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateOneOf(branches any, inst any, path string) error {
	list, ok := branches.([]any)
	if !ok {
		return fmt.Errorf("%s: oneOf is not an array", path)
	}
	matched := 0
	var first error
	for _, branch := range list {
		sub, ok := branch.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: oneOf branch is not an object", path)
		}
		err := validateAt(sub, inst, path)
		if err == nil {
			matched++
			continue
		}
		if first == nil {
			first = err
		}
	}
	switch matched {
	case 1:
		return nil
	case 0:
		if first != nil {
			return fmt.Errorf("%s: oneOf matched none: %v", path, first)
		}
		return fmt.Errorf("%s: oneOf matched none", path)
	default:
		return fmt.Errorf("%s: oneOf matched %d schemas", path, matched)
	}
}

func checkType(spec any, inst any, path string) error {
	names := typeNames(spec)
	if len(names) == 0 {
		return nil
	}
	for _, name := range names {
		if typeOK(name, inst) {
			return nil
		}
	}
	return fmt.Errorf("%s: type %s, schema allows %s", path, jsonKind(inst), strings.Join(names, "|"))
}

func typeOK(name string, inst any) bool {
	switch name {
	case "object":
		_, ok := inst.(map[string]any)
		return ok
	case "array":
		_, ok := inst.([]any)
		return ok
	case "string":
		_, ok := inst.(string)
		return ok
	case "boolean":
		_, ok := inst.(bool)
		return ok
	case "integer":
		n, ok := numberValue(inst)
		return ok && !math.IsInf(n, 0) && n == math.Trunc(n)
	case "number":
		_, ok := numberValue(inst)
		return ok
	case "null":
		return inst == nil
	default:
		return false
	}
}

func numberValue(inst any) (float64, bool) {
	switch n := inst.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func typeNames(spec any) []string {
	switch spec := spec.(type) {
	case nil:
		return nil
	case string:
		return []string{spec}
	case []string:
		return spec
	case []any:
		out := make([]string, 0, len(spec))
		for _, item := range spec {
			s, _ := item.(string)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func typeListHas(spec any, want string) bool {
	return contains(typeNames(spec), want)
}

func stringList(v any) []string {
	switch v := v.(type) {
	case nil:
		return nil
	case []string:
		return append([]string(nil), v...)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, _ := item.(string)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func sameJSON(a, b any) bool {
	ab, errA := json.Marshal(a)
	bb, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(ab, bb)
}

func jsonKind(v any) string {
	if v == nil {
		return "null"
	}
	return reflect.TypeOf(v).Kind().String()
}
