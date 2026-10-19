package flashback

import "errors"

// GenerationRefusal is an exit-6 flashback refusal with a stable reason.
// Table is set when the refusal names one table.
type GenerationRefusal struct {
	Code  string
	Table string
	Err   error
}

func (e *GenerationRefusal) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *GenerationRefusal) Unwrap() error { return e.Err }

func refuse(code, table string, err error) error {
	if err == nil {
		return nil
	}
	return &GenerationRefusal{Code: code, Table: table, Err: err}
}

// RefusalDetails reports the stable reason of a generation refusal.
func RefusalDetails(err error) (reason, table string, ok bool) {
	var gen *GenerationRefusal
	if errors.As(err, &gen) && gen != nil {
		return gen.Code, gen.Table, true
	}
	var row *generatedRowRefusal
	if errors.As(err, &row) && row != nil {
		return "generated_row", row.shownTable(), true
	}
	return "", "", false
}
