package flashback

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestPublishCopiesOnlyAfterSuccess(t *testing.T) {
	var dst bytes.Buffer
	err := Publish(&dst, func(w io.Writer) error {
		if _, werr := io.WriteString(w, "prefix"); werr != nil {
			return werr
		}
		return errors.New("disk full")
	})
	if err == nil || dst.Len() != 0 {
		t.Fatalf("err %v dst %q", err, dst.String())
	}

	dst.Reset()
	err = Publish(&dst, func(w io.Writer) error {
		_, werr := io.WriteString(w, "sealed-script")
		return werr
	})
	if err != nil || dst.String() != "sealed-script" {
		t.Fatalf("err %v dst %q", err, dst.String())
	}
}

func TestPublishCopyFailureLeavesAPrefix(t *testing.T) {
	err := Publish(&failAfter{n: 4}, func(w io.Writer) error {
		_, werr := io.WriteString(w, "sealed-script")
		return werr
	})
	if err == nil {
		t.Fatal("copy succeeded")
	}
}

type failAfter struct {
	n int
	b strings.Builder
}

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, errors.New("short write")
	}
	if len(p) > f.n {
		p = p[:f.n]
	}
	n, _ := f.b.Write(p)
	f.n -= n
	if f.n == 0 {
		return n, errors.New("short write")
	}
	return n, nil
}
