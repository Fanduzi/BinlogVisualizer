package guard

import (
	"crypto/sha256"
	"io"
	"strings"
	"testing"
)

func TestSealWriterMatchesSeal(t *testing.T) {
	g := New([]Col{{Schema: "shop", Table: "t", Column: "g"}})
	script := "SET NAMES utf8mb4;\n" + g.Header() + "INSERT INTO `shop`.`t` (`id`) SELECT 1 FROM DUAL WHERE " + g.RowCond() + ";\n"
	if !strings.Contains(script, guardTokenMark) {
		t.Fatal("script has no token placeholder")
	}
	sum := sha256.Sum256([]byte(script))
	var b strings.Builder
	write, err := g.SealWriter(&b, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	if err := write(script); err != nil {
		t.Fatal(err)
	}
	if b.String() != g.Seal(script) {
		t.Fatalf("SealWriter:\n%s\nSeal:\n%s", b.String(), g.Seal(script))
	}

	zero := New(nil)
	plain := "INSERT INTO t VALUES (1);\n"
	zsum := sha256.Sum256([]byte(plain))
	b.Reset()
	write, err = zero.SealWriter(&b, zsum[:])
	if err != nil {
		t.Fatal(err)
	}
	if err := write(plain); err != nil {
		t.Fatal(err)
	}
	if b.String() != zero.Seal(plain) {
		t.Fatalf("zero guard SealWriter %q Seal %q", b.String(), zero.Seal(plain))
	}
}

func TestSealWriterRejectsShortDigest(t *testing.T) {
	g := New([]Col{{Schema: "shop", Table: "t", Column: "g"}})
	for _, n := range []int{0, 7} {
		if _, err := g.SealWriter(io.Discard, make([]byte, n)); err == nil {
			t.Fatalf("%d-byte digest was accepted", n)
		}
	}
	sum := sha256.Sum256([]byte("x"))
	if _, err := g.SealWriter(io.Discard, sum[:8]); err != nil {
		t.Fatal(err)
	}
}
