package flashback

import (
	"testing"

	"binlogviz/internal/schemacatalog"
)

func TestRefusalLabelNamesFoldedFileTable(t *testing.T) {
	c := schemacatalog.New("")
	c.LoadFile("USE `P185L`;\nCREATE TABLE `MixT` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `Gc` int GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL,\n" +
		"  PRIMARY KEY (`id`)\n);\n")
	res := c.Resolve("p185l", "mixt")
	if got := refusalLabel(res, "p185l", "mixt"); got != "p185l.mixt (schema file table P185L.MixT, letter case differs)" {
		t.Fatalf("label = %q", got)
	}
	if got := refusalLabel(c.Peek("p185l", "other"), "p185l", "other"); got != "p185l.other" {
		t.Fatalf("plain label = %q", got)
	}
}
