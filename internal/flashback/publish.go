package flashback

import (
	"bufio"
	"io"
	"os"
)

// Publish writes a script to a temp file and copies it to dst only after
// write returns nil. On any error the temp file is removed and dst receives
// nothing. A failure of the copy itself can still leave a prefix on dst when
// dst is a shell redirect; that prefix has no commit trailer (ADR 0013).
func Publish(dst io.Writer, write func(io.Writer) error) error {
	f, err := os.CreateTemp("", "binlogviz-flashback-script-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	bw := bufio.NewWriterSize(f, 1<<16)
	if err := write(bw); err != nil {
		_ = f.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := io.Copy(dst, f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
