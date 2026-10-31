package binlogviz

import (
	"context"
	"errors"
	"testing"

	"binlogviz/internal/clierr"
	"binlogviz/internal/remote"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestAnalyzeMariaDBGTIDReachesRemoteConfig(t *testing.T) {
	previous := remoteOpenSource
	defer func() { remoteOpenSource = previous }()
	for _, tc := range []struct {
		input string
		want  string
	}{
		{"0-0-0", ""},
		{"0-0-0,1-2-3", ""},
		{"0-0-0,0-0-1", ""},
		{"1-2-3,,2-3-4", ""},
		{"0-1-0", "0-1-0"},
		{"1-0-0", "1-0-0"},
		{"1-1-0", "1-1-0"},
		{"0-1-0,1-0-0,2-3-4", "0-1-0,1-0-0,2-3-4"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			var got remote.Config
			called := false
			remoteOpenSource = func(_ context.Context, cfg remote.Config) (remoteSource, error) {
				got, called = cfg, true
				return nil, errors.New("offline test: connection not attempted")
			}
			cmd := newAnalyzeCommand()
			cmd.SetArgs([]string{"--host", "db", "--user", "replica", "--start-gtid", tc.input})
			err := cmd.Execute()
			if tc.want == "" {
				if called || clierrKind(t, err) != clierr.KindUsage {
					t.Fatalf("invalid GTID reached remote open: called=%t, err=%v", called, err)
				}
				return
			}
			if !called || got.Flavor != "mariadb" || got.GTIDSet == nil {
				t.Fatalf("remote config: called=%t, config=%+v, err=%v", called, got, err)
			}
			if text := got.GTIDSet.String(); text != tc.want {
				t.Fatalf("remote GTID state = %q, want %q", text, tc.want)
			}
		})
	}
}

func TestAnalyzeMySQLGTIDIntervalsReachRemoteConfig(t *testing.T) {
	const want = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-2:5"
	var got remote.Config
	previous := remoteOpenSource
	remoteOpenSource = func(_ context.Context, cfg remote.Config) (remoteSource, error) {
		got = cfg
		return nil, errors.New("offline test: connection not attempted")
	}
	defer func() { remoteOpenSource = previous }()
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"--host", "db", "--user", "replica", "--start-gtid", want})
	_ = cmd.Execute()
	if got.Flavor != "mysql" || got.GTIDSet == nil {
		t.Fatalf("remote config = %+v", got)
	}
	if text := got.GTIDSet.String(); text != want {
		t.Fatalf("remote GTID state = %q, want %q", text, want)
	}
	decoded, err := mysql.DecodeMysqlGTIDSet(got.GTIDSet.Encode())
	if err != nil || !got.GTIDSet.Equal(decoded) {
		t.Fatalf("encoded remote GTID state did not round trip: %v", err)
	}
}
