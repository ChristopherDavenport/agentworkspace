package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/internal/wireerr"
)

// failingExec is a Local whose Exec fails with err.
type failingExec struct {
	*agentworkspace.Local
	err error
}

func (f failingExec) Exec(context.Context, agentworkspace.Command) (*agentworkspace.Output, error) {
	return nil, f.err
}

// A served refusal's own text never crosses, however a path is written
// in it: one of no kind is wireerr.Other, one of a kind its words, and
// a cancellation that wraps text its own words (RFC 0001 §15).
func TestNoServedTextCrosses(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"after a newline":        {errors.New("runtime failed:\n/srv/hidden/creds missing"), wireerr.Other},
		"after a tab":            {errors.New("runtime failed:\t/srv/hidden/creds missing"), wireerr.Other},
		"as key=/path":           {errors.New("mount source=/srv/hidden/creds failed"), wireerr.Other},
		"with spaces in it":      {errors.New("open /srv/my hidden dir/creds: boom"), wireerr.Other},
		"as a file:// URL":       {errors.New("get file:///srv/hidden/creds: boom"), wireerr.Other},
		"in a refusal of a kind": {fmt.Errorf("open /srv/hidden/creds: %w", agentworkspace.ErrOutside), agentworkspace.ErrOutside.Error()},
	} {
		t.Run(name, func(t *testing.T) {
			l, _ := newLocal(t, pathEnv())
			_, url := serve(t, failingExec{l, tc.err}, WithInsecureNoAuth())
			_, err := dial(t, url).Exec(context.Background(), agentworkspace.Command{Args: []string{"x"}})
			if err == nil || strings.Contains(err.Error(), "hidden") || !strings.HasSuffix(err.Error(), tc.want) {
				t.Errorf("Exec = %v, want the words %q alone", err, tc.want)
			}
		})
	}
}

// A root of "/" is withheld from a principal without read as any other
// root is: the client's Root is "".
func TestARootOfSlashIsWithheld(t *testing.T) {
	l, err := agentworkspace.NewLocal("/", nil)
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { l.Close() })
	_, url := serve(t, l, WithAuthenticator(grant(map[string]Principal{"s": {Name: "s", Scopes: []Scope{ScopeServers}}})))
	ws := dial(t, url, withToken("s"))
	if ws.Root() != "" || ws.Served().Root != "" || ws.Descriptor().Root != "" {
		t.Errorf("a root of / told a principal without read: Root %q, Served %q, Descriptor %q", ws.Root(), ws.Served().Root, ws.Descriptor().Root)
	}
}

// The corpus compares JSON values with their types.
func TestJSONEqualKeepsTypes(t *testing.T) {
	for _, p := range [][2]string{
		{`{"exitCode":0}`, `{"exitCode":"0"}`},
		{`{"args":["a","b"]}`, `{"args":["a b"]}`},
		{`{"timedOut":true}`, `{"timedOut":"true"}`},
		{`{"stdout":"b2s="}`, `{"stdout":["b2s="]}`},
	} {
		if jsonEqual(json.RawMessage(p[0]), json.RawMessage(p[1])) {
			t.Errorf("jsonEqual(%s, %s) = true", p[0], p[1])
		}
	}
	if !jsonEqual(json.RawMessage(`{"a":1,"b":[true]}`), json.RawMessage(`{"b":[true],"a":1}`)) {
		t.Error("jsonEqual is not structural")
	}
}
