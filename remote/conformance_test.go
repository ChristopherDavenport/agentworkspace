package remote_test

// The remote backend in the conformance tables: remote.Dial over
// remote.NewHandler serving Local, through httptest.

import (
	"context"
	"io/fs"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
)

func TestConformance(t *testing.T) {
	workspacetest.Run(t,
		workspacetest.Backend{Name: "remote", Links: true, Open: func(t *testing.T, dir string) agentworkspace.Workspace {
			return served(t, local(t, dir, pathEnv()))
		}},
		workspacetest.Backend{Name: "remote, no links", Open: func(t *testing.T, dir string) agentworkspace.Workspace {
			return served(t, noLinks{local(t, dir, pathEnv())})
		}},
	)
}

func TestStarterConformance(t *testing.T) {
	workspacetest.RunStarter(t, workspacetest.Starter{Name: "remote", Open: func(t *testing.T, dir string, env []string) agentworkspace.Starter {
		s, ok := served(t, local(t, dir, env)).(agentworkspace.Starter)
		if !ok {
			t.Fatal("a remote over Local is not a Starter")
		}
		return s
	}})
}

func pathEnv() []string { return []string{"PATH=" + os.Getenv("PATH")} }

func local(t *testing.T, dir string, env []string) *agentworkspace.Local {
	t.Helper()
	l, err := agentworkspace.NewLocal(dir, env)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// noLinks is a workspace whose FS cannot tell a link from what it
// leads to, so the remote's cannot either.
type noLinks struct{ *agentworkspace.Local }

func (n noLinks) FS() fs.FS { return struct{ fs.FS }{n.Local.FS()} }

// served is ws served over httptest and dialled, unauthenticated; the
// client, the handler and the server are closed when the test ends, in
// that order, before ws.
func served(t *testing.T, ws agentworkspace.Workspace) remote.Workspace {
	t.Helper()
	h, err := remote.NewHandler(ws, remote.WithInsecureNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { h.Close() })
	c, err := remote.Dial(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
