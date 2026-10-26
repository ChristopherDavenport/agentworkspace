package remote_test

// The remote backend in the conformance tables: remote.Dial over
// remote.NewHandler serving Local, through httptest.

import (
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
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

// The server tables run on an Offering over Local, served: to a
// principal holding every scope, a StarterServerClient, and to one
// holding read and servers alone, a ServerClient, which is no Starter.
func TestServerProviderConformance(t *testing.T) {
	open := func(scopes ...remote.Scope) func(*testing.T, string, []string, []agentworkspace.OfferedServer, ...agentworkspace.OfferOption) agentworkspace.ServerProvider {
		return func(t *testing.T, dir string, env []string, servers []agentworkspace.OfferedServer, opts ...agentworkspace.OfferOption) agentworkspace.ServerProvider {
			o, err := agentworkspace.Offer(local(t, dir, env), servers, opts...)
			if err != nil {
				t.Fatal(err)
			}
			c := servedAs(t, o, scopes)
			sp, ok := c.(agentworkspace.ServerProvider)
			if !ok {
				t.Fatalf("a remote over an Offering, with %v, is not a ServerProvider", scopes)
			}
			if _, starter := c.(agentworkspace.Starter); starter != slices.Contains(scopes, remote.ScopeExec) {
				t.Fatalf("a remote over an Offering, with %v: a Starter is %v", scopes, starter)
			}
			return sp
		}
	}
	workspacetest.RunServerProvider(t,
		workspacetest.ServerProvider{Name: "remote", Open: open(remote.ScopeRead, remote.ScopeWrite, remote.ScopeExec, remote.ScopeServers)},
		workspacetest.ServerProvider{Name: "remote, servers alone", Open: open(remote.ScopeRead, remote.ScopeServers)},
	)
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
	return servedWith(t, ws, remote.WithInsecureNoAuth())
}

// servedAs is served, every request from one principal holding scopes.
func servedAs(t *testing.T, ws agentworkspace.Workspace, scopes []remote.Scope) remote.Workspace {
	t.Helper()
	return servedWith(t, ws, remote.WithAuthenticator(func(*http.Request) (remote.Principal, error) {
		return remote.Principal{Name: "client", Scopes: slices.Clone(scopes)}, nil
	}))
}

func servedWith(t *testing.T, ws agentworkspace.Workspace, opts ...remote.Option) remote.Workspace {
	t.Helper()
	h, err := remote.NewHandler(ws, opts...)
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
