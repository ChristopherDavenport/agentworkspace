package tunnel

// The tunnel in the conformance tables: a Hub in httptest, Serve over
// Local dialling it, and the hub's Workspace for it.

import (
	"bytes"
	"context"
	"io/fs"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
)

// noLinks is a workspace whose FS cannot tell a link from what it
// leads to, so the tunnel's cannot either.
type noLinks struct{ *agentworkspace.Local }

func (n noLinks) FS() fs.FS { return struct{ fs.FS }{n.Local.FS()} }

// tunnelled is ws served through a tunnel with every scope, as the
// hub's client sees it.
func tunnelled(t *testing.T, ws agentworkspace.Workspace) agentworkspace.Workspace {
	t.Helper()
	r := newRig(t)
	r.serve(t, ws, WithScopes(allScopes...), bearer(tokA), fast)
	return r.workspace(t, "a")
}

func TestConformance(t *testing.T) {
	workspacetest.Run(t,
		workspacetest.Backend{Name: "tunnel", Links: true, Open: func(t *testing.T, dir string) agentworkspace.Workspace {
			return tunnelled(t, local(t, dir, pathEnv()))
		}},
		workspacetest.Backend{Name: "tunnel, no links", Open: func(t *testing.T, dir string) agentworkspace.Workspace {
			return tunnelled(t, noLinks{local(t, dir, pathEnv())})
		}},
	)
}

// The server tables run on an Offering over Local through a tunnel:
// with every scope, and with servers alone, whose workspace at the hub
// is a ServerProvider and no Starter.
func TestServerProviderConformance(t *testing.T) {
	open := func(scopes ...remote.Scope) func(*testing.T, string, []string, []agentworkspace.OfferedServer, ...agentworkspace.OfferOption) agentworkspace.ServerProvider {
		return func(t *testing.T, dir string, env []string, servers []agentworkspace.OfferedServer, opts ...agentworkspace.OfferOption) agentworkspace.ServerProvider {
			o, err := agentworkspace.Offer(local(t, dir, env), servers, opts...)
			if err != nil {
				t.Fatal(err)
			}
			r := newRig(t)
			r.serve(t, o, WithScopes(scopes...), bearer(tokA), fast)
			ws := r.workspace(t, "a")
			sp, ok := ws.(agentworkspace.ServerProvider)
			if !ok {
				t.Fatalf("a tunnel over an Offering, with %v, is not a ServerProvider", scopes)
			}
			if _, starter := ws.(agentworkspace.Starter); starter != slices.Contains(scopes, remote.ScopeExec) {
				t.Fatalf("a tunnel over an Offering, with %v: a Starter is %v", scopes, starter)
			}
			return sp
		}
	}
	workspacetest.RunServerProvider(t,
		workspacetest.ServerProvider{Name: "tunnel", Open: open(allScopes...)},
		workspacetest.ServerProvider{Name: "tunnel, servers alone", Open: open(remote.ScopeServers)},
	)
}

func TestStarterConformance(t *testing.T) {
	workspacetest.RunStarter(t, workspacetest.Starter{Name: "tunnel", Open: func(t *testing.T, dir string, env []string) agentworkspace.Starter {
		s, ok := tunnelled(t, local(t, dir, env)).(agentworkspace.Starter)
		if !ok {
			t.Fatal("a tunnel over Local with exec is not a Starter")
		}
		return s
	}})
}

// A process's standard error reaches its Stream to the end when it is
// closed at once after its start, over the tunnel as over remote's
// HTTP/1: Close waits for the stream to open, and what the process
// writes on its way out is sent before the stream is cut.
func TestStandardErrorOutlivesAClose(t *testing.T) {
	s := tunnelled(t, local(t, t.TempDir(), pathEnv())).(agentworkspace.Starter)
	for i := range 50 {
		var stream lockedBuffer
		p, err := s.Start(context.Background(), agentworkspace.Command{Args: []string{"sh", "-c", "read x; echo bye >&2"}, Stream: &stream})
		if err != nil {
			t.Fatal(err)
		}
		within(t, 10*time.Second, "the close", func() { p.Close() })
		if got := stream.String(); got != "bye\n" {
			t.Fatalf("close %d: the stream has %q, want %q", i, got, "bye\n")
		}
	}
}

// lockedBuffer is a bytes.Buffer safe for a stream's writer and the
// test reading it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
