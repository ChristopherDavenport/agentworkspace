package tunnel

// The tunnel in the conformance tables: a Hub in httptest, Serve over
// Local dialling it, and the hub's Workspace for it.

import (
	"io/fs"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
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

func TestStarterConformance(t *testing.T) {
	workspacetest.RunStarter(t, workspacetest.Starter{Name: "tunnel", Open: func(t *testing.T, dir string, env []string) agentworkspace.Starter {
		s, ok := tunnelled(t, local(t, dir, env)).(agentworkspace.Starter)
		if !ok {
			t.Fatal("a tunnel over Local with exec is not a Starter")
		}
		return s
	}})
}
