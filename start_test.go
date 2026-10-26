package agentworkspace_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
)

// Local runs the Start tables; remote runs them from its own tests,
// and every nested module's Starter from its.
func TestStarterConformance(t *testing.T) {
	workspacetest.RunStarter(t, workspacetest.Starter{Name: "local", Open: func(t *testing.T, dir string, env []string) agentworkspace.Starter {
		l, err := agentworkspace.NewLocal(dir, env)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		return l
	}})
}

// An Offering over Local runs the server tables; remote and tunnel
// run them over one served.
func TestServerProviderConformance(t *testing.T) {
	workspacetest.RunServerProvider(t, workspacetest.ServerProvider{Name: "offering", Open: func(t *testing.T, dir string, env []string, servers []agentworkspace.OfferedServer, opts ...agentworkspace.OfferOption) agentworkspace.ServerProvider {
		l, err := agentworkspace.NewLocal(dir, env)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		o, err := agentworkspace.Offer(l, servers, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return o
	}})
}

// Start is optional: the package's Start runs on a workspace that
// offers it and says a workspace that does not cannot.
func TestStartIsOptional(t *testing.T) {
	dir, _ := workspacetest.Confined(t)
	l := mustLocal(t, dir)
	p, err := agentworkspace.Start(context.Background(), l, agentworkspace.Command{Args: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	m := &mapped{backing: l, dir: l.Root()}
	if _, err := agentworkspace.Start(context.Background(), m, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), agentworkspace.KindContainer) {
		t.Errorf("Start on a workspace that cannot: %v", err)
	}
}
