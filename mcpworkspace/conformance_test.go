package mcpworkspace

// mcpworkspace's client in the server tables: an Offering over Local
// served by NewProcesses on an in-memory MCP connection, and Servers
// over that connection, as the program at the other end sees it.

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
)

// ownedServers is the servers at the other end of a connection, with
// the connection's close as the owner's: ending Done and closing the
// session, whose end ends every server the session started.
type ownedServers struct {
	agentworkspace.ServerProvider
	close func()
}

func (o ownedServers) Close() error {
	o.close()
	return nil
}

func TestServerProviderConformance(t *testing.T) {
	workspacetest.RunServerProvider(t, workspacetest.ServerProvider{Name: "mcpworkspace", Open: func(t *testing.T, dir string, env []string, servers []agentworkspace.OfferedServer, opts ...agentworkspace.OfferOption) agentworkspace.ServerProvider {
		l, err := agentworkspace.NewLocal(dir, env)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		o, err := agentworkspace.Offer(l, servers, opts...)
		if err != nil {
			t.Fatal(err)
		}
		cs, _ := serving(t, dir, o).connect(t)
		c := conn(t, cs)
		done, end := context.WithCancel(c.Done)
		t.Cleanup(end)
		c.Done = done
		return ownedServers{Servers(c), func() { end(); cs.Close() }}
	}})
}
