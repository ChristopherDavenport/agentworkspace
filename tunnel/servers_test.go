package tunnel

import (
	"bufio"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
)

// A Hub carries the servers a workspace offers as remote does: the
// workspace at the hub lists them and starts one by name when the
// workspace end grants servers, and is a Starter only when it grants
// exec, so read and servers alone start the servers and run no
// command.
func TestAHubCarriesOfferedServers(t *testing.T) {
	for _, c := range []struct {
		name            string
		scopes          []remote.Scope
		starter, server bool
	}{
		{"servers alone", []remote.Scope{remote.ScopeRead, remote.ScopeServers}, false, true},
		{"exec alone", []remote.Scope{remote.ScopeRead, remote.ScopeExec}, true, false},
		{"both", allScopes, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			l := local(t, t.TempDir(), pathEnv())
			o, err := agentworkspace.Offer(l, []agentworkspace.OfferedServer{
				{Server: agentworkspace.Server{Name: "echo", Description: "says it back"}, Command: agentworkspace.Command{Args: []string{"cat"}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			r := newRig(t)
			r.serve(t, o, WithScopes(c.scopes...), bearer(tokA), fast)
			ws := r.workspace(t, "a")
			_, starter := ws.(agentworkspace.Starter)
			sp, server := ws.(agentworkspace.ServerProvider)
			if starter != c.starter || server != c.server {
				t.Fatalf("%T: a Starter %v, a ServerProvider %v; want %v, %v", ws, starter, server, c.starter, c.server)
			}
			ctx := context.Background()
			if !c.starter {
				if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, remote.ErrForbidden) {
					t.Errorf("Exec without exec = %v, want ErrForbidden", err)
				}
			}
			if !c.server {
				return
			}
			want := []agentworkspace.Server{{Name: "echo", Description: "says it back"}}
			if list, err := sp.Servers(ctx); err != nil || !slices.Equal(list, want) {
				t.Errorf("Servers = %+v, %v", list, err)
			}
			if p, err := sp.StartServer(ctx, "nope", nil); !errors.Is(err, agentworkspace.ErrNoServer) {
				if p != nil {
					p.Close()
				}
				t.Errorf("StartServer of a name not offered = %v, want ErrNoServer", err)
			}
			p, err := sp.StartServer(ctx, "echo", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			within(t, 10*time.Second, "the conversation", func() {
				io.WriteString(p.Stdin(), "ping\n")
				if got, err := bufio.NewReader(p.Stdout()).ReadString('\n'); got != "ping\n" || err != nil {
					t.Errorf("answer = %q, %v", got, err)
				}
				p.Stdin().Close()
				if status, err := p.Wait(); status != 0 || err != nil {
					t.Errorf("Wait = %d, %v", status, err)
				}
			})
		})
	}
}
