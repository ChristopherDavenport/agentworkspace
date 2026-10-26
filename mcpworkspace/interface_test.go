package mcpworkspace

// One MCP server, one interface, wherever it runs and whatever wire
// reaches it: the test binary's stdio server, offered by an Offering or
// started as a command, is the same server to a program's MCP client
// through every transport this module and remote give.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// served is ws behind a remote handler whose every request is from one
// principal holding scopes, dialled; all of it closed at the test's end.
func served(t *testing.T, ws agentworkspace.Workspace, scopes ...remote.Scope) remote.Workspace {
	t.Helper()
	h, err := remote.NewHandler(ws, remote.WithAuthenticator(func(*http.Request) (remote.Principal, error) {
		return remote.Principal{Name: "program", Scopes: scopes}, nil
	}))
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

// face is what a program's MCP client sees of a server: who it says it
// is, its tools, and what a call of one gives, each as JSON.
type face struct {
	server, tools, call string
}

// faceOf connects to the server tr reaches and reads its face, calling
// the tool "where"; the connection is closed, and the server with it,
// before it returns.
func faceOf(t *testing.T, tr mcp.Transport) face {
	t.Helper()
	var f face
	within(t, 30*time.Second, "the server's face", func() {
		ctx := context.Background()
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "program", Version: "test"}, nil).Connect(ctx, tr, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer cs.Close()
		js := func(v any) string {
			b, err := json.Marshal(v)
			if err != nil {
				t.Error(err)
			}
			return string(b)
		}
		f.server = js(cs.InitializeResult().ServerInfo)
		tools, err := cs.ListTools(ctx, nil)
		if err != nil {
			t.Error(err)
			return
		}
		f.tools = js(tools.Tools)
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "where"})
		if err != nil {
			t.Error(err)
			return
		}
		f.call = js(res)
	})
	return f
}

// The test's server gives the same initialize, tools/list and
// tools/call whichever way it is reached: offered by an Offering and
// started by name here, over mcpworkspace's connection, over remote to
// a principal holding servers alone, and started as a command on a
// Starter here and over remote. The wire never changes the interface.
func TestOneServerOneInterfaceOverEveryWire(t *testing.T) {
	dir := t.TempDir()
	l, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	o, err := agentworkspace.Offer(l, []agentworkspace.OfferedServer{
		{Server: agentworkspace.Server{Name: "inner", Description: "the test's server"}, Command: serverCommand(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	cs, _ := serving(t, dir, o).connect(t)
	overMCP := Servers(conn(t, cs))
	overRemote, ok := served(t, o, remote.ScopeServers).(agentworkspace.ServerProvider)
	if !ok {
		t.Fatal("remote with servers alone is no ServerProvider")
	}
	starter, ok := served(t, l, remote.ScopeRead, remote.ScopeExec).(agentworkspace.Starter)
	if !ok {
		t.Fatal("remote with exec over Local is no Starter")
	}

	ways := []struct {
		name string
		tr   mcp.Transport
	}{
		{"offered, here", ServerTransport(o, "inner", nil)},
		{"offered, over mcpworkspace", ServerTransport(overMCP, "inner", nil)},
		{"offered, over remote with servers alone", ServerTransport(overRemote, "inner", nil)},
		{"a command, here", CommandTransport(l, serverCommand(t))},
		{"a command, over remote", CommandTransport(starter, serverCommand(t))},
	}
	want := faceOf(t, ways[0].tr)
	root, _ := json.Marshal(l.Root())
	if !strings.Contains(want.tools, `"where"`) || !strings.Contains(want.tools, `"pid"`) || !strings.Contains(want.call, string(root)) {
		t.Fatalf("the server here gave the wrong face: %+v; want its two tools, and where saying %s", want, root)
	}
	for _, w := range ways[1:] {
		t.Run(w.name, func(t *testing.T) {
			if got := faceOf(t, w.tr); got != want {
				t.Errorf("the server's face\n%+v\nwant, as offered here,\n%+v", got, want)
			}
		})
	}
}
