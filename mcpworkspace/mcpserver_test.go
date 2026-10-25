package mcpworkspace

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// serverEnv makes the test binary an MCP stdio server: the server a
// program starts in the workspace, through Start, to talk to over the
// process's pipes.
const serverEnv = "MCPWORKSPACE_TEST_SERVER"

func TestMain(m *testing.M) {
	if os.Getenv(serverEnv) == "1" {
		srv := mcp.NewServer(&mcp.Implementation{Name: "in-the-workspace", Version: "1"}, nil)
		mcp.AddTool(srv, &mcp.Tool{Name: "where", Description: "The server's working directory."},
			func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
				wd, err := os.Getwd()
				if err != nil {
					return nil, struct{}{}, err
				}
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: wd}}}, struct{}{}, nil
			})
		mcp.AddTool(srv, &mcp.Tool{Name: "pid", Description: "The server's process id."},
			func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: strconv.Itoa(os.Getpid())}}}, struct{}{}, nil
			})
		if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// An MCP stdio server started through Start runs in the workspace at
// the other end of the connection, and answers over the process's
// pipes: what a program needs to run a user's MCP server where its
// tools act. Closing the client ends the server.
func TestAnMCPServerRunsThroughStart(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := start(t, conn(t, cs), agentworkspace.Command{Args: []string{exe}, Env: []string{serverEnv + "=1"}})

	ctx := context.Background()
	inner, err := mcp.NewClient(&mcp.Implementation{Name: "program", Version: "test"}, nil).
		Connect(ctx, &mcp.IOTransport{Reader: p.Stdout(), Writer: p.Stdin()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tools, err := inner.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 2 || tools.Tools[0].Name != "pid" || tools.Tools[1].Name != "where" {
		t.Fatalf("tools %+v", tools.Tools)
	}
	res, err := inner.CallTool(ctx, &mcp.CallToolParams{Name: "where"})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Content[0].(*mcp.TextContent).Text; got != s.ws.Root() {
		t.Errorf("the server runs in %q, want the workspace's root %q", got, s.ws.Root())
	}
	within(t, 15*time.Second, "the server's end", func() {
		inner.Close()
		p.Wait()
	})
}

// serverCommand is the test binary as an MCP stdio server.
func serverCommand(t *testing.T) agentworkspace.Command {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return agentworkspace.Command{Args: []string{exe}, Env: []string{serverEnv + "=1"}}
}

// callText calls the tool called name on cs and returns its text.
func callText(t *testing.T, cs *mcp.ClientSession, name string) string {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	return res.Content[0].(*mcp.TextContent).Text
}

// dial connects a client to the server t reaches, closed at the test's
// end.
func dial(t *testing.T, tr mcp.Transport) *mcp.ClientSession {
	t.Helper()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "program", Version: "test"}, nil).Connect(context.Background(), tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

// CommandTransport starts the server in the workspace, at its root, and
// each Connect starts a new one, as a client connecting again after a
// server ended needs. Closing the client ends the server.
func TestCommandTransportStartsTheServerInTheWorkspace(t *testing.T) {
	dir := t.TempDir()
	ws, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	tr := CommandTransport(ws, serverCommand(t))

	// The Connect's context bounds the connect alone, not the server.
	ctx, cancel := context.WithCancel(context.Background())
	first, err := mcp.NewClient(&mcp.Implementation{Name: "program", Version: "test"}, nil).Connect(ctx, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if got := callText(t, first, "where"); got != ws.Root() {
		t.Errorf("the server runs in %q, want the workspace's root %q", got, ws.Root())
	}
	firstPid, _ := strconv.Atoi(callText(t, first, "pid"))
	second := dial(t, tr)
	if secondPid, _ := strconv.Atoi(callText(t, second, "pid")); secondPid == firstPid || secondPid == 0 {
		t.Errorf("the second Connect reached process %d, the first %d; want a new one", secondPid, firstPid)
	}
	within(t, 15*time.Second, "the server's end", func() { first.Close() })
	gone(t, firstPid)
}

// offering serves a workspace on this machine offering the test binary
// as the server "inner", and a server whose standard error says so.
func offering(t *testing.T) *testServer {
	t.Helper()
	dir := t.TempDir()
	ws, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	o, err := agentworkspace.Offer(ws, []agentworkspace.OfferedServer{
		{Server: agentworkspace.Server{Name: "inner", Description: "the test's server"}, Command: serverCommand(t)},
		{Server: agentworkspace.Server{Name: "noisy"}, Command: agentworkspace.Command{Args: []string{"sh", "-c", "echo hello from noisy >&2; exec cat"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return serving(t, dir, o)
}

// The servers a workspace offers are listed over the connection, and
// one started by name through ServerTransport answers as it would here.
func TestOfferedServersOverTheConnection(t *testing.T) {
	s := offering(t)
	cs, _ := s.connect(t)
	c := conn(t, cs)
	list, err := ListServers(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	want := []agentworkspace.Server{{Name: "inner", Description: "the test's server"}, {Name: "noisy"}}
	if len(list) != len(want) || list[0] != want[0] || list[1] != want[1] {
		t.Fatalf("ListServers = %+v, want %+v", list, want)
	}
	inner := dial(t, ServerTransport(Servers(c), "inner", nil))
	if got := callText(t, inner, "where"); got != s.ws.Root() {
		t.Errorf("the server runs in %q, want %q", got, s.ws.Root())
	}
	within(t, 15*time.Second, "the server's end", func() { inner.Close() })
}

// A started server's standard error comes back to the writer it was
// started with, and the server is a process of the session's.
func TestAnOfferedServersStandardError(t *testing.T) {
	s := offering(t)
	cs, _ := s.connect(t)
	var stderr lockedBuffer
	p, err := StartServer(context.Background(), conn(t, cs), "noisy", &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ProcessID(p); !ok {
		t.Error("a started server is not a process of the connection's")
	}
	within(t, 10*time.Second, "the server's standard error", func() {
		for !strings.Contains(stderr.String(), "hello from noisy") {
			time.Sleep(20 * time.Millisecond)
		}
	})
	within(t, 10*time.Second, "the close", func() { p.Close() })
}

// A name the workspace does not offer is ErrNoServer, a name no
// workspace could offer is refused before it is sent, and a server
// whose workspace offers none lists none and starts none.
func TestServersNotOffered(t *testing.T) {
	s := offering(t)
	cs, _ := s.connect(t)
	c := conn(t, cs)
	if p, err := StartServer(context.Background(), c, "absent", nil); !errors.Is(err, agentworkspace.ErrNoServer) {
		if p != nil {
			p.Close()
		}
		t.Errorf("StartServer(absent) = %v, want ErrNoServer", err)
	}
	if _, err := StartServer(context.Background(), c, "Not_A_Name", nil); err == nil || errors.Is(err, agentworkspace.ErrNoServer) {
		t.Errorf("StartServer(Not_A_Name) = %v, want a refusal of the name", err)
	}

	plain := newTestServer(t)
	pcs, _ := plain.connect(t)
	pc := conn(t, pcs)
	if list, err := ListServers(context.Background(), pc); err != nil || len(list) != 0 {
		t.Errorf("ListServers of a workspace offering none = %+v, %v; want none", list, err)
	}
	if p, err := StartServer(context.Background(), pc, "inner", nil); !errors.Is(err, agentworkspace.ErrNoServer) {
		if p != nil {
			p.Close()
		}
		t.Errorf("StartServer on a workspace offering none = %v, want ErrNoServer", err)
	}
}
