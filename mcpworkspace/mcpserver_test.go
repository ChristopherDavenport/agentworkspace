package mcpworkspace

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// serverEnv makes the test binary an MCP stdio server: the server a
// program starts in the workspace, through Start, to talk to over the
// process's pipes.
const serverEnv = "MCPWORKSPACE_TEST_SERVER"

// tokenEnv gives the test server a secret, which its "token" tool
// tells.
const tokenEnv = "MCPWORKSPACE_TEST_TOKEN"

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
		if tok := os.Getenv(tokenEnv); tok != "" {
			mcp.AddTool(srv, &mcp.Tool{Name: "token", Description: "The server's own secret, as a careless server tells it."},
				func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
					return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "my token is " + tok}}}, struct{}{}, nil
				})
		}
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
		{Server: agentworkspace.Server{Name: "sleeper"}, Command: sleeper},
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
	want := []agentworkspace.Server{{Name: "inner", Description: "the test's server"}, {Name: "noisy"}, {Name: "sleeper"}}
	if len(list) != len(want) || list[0] != want[0] || list[1] != want[1] || list[2] != want[2] {
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

// A name the workspace does not offer is ErrNoServer, and so is a name
// no workspace could offer, refused before it is sent; a server whose
// workspace offers none lists none and starts none.
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
	if _, err := StartServer(context.Background(), c, "Not_A_Name", nil); !errors.Is(err, agentworkspace.ErrNoServer) {
		t.Errorf("StartServer(Not_A_Name) = %v, want ErrNoServer", err)
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

// A started server is a process of its MCP session's: the session's end
// ends it, as it ends a process Start started, with no Close.
func TestAStartedServerEndsWithItsSession(t *testing.T) {
	s := offering(t)
	cs, _ := s.connect(t)
	done, end := context.WithCancel(context.Background())
	c := conn(t, cs)
	c.Done = done
	p, err := StartServer(context.Background(), c, "sleeper", nil)
	if err != nil {
		t.Fatal(err)
	}
	pid := pidOf(t, p)
	end()
	within(t, 15*time.Second, "the client's close", func() { cs.Close() })
	gone(t, pid)
}

// The server half refuses a name the workspace does not offer with
// CodeNoServer, and a name no workspace could offer too, sent as the
// raw request since the client refuses it before sending.
func TestTheServerRefusesANameNotOffered(t *testing.T) {
	s := offering(t)
	cs, _ := s.connect(t)
	ctx := context.Background()
	for _, name := range []string{"absent", "Not_A_Name", ""} {
		_, err := mcp.CallCustomMethod[*ServerStartParams, *StartResult](ctx, cs, wire.MethodServersStart,
			&ServerStartParams{ServerStartArgs: wire.ServerStartArgs{Name: name}})
		var we *jsonrpc.Error
		if !(errors.As(err, &we) && we.Code == wire.CodeNoServer) {
			t.Errorf("servers.start %q = %v, want CodeNoServer", name, err)
		}
	}
}

// lineFilter is an OutputFilter's reader for the test, as a redactor
// is: it holds what it reads until a newline, then gives the line with
// each secret's value written as its name in brackets.
type lineFilter struct {
	r      *bufio.Reader
	values map[string]string
	buf    []byte
	err    error
}

func (f *lineFilter) Read(p []byte) (int, error) {
	for len(f.buf) == 0 {
		if f.err != nil {
			return 0, f.err
		}
		line, err := f.r.ReadString('\n')
		for name, v := range f.values {
			line = strings.ReplaceAll(line, v, "["+name+"]")
		}
		f.buf, f.err = []byte(line), err
	}
	n := copy(p, f.buf)
	f.buf = f.buf[n:]
	return n, nil
}

// A server holding a secret, offered through a filter that holds what
// it reads until a newline, answers over the connection as any other:
// initialize, tools/list and a call pass the filter whole, and the
// secret the server tells comes back as its name.
func TestAFilteredOfferedServerOverTheConnection(t *testing.T) {
	const secret = "tok-5678-fake"
	dir := t.TempDir()
	ws, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	cmd := serverCommand(t)
	cmd.Env = append(cmd.Env, tokenEnv+"="+secret)
	o, err := agentworkspace.Offer(ws, []agentworkspace.OfferedServer{
		{Server: agentworkspace.Server{Name: "secret"}, Command: cmd, Secrets: []string{tokenEnv}},
	}, agentworkspace.WithOutputFilter(func(_ string, values map[string]string, r io.Reader) io.Reader {
		return &lineFilter{r: bufio.NewReader(r), values: values}
	}))
	if err != nil {
		t.Fatal(err)
	}
	s := serving(t, dir, o)
	cs, _ := s.connect(t)
	inner := dial(t, ServerTransport(Servers(conn(t, cs)), "secret", nil))
	tools, err := inner.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 3 || tools.Tools[1].Name != "token" {
		t.Fatalf("tools %+v", tools.Tools)
	}
	if got := callText(t, inner, "token"); got != "my token is ["+tokenEnv+"]" {
		t.Errorf("the token tool said %q, want the secret's name in its place", got)
	}
	if got := callText(t, inner, "where"); got != s.ws.Root() {
		t.Errorf("the server runs in %q, want %q", got, s.ws.Root())
	}
	within(t, 15*time.Second, "the server's end", func() { inner.Close() })
}
