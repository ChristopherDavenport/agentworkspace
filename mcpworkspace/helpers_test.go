package mcpworkspace

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// testServer is a workspace on this machine served over MCP: its files
// and, when the workspace can start one, its processes.
type testServer struct {
	dir   string
	ws    agentworkspace.Workspace
	procs *Processes // nil when ws is not a Starter
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	dir := t.TempDir()
	ws, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	return serving(t, dir, ws)
}

// serving serves ws, whose root is dir.
func serving(t *testing.T, dir string, ws agentworkspace.Workspace) *testServer {
	t.Helper()
	s := &testServer{dir: dir, ws: ws}
	if _, ok := ws.(agentworkspace.Starter); ok {
		s.procs = NewProcesses(ws)
		t.Cleanup(s.procs.Close)
	}
	return s
}

// server is a new MCP server for one session, as a program makes one.
func (s *testServer) server(t *testing.T) *mcp.Server {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "workspace", Version: "test"}, &mcp.ServerOptions{
		Capabilities: &mcp.ServerCapabilities{Resources: &mcp.ResourceCapabilities{}},
	})
	ServeFiles(srv, s.ws.FS())
	if s.procs != nil {
		if err := s.procs.Register(srv); err != nil {
			t.Fatal(err)
		}
	}
	return srv
}

// connect is a client session of a new server session, both closed at
// the test's end.
func (s *testServer) connect(t *testing.T) (*mcp.ClientSession, *mcp.ServerSession) {
	t.Helper()
	return connectTo(t, s.server(t))
}

func connectTo(t *testing.T, srv *mcp.Server) (*mcp.ClientSession, *mcp.ServerSession) {
	t.Helper()
	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := mcp.NewClient(&mcp.Implementation{Name: "client", Version: "test"}, nil)
	if err := RegisterClient(c); err != nil {
		t.Fatal(err)
	}
	cs, err := c.Connect(ctx, ct, nil)
	if err != nil {
		ss.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close(); ss.Close() })
	return cs, ss
}

// conn is cs as a Conn with the default bounds and a Done ended at the
// test's end, before cs is closed (connectTo's cleanup runs after it).
func conn(t *testing.T, cs *mcp.ClientSession) Conn {
	t.Helper()
	done, end := context.WithCancel(context.Background())
	t.Cleanup(end)
	return Conn{Session: cs, Bounds: wire.DefaultBounds(), Done: done}
}

// start is Start of cmd over c, failing the test on an error; the
// process is closed at the test's end.
func start(t *testing.T, c Conn, cmd agentworkspace.Command) agentworkspace.Process {
	t.Helper()
	p, err := Start(context.Background(), c, cmd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// idOf is p's id at the server.
func idOf(t *testing.T, p agentworkspace.Process) string {
	t.Helper()
	id, ok := ProcessID(p)
	if !ok {
		t.Fatalf("%T is not a process Start started", p)
	}
	return id
}

// lockedBuffer is a stream a process's standard error is written to
// while the test reads it.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// sleeper is a process that prints its pid and then does nothing,
// whatever its input does, until it is ended.
var sleeper = agentworkspace.Command{Args: []string{"sh", "-c", "echo $$; exec sleep 1000"}}

// pidOf reads the pid sleeper prints.
func pidOf(t *testing.T, p agentworkspace.Process) int {
	t.Helper()
	line, err := bufio.NewReader(p.Stdout()).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// gone waits up to 10 seconds for pid to have exited and been reaped.
func gone(t *testing.T, pid int) {
	t.Helper()
	for range 200 {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("process %d is still running", pid)
}

// within fails the test if f takes longer than d.
func within(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s took longer than %v", what, d)
	}
}
