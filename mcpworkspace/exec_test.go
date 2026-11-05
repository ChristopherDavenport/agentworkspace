package mcpworkspace

// The mcpworkspace client's Exec in the Exec table: Local served by
// NewProcesses on an in-memory MCP connection, run as the program at
// the other end runs it.

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// execer is Exec over a Conn, as workspacetest.Execer.
type execer struct{ c Conn }

func (e execer) Exec(ctx context.Context, cmd agentworkspace.Command) (*agentworkspace.Output, error) {
	return Exec(ctx, e.c, cmd)
}

func (e execer) Reach() agentworkspace.Reach { return e.c.Reach }

func TestExecConformance(t *testing.T) {
	// The Local each execer runs its commands in, for CloseServed.
	var served sync.Map // execer -> *agentworkspace.Local
	workspacetest.RunExec(t, workspacetest.ExecBackend{Name: "mcpworkspace", Wire: true, CloseServed: func(t *testing.T, ws workspacetest.Execer) {
		l, ok := served.Load(ws)
		if !ok {
			t.Fatal("no served workspace kept for this execer")
		}
		l.(*agentworkspace.Local).Close()
	}, Open: func(t *testing.T, dir string, env []string) workspacetest.Execer {
		l, err := agentworkspace.NewLocal(dir, env)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		cs, _ := serving(t, dir, l).connect(t)
		c := conn(t, cs)
		reach, bounds, err := ProcessInfo(context.Background(), cs)
		if err != nil {
			t.Fatal(err)
		}
		c.Reach, c.Bounds = reach, bounds
		e := &execer{c}
		served.Store(e, l)
		return e
	}})
}

var errWrite = errors.New("the stream fails")

type failing struct{}

func (failing) Write([]byte) (int, error) { return 0, errWrite }

// A Stream whose write fails ends the command, and Exec says why; a
// command that cannot start is Start's refusal, of its kind.
func TestExecRefusals(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	c := conn(t, cs)
	within(t, 10*time.Second, "Exec with a failing stream", func() {
		if _, err := Exec(context.Background(), c, agentworkspace.Command{Args: []string{"sh", "-c", "echo x; sleep 30"}, Stream: failing{}}); !errors.Is(err, errWrite) {
			t.Errorf("Exec = %v, want the stream's error", err)
		}
	})
	if _, err := Exec(context.Background(), c, agentworkspace.Command{Args: []string{"pwd"}, Dir: "../x"}); !errors.Is(err, agentworkspace.ErrOutside) {
		t.Errorf("a Dir outside: %v", err)
	}
	if _, err := Exec(context.Background(), c, agentworkspace.Command{}); err == nil {
		t.Error("no command ran")
	}
}

// The server says its workspace's reach and its bounds; one that says
// no reach says none.
func TestTheServersReachCrosses(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	reach, bounds, err := ProcessInfo(context.Background(), cs)
	if want := agentworkspace.ReachOf(s.ws); err != nil || reach != want || want == "" || bounds != wire.DefaultBounds() {
		t.Errorf("ProcessInfo = %q, %+v, %v; want %q and the default bounds", reach, bounds, err, want)
	}
	l := s.ws.(*agentworkspace.Local)
	hidden := struct{ agentworkspace.Starter }{l}
	cs, _ = serving(t, s.dir, hidden).connect(t)
	if reach, _, err := ProcessInfo(context.Background(), cs); err != nil || reach != "" {
		t.Errorf("ProcessInfo of a workspace that says no reach = %q, %v", reach, err)
	}
}

// A connect carries a stream to a port on the workspace's loopback as a
// process of the session: what is written arrives, CloseWrite is end of
// file there, and the answer is read.
func TestAConnectCrosses(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	c := conn(t, cs)
	pc, err := Connect(context.Background(), c, workspacetest.Upper(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	within(t, 10*time.Second, "the stream", func() {
		io.WriteString(pc, "hello")
		if err := pc.CloseWrite(); err != nil {
			t.Error(err)
		}
		if got, err := io.ReadAll(pc); string(got) != "HELLO" || err != nil {
			t.Errorf("the port answered %q, %v", got, err)
		}
	})
	if _, err := Connect(context.Background(), c, 70000); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("port 70000: %v", err)
	}
}

// Against a server that does not answer process.cancel (an older one),
// the end of Exec's context closes the process instead, as Start's end
// does, and Exec returns the interrupted result beside ctx's error.
func TestExecInterruptsWithoutTheCancelMethod(t *testing.T) {
	dir := t.TempDir()
	l, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	p := NewProcesses(l)
	t.Cleanup(p.Close)
	srv := mcp.NewServer(&mcp.Implementation{Name: "older", Version: "test"}, nil)
	c0 := p.cfg
	if err := errors.Join(
		mcp.AddReceivingCustomMethod(srv, wire.MethodStart, answering(c0, p.start)),
		mcp.AddReceivingCustomMethod(srv, wire.MethodWrite, answering(c0, p.write)),
		mcp.AddReceivingCustomMethod(srv, wire.MethodRead, answering(c0, p.read)),
		mcp.AddReceivingCustomMethod(srv, wire.MethodCloseStdin, answering(c0, p.closeStdin)),
		mcp.AddReceivingCustomMethod(srv, wire.MethodSignal, answering(c0, p.signal)),
		mcp.AddReceivingCustomMethod(srv, wire.MethodWait, answering(c0, p.wait)),
		mcp.AddReceivingCustomMethod(srv, wire.MethodClose, answering(c0, p.close)),
	); err != nil {
		t.Fatal(err)
	}
	cs, _ := connectTo(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	within(t, 10*time.Second, "Exec after its context's end", func() {
		out, err := Exec(ctx, conn(t, cs), agentworkspace.Command{Args: []string{"sleep", "15"}})
		if !errors.Is(err, context.DeadlineExceeded) || out == nil || !out.Interrupted || out.ExitCode != -1 {
			t.Errorf("Exec = %+v, %v; want ExitCode -1 and Interrupted beside the context's error", out, err)
		}
	})
}
