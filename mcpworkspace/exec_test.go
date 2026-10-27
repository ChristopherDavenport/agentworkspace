package mcpworkspace

// The mcpworkspace client's Exec in the Exec table: Local served by
// NewProcesses on an in-memory MCP connection, run as the program at
// the other end runs it.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
)

// execer is Exec over a Conn, as workspacetest.Execer.
type execer struct{ c Conn }

func (e execer) Exec(ctx context.Context, cmd agentworkspace.Command) (*agentworkspace.Output, error) {
	return Exec(ctx, e.c, cmd)
}

func (e execer) Reach() agentworkspace.Reach { return e.c.Reach }

func TestExecConformance(t *testing.T) {
	workspacetest.RunExec(t, workspacetest.ExecBackend{Name: "mcpworkspace", Wire: true, Open: func(t *testing.T, dir string, env []string) workspacetest.Execer {
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
		return execer{c}
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
