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
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
)

// execer is Exec over a Conn, as workspacetest.Execer.
type execer struct{ c Conn }

func (e execer) Exec(ctx context.Context, cmd agentworkspace.Command) (*agentworkspace.Output, error) {
	return Exec(ctx, e.c, cmd)
}

func TestExecConformance(t *testing.T) {
	workspacetest.RunExec(t, workspacetest.ExecBackend{Name: "mcpworkspace", Wire: true, Open: func(t *testing.T, dir string, env []string) workspacetest.Execer {
		l, err := agentworkspace.NewLocal(dir, env)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		cs, _ := serving(t, dir, l).connect(t)
		return execer{conn(t, cs)}
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
