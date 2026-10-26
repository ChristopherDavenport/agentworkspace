package workspacetest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/internal/lifetime"
)

// Starter is a Starter the Start tables run on. Open opens one over
// dir with env as its environment, and closes it when the test ends.
type Starter struct {
	Name string
	Open func(t *testing.T, dir string, env []string) agentworkspace.Starter
}

// grace is Local's, the one value both use: how long a process's end
// waits for it to exit after its input closes, and again after SIGTERM.
// A process that leaves at the end of its input closes well inside it.
const grace = lifetime.Grace

// RunStarter runs every Start table on every starter, each a subtest
// named after the table and then the starter. It calls t.Setenv, so it
// cannot run under t.Parallel, nor can the test that calls it.
func RunStarter(t *testing.T, starters ...Starter) {
	path := []string{"PATH=" + os.Getenv("PATH")}
	for _, table := range []struct {
		name   string
		env    []string
		secret bool // this process's environment holds a secret the started one must not see
		run    func(t *testing.T, s agentworkspace.Starter, dir string)
	}{
		{"EchoesThroughThePipes", path, false, startEchoes},
		{"RunsInTheWorkspace", []string{"A=1", "PATH=" + os.Getenv("PATH")}, true, startRunsInTheWorkspace},
		{"IsKilledWithItsContext", path, false, startIsKilledWithItsContext},
		{"Timeout", path, false, startTimeout},
		{"Close", path, false, startClose},
		{"Signal", path, false, startSignal},
		{"KillsWhatTheProcessLeft", path, false, startKillsWhatTheProcessLeft},
		{"ClosingTheWorkspaceEndsItsProcesses", path, false, closingTheWorkspaceEndsItsProcesses},
		{"Concurrently", path, false, startConcurrently},
	} {
		t.Run(table.name, func(t *testing.T) {
			if table.secret {
				t.Setenv("AGENTWORKSPACE_SECRET", "hunter2")
			}
			for _, st := range starters {
				t.Run(st.Name, func(t *testing.T) {
					dir := t.TempDir()
					table.run(t, st.Open(t, dir, table.env), dir)
				})
			}
		})
	}
	t.Run("Refuses", func(t *testing.T) {
		for _, st := range starters {
			t.Run(st.Name, func(t *testing.T) {
				dir, _ := Confined(t)
				startRefuses(t, st.Open(t, dir, path))
			})
		}
	})
}

// start starts c in s, failing the test if it cannot, and closes the
// process when the test ends.
func start(t *testing.T, ctx context.Context, s agentworkspace.Starter, c agentworkspace.Command) agentworkspace.Process {
	t.Helper()
	p, err := s.Start(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// A started process is a conversation: a line written to its input is
// answered on its output before the next is written, as an MCP stdio
// server answers a request.
func startEchoes(t *testing.T, l agentworkspace.Starter, _ string) {
	p := start(t, context.Background(), l, agentworkspace.Command{Args: []string{"cat"}})
	out := bufio.NewReader(p.Stdout())
	within(t, 10*time.Second, "the conversation", func() {
		for _, line := range []string{`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, "second"} {
			if _, err := io.WriteString(p.Stdin(), line+"\n"); err != nil {
				t.Error(err)
				return
			}
			got, err := out.ReadString('\n')
			if err != nil || got != line+"\n" {
				t.Errorf("answer = %q, %v; want %q", got, err, line)
				return
			}
		}
		p.Stdin().Close()
		if rest, err := io.ReadAll(out); err != nil || len(rest) != 0 {
			t.Errorf("after end of input: %q, %v", rest, err)
		}
		if status, err := p.Wait(); status != 0 || err != nil {
			t.Errorf("Wait = %d, %v", status, err)
		}
	})
}

// The process runs in the directory and with the environment of the
// workspace, Command's added, and nothing of this process's.
func startRunsInTheWorkspace(t *testing.T, l agentworkspace.Starter, dir string) {
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	var stderr bytes.Buffer
	p := start(t, context.Background(), l, agentworkspace.Command{
		Args:   []string{"sh", "-c", "pwd; echo $A$B[$AGENTWORKSPACE_SECRET]; echo to-stderr >&2; exit 3"},
		Dir:    "sub",
		Env:    []string{"B=2"},
		Stream: &stderr,
	})
	var got []byte
	within(t, 10*time.Second, "the process", func() {
		got, _ = io.ReadAll(p.Stdout())
	})
	if want := filepath.Join(dir, "sub") + "\n12[]\n"; string(got) != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if status, err := p.Wait(); status != 3 || err != nil {
		t.Errorf("Wait = %d, %v; want 3", status, err)
	}
	// Wait returns once standard error is copied.
	if stderr.String() != "to-stderr\n" {
		t.Errorf("Stream got %q", stderr.String())
	}
	// Wait answers the same every time.
	if status, err := p.Wait(); status != 3 || err != nil {
		t.Errorf("second Wait = %d, %v", status, err)
	}
}

// The end of the context kills the process and everything it started:
// the child that holds the output open dies too, so the output ends.
func startIsKilledWithItsContext(t *testing.T, l agentworkspace.Starter, _ string) {
	ctx, cancel := context.WithCancel(context.Background())
	p := start(t, ctx, l, agentworkspace.Command{Args: []string{"sh", "-c", "sleep 30 & echo started; wait"}})
	out := bufio.NewReader(p.Stdout())
	if line, err := out.ReadString('\n'); err != nil || line != "started\n" {
		t.Fatalf("first line = %q, %v", line, err)
	}
	cancel()
	within(t, 10*time.Second, "the end after cancel", func() {
		if _, err := io.ReadAll(out); err != nil {
			t.Errorf("reading to the end: %v", err)
		}
		if status, err := p.Wait(); status != -1 || !errors.Is(err, context.Canceled) {
			t.Errorf("Wait = %d, %v; want -1, context.Canceled", status, err)
		}
	})
	if err := p.Signal(syscall.SIGTERM); !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("Signal after the end = %v", err)
	}
}

// Command.Timeout ends a started process as the context's deadline
// does.
func startTimeout(t *testing.T, l agentworkspace.Starter, _ string) {
	p := start(t, context.Background(), l, agentworkspace.Command{Args: []string{"sleep", "30"}, Timeout: 200 * time.Millisecond})
	within(t, 10*time.Second, "the timeout", func() {
		if status, err := p.Wait(); status != -1 || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Wait = %d, %v; want -1, DeadlineExceeded", status, err)
		}
	})
}

// Close ends a process that leaves when its input closes at once, and
// one that does not after the grace period, with what it started.
func startClose(t *testing.T, l agentworkspace.Starter, _ string) {
	ctx := context.Background()

	polite := start(t, ctx, l, agentworkspace.Command{Args: []string{"cat"}})
	began := time.Now()
	within(t, 10*time.Second, "closing cat", func() { polite.Close() })
	if status, err := polite.Wait(); status != 0 || err != nil {
		t.Errorf("cat after Close: Wait = %d, %v", status, err)
	}
	if d := time.Since(began); d >= grace {
		t.Errorf("cat, which leaves at end of input, took %v to close", d)
	}

	stubborn, err := l.Start(ctx, agentworkspace.Command{Args: []string{"sh", "-c", "sleep 30 & echo started; wait"}})
	if err != nil {
		t.Fatal(err)
	}
	out := bufio.NewReader(stubborn.Stdout())
	if line, err := out.ReadString('\n'); err != nil || line != "started\n" {
		t.Fatalf("first line = %q, %v", line, err)
	}
	within(t, 10*time.Second, "closing a process that ignores its input", func() { stubborn.Close() })
	if status, err := stubborn.Wait(); status != -1 || err != nil {
		t.Errorf("after Close: Wait = %d, %v; want -1, nil", status, err)
	}
	if err := stubborn.Close(); err != nil {
		t.Errorf("a second Close: %v", err)
	}
}

// Signal reaches the process.
func startSignal(t *testing.T, l agentworkspace.Starter, _ string) {
	p := start(t, context.Background(), l, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err := p.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	within(t, 10*time.Second, "the signalled process", func() {
		if status, err := p.Wait(); status != -1 || err != nil {
			t.Errorf("Wait = %d, %v; want -1, nil", status, err)
		}
	})
}

// When the process exits by itself, what it left running goes with
// it: the background child holding the output is killed, so the output
// ends.
func startKillsWhatTheProcessLeft(t *testing.T, l agentworkspace.Starter, _ string) {
	p := start(t, context.Background(), l, agentworkspace.Command{Args: []string{"sh", "-c", "sleep 30 & echo started"}})
	within(t, 10*time.Second, "the output's end", func() {
		got, err := io.ReadAll(p.Stdout())
		if err != nil || string(got) != "started\n" {
			t.Errorf("output = %q, %v", got, err)
		}
		if status, err := p.Wait(); status != 0 || err != nil {
			t.Errorf("Wait = %d, %v", status, err)
		}
	})
}

// The workspace's Close ends what it started, and a closed workspace
// starts nothing.
func closingTheWorkspaceEndsItsProcesses(t *testing.T, l agentworkspace.Starter, _ string) {
	ctx := context.Background()
	var ps []agentworkspace.Process
	for range 2 {
		p, err := l.Start(ctx, agentworkspace.Command{Args: []string{"sleep", "30"}})
		if err != nil {
			t.Fatal(err)
		}
		ps = append(ps, p)
	}
	within(t, 10*time.Second, "closing the workspace", func() { l.Close() })
	for _, p := range ps {
		if status, err := p.Wait(); status != -1 || err != nil {
			t.Errorf("Wait after the workspace closed = %d, %v", status, err)
		}
	}
	if _, err := l.Start(ctx, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("Start on a closed workspace: %v", err)
	}
}

// Start refuses what it cannot do before starting anything.
func startRefuses(t *testing.T, l agentworkspace.Starter) {
	ctx := context.Background()
	for name, c := range map[string]struct {
		cmd  agentworkspace.Command
		want error // nil: any error
	}{
		"no command":     {agentworkspace.Command{}, nil},
		"a Dir out":      {agentworkspace.Command{Args: []string{"true"}, Dir: ".."}, agentworkspace.ErrOutside},
		"a Dir via link": {agentworkspace.Command{Args: []string{"true"}, Dir: "linkdir"}, agentworkspace.ErrOutside},
		"Stdin bytes":    {agentworkspace.Command{Args: []string{"cat"}, Stdin: []byte("x")}, nil},
	} {
		p, err := l.Start(ctx, c.cmd)
		switch {
		case err == nil:
			p.Close()
			t.Errorf("%s: started", name)
		case c.want != nil && !errors.Is(err, c.want):
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if p, err := l.Start(cctx, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, context.Canceled) {
		if p != nil {
			p.Close()
		}
		t.Errorf("a cancelled context: %v", err)
	}
}

// Many processes started and ended at once share the workspace safely.
func startConcurrently(t *testing.T, l agentworkspace.Starter, _ string) {
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			p, err := l.Start(context.Background(), agentworkspace.Command{Args: []string{"cat"}})
			if err != nil {
				t.Error(err)
				return
			}
			defer p.Close()
			msg := strings.Repeat("x", i) + "\n"
			io.WriteString(p.Stdin(), msg)
			p.Stdin().Close()
			if got, _ := io.ReadAll(p.Stdout()); string(got) != msg {
				t.Errorf("process %d echoed %q", i, got)
			}
		})
	}
	wg.Wait()
}
