package agentworkspace

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
)

// start starts c in l, failing the test if it cannot, and closes the
// process when the test ends.
func start(t *testing.T, ctx context.Context, l *Local, c Command) Process {
	t.Helper()
	p, err := l.Start(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// within fails the test if f takes longer than d, as a process that
// was not ended would.
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

// A started process is a conversation: a line written to its input is
// answered on its output before the next is written, as an MCP stdio
// server answers a request.
func TestStartEchoesThroughThePipes(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	p := start(t, context.Background(), l, Command{Args: []string{"cat"}})
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
func TestStartRunsInTheWorkspace(t *testing.T) {
	t.Setenv("AGENTWORKSPACE_SECRET", "hunter2")
	l, dir := local(t, []string{"A=1", "PATH=" + os.Getenv("PATH")})
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	var stderr bytes.Buffer
	p := start(t, context.Background(), l, Command{
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
func TestStartIsKilledWithItsContext(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	ctx, cancel := context.WithCancel(context.Background())
	p := start(t, ctx, l, Command{Args: []string{"sh", "-c", "sleep 30 & echo started; wait"}})
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
func TestStartTimeout(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	p := start(t, context.Background(), l, Command{Args: []string{"sleep", "30"}, Timeout: 200 * time.Millisecond})
	within(t, 10*time.Second, "the timeout", func() {
		if status, err := p.Wait(); status != -1 || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Wait = %d, %v; want -1, DeadlineExceeded", status, err)
		}
	})
}

// Close ends a process that leaves when its input closes at once, and
// one that does not after the grace period, with what it started.
func TestStartClose(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	ctx := context.Background()

	polite := start(t, ctx, l, Command{Args: []string{"cat"}})
	began := time.Now()
	within(t, 10*time.Second, "closing cat", func() { polite.Close() })
	if status, err := polite.Wait(); status != 0 || err != nil {
		t.Errorf("cat after Close: Wait = %d, %v", status, err)
	}
	if d := time.Since(began); d >= grace {
		t.Errorf("cat, which leaves at end of input, took %v to close", d)
	}

	stubborn, err := l.Start(ctx, Command{Args: []string{"sh", "-c", "sleep 30 & echo started; wait"}})
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
func TestStartSignal(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	p := start(t, context.Background(), l, Command{Args: []string{"sleep", "30"}})
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
func TestStartKillsWhatTheProcessLeft(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	p := start(t, context.Background(), l, Command{Args: []string{"sh", "-c", "sleep 30 & echo started"}})
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
func TestClosingTheWorkspaceEndsItsProcesses(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var ps []Process
	for range 2 {
		p, err := l.Start(ctx, Command{Args: []string{"sleep", "30"}})
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
	if _, err := l.Start(ctx, Command{Args: []string{"true"}}); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("Start on a closed workspace: %v", err)
	}
}

// Start refuses what it cannot do before starting anything.
func TestStartRefuses(t *testing.T) {
	dir, _ := confined(t)
	l := mustLocal(t, dir)
	ctx := context.Background()
	for name, c := range map[string]Command{
		"no command":     {},
		"a Dir out":      {Args: []string{"true"}, Dir: ".."},
		"a Dir via link": {Args: []string{"true"}, Dir: "linkdir"},
		"Stdin bytes":    {Args: []string{"cat"}, Stdin: []byte("x")},
	} {
		if p, err := l.Start(ctx, c); err == nil {
			p.Close()
			t.Errorf("%s: started", name)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if p, err := l.Start(cctx, Command{Args: []string{"true"}}); !errors.Is(err, context.Canceled) {
		if p != nil {
			p.Close()
		}
		t.Errorf("a cancelled context: %v", err)
	}
}

// Start is optional: the package's Start runs on a workspace that
// offers it and says a workspace that does not cannot.
func TestStartIsOptional(t *testing.T) {
	dir, _ := confined(t)
	l := mustLocal(t, dir)
	p, err := Start(context.Background(), l, Command{Args: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	m := &mapped{backing: l, dir: l.Root()}
	if _, err := Start(context.Background(), m, Command{Args: []string{"true"}}); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), KindContainer) {
		t.Errorf("Start on a workspace that cannot: %v", err)
	}
}

// Many processes started and ended at once share the workspace safely.
func TestStartConcurrently(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			p, err := l.Start(context.Background(), Command{Args: []string{"cat"}})
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
