package tunnel

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
)

// pidOf starts cmd, which must print its pid first, and reads it: the
// process's stdout is open from then on.
func pidOf(t *testing.T, s agentworkspace.Starter, cmd string) (agentworkspace.Process, *bufio.Reader, int) {
	t.Helper()
	p, err := s.Start(context.Background(), agentworkspace.Command{Args: []string{"sh", "-c", cmd}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	out := bufio.NewReader(p.Stdout())
	var line string
	within(t, 10*time.Second, "the pid", func() { line, err = out.ReadString('\n') })
	pid, perr := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || perr != nil {
		t.Fatalf("first line %q: %v %v", line, err, perr)
	}
	return p, out, pid
}

// A drop shorter than the lease is survived: the host dials again, the
// session is renewed through the new connection past its lease, and the
// processes started before the drop are alive, their Signal, Wait and
// Close working, and a pipe first opened after the drop working. A pipe
// open across the drop fails at the server: it was one stream, which
// is not resumed.
func TestADropShorterThanTheLeaseIsSurvived(t *testing.T) {
	const lease = time.Second
	r := newRig(t)
	r.serve(t, local(t, t.TempDir(), pathEnv()),
		WithScopes(allScopes...), bearer(tokA), fast,
		WithHandlerOptions(remote.WithLease(lease)))
	s := r.workspace(t, "a").(agentworkspace.Starter)

	// Its output streamed across the drop.
	streamed, out, pid := pidOf(t, s, "echo $$; exec sleep 30")
	// Nothing of it streamed before the drop.
	signalled, err := s.Start(context.Background(), agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { signalled.Close() })
	echo, err := s.Start(context.Background(), agentworkspace.Command{Args: []string{"cat"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { echo.Close() })

	old := r.drop(t, "a")
	r.reconnected(t, "a", old)

	// Twice the lease on the new connection: renewals reach it.
	time.Sleep(2 * lease)
	if !alive(pid) {
		t.Fatal("a process started before the drop was ended")
	}
	within(t, 10*time.Second, "the stream cut by the drop", func() {
		if _, err := out.ReadString('\n'); err == nil || errors.Is(err, io.EOF) {
			t.Errorf("a stream open across the drop read on: %v", err)
		}
	})
	within(t, 10*time.Second, "the pipes opened after the drop", func() {
		if _, err := io.WriteString(echo.Stdin(), "after\n"); err != nil {
			t.Fatal(err)
		}
		if got, err := bufio.NewReader(echo.Stdout()).ReadString('\n'); err != nil || got != "after\n" {
			t.Errorf("echoed %q, %v", got, err)
		}
	})
	if err := signalled.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Signal after the drop: %v", err)
	}
	within(t, 10*time.Second, "Wait after the drop", func() {
		if status, err := signalled.Wait(); status != -1 || err != nil {
			t.Errorf("Wait = %d, %v; want -1, nil", status, err)
		}
	})
	within(t, 15*time.Second, "Close after the drop", func() {
		if err := streamed.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	eventually(t, 10*time.Second, "the closed process's end", func() bool { return !alive(pid) })
}

// A drop longer than the lease ends the session's processes at the
// workspace end, and the server's client sees ErrNoSession; the next
// Start opens another session.
func TestADropLongerThanTheLeaseEndsTheProcesses(t *testing.T) {
	const lease = 500 * time.Millisecond
	r := newRig(t)
	r.serve(t, local(t, t.TempDir(), pathEnv()),
		WithScopes(allScopes...), bearer(tokA), fast,
		WithHandlerOptions(remote.WithLease(lease)))
	s := r.workspace(t, "a").(agentworkspace.Starter)
	p, _, pid := pidOf(t, s, "echo $$; exec sleep 30")

	r.shut.Store(true) // the host cannot dial again
	old := r.drop(t, "a")
	eventually(t, 10*time.Second, "the lapsed session's process's end", func() bool { return !alive(pid) })
	r.shut.Store(false)
	r.reconnected(t, "a", old)

	within(t, 10*time.Second, "Wait on a lost session", func() {
		if status, err := p.Wait(); status != -1 || !errors.Is(err, remote.ErrNoSession) {
			t.Errorf("Wait = %d, %v; want -1, ErrNoSession", status, err)
		}
	})
	again, err := s.Start(context.Background(), agentworkspace.Command{Args: []string{"true"}})
	if err != nil {
		t.Fatalf("a Start after the lapse: %v", err)
	}
	defer again.Close()
	within(t, 10*time.Second, "the new session's process", func() {
		if status, err := again.Wait(); status != 0 || err != nil {
			t.Errorf("Wait = %d, %v", status, err)
		}
	})
}

// A request made while the workspace is away waits for it, bounded by
// its context: ErrNotConnected past that, and served once the host is
// back.
func TestARequestWaitsForTheReconnect(t *testing.T) {
	r := newRig(t)
	dir := t.TempDir()
	l := local(t, dir, pathEnv())
	l.WriteFile(context.Background(), "f.txt", []byte("hi"), 0o644)
	r.serve(t, l, WithScopes(remote.ScopeRead), bearer(tokA), fast)
	ws := r.workspace(t, "a")

	r.shut.Store(true)
	r.drop(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Exec while away: %v, want its context's end", err)
	}
	if _, err := r.hub.Workspace(ctx, "a"); !errors.Is(err, ErrNotConnected) {
		t.Errorf("Workspace while away: %v, want ErrNotConnected", err)
	}
	got := make(chan error, 1)
	go func() {
		_, err := agentworkspace.Read(ws, "f.txt", 10)
		got <- err
	}()
	time.Sleep(100 * time.Millisecond)
	r.shut.Store(false)
	select {
	case err := <-got:
		if err != nil {
			t.Errorf("a read made while away: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a read made while away was not served after the reconnect")
	}
}
