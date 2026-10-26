package remote

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// A client that cancels an Exec ends its process and everything that
// process started: the request's context is the process's.
func TestCancellingAnExecEndsItsProcessGroup(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"sh", "-c", "echo $$ > pgid; sleep 30 & sleep 30"}})
		done <- err
	}()
	var pgid int
	within(t, 10*time.Second, "the pgid file", func() {
		for {
			b, _ := os.ReadFile(filepath.Join(dir, "pgid"))
			if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && n > 0 {
				pgid = n
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	cancel()
	within(t, 10*time.Second, "Exec after the cancel", func() {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Exec = %v, want context.Canceled", err)
		}
	})
	groupGone(t, pgid)
}

// A Command.Stream whose write fails ends the command, and Exec says
// why.
func TestAFailingStreamEndsAnExec(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	within(t, 10*time.Second, "Exec with a failing stream", func() {
		_, err := ws.Exec(context.Background(), agentworkspace.Command{Args: []string{"sh", "-c", "echo x; sleep 30"}, Stream: failing{}})
		if !errors.Is(err, errWrite) {
			t.Errorf("Exec = %v, want the stream's error", err)
		}
	})
}

var errWrite = errors.New("the stream fails")

type failing struct{}

func (failing) Write([]byte) (int, error) { return 0, errWrite }

// A session that goes its lease unrenewed lapses, and its processes,
// with everything they started, end with it.
func TestALapsedSessionEndsItsProcesses(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	h, url := serve(t, l, WithInsecureNoAuth(), WithLease(300*time.Millisecond))
	ws := dial(t, url)
	_, pgid := groupLeader(t, ws, context.Background())
	// The client goes, unannounced: its renewal and its requests stop.
	clientOf(ws).end()
	groupGone(t, pgid)
	within(t, 10*time.Second, "the session's end", func() {
		for {
			h.mu.Lock()
			n := len(h.sessions)
			h.mu.Unlock()
			if n == 0 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// A session that is renewed lives past its lease.
func TestARenewedSessionLives(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth(), WithLease(150*time.Millisecond))
	ws := dial(t, url)
	p, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	time.Sleep(600 * time.Millisecond)
	if err := p.Signal(syscall.SIGCONT); err != nil {
		t.Errorf("after four leases the process is gone: %v", err)
	}
	if _, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"true"}}); err != nil {
		t.Errorf("after four leases the session is gone: %v", err)
	}
}

// The client's Close ends every process it started, and the session.
func TestClosingTheClientEndsItsProcesses(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	h, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	p, pgid := groupLeader(t, ws, context.Background())
	within(t, 10*time.Second, "the client's close", func() { ws.Close() })
	if status, err := p.Wait(); status != -1 || err != nil {
		t.Errorf("Wait after the client closed = %d, %v", status, err)
	}
	groupGone(t, pgid)
	h.mu.Lock()
	n := len(h.sessions)
	h.mu.Unlock()
	if n != 0 {
		t.Errorf("%d sessions open after the client closed", n)
	}
	for what, err := range map[string]error{
		"write": ws.WriteFile(context.Background(), "x", nil, 0o644),
		"exec": func() error {
			_, err := ws.Exec(context.Background(), agentworkspace.Command{Args: []string{"true"}})
			return err
		}(),
		"start": func() error {
			_, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"true"}})
			return err
		}(),
	} {
		if !errors.Is(err, os.ErrClosed) {
			t.Errorf("%s after Close: %v", what, err)
		}
	}
}

// The handler's Close ends every session's processes, and every Exec
// in flight.
func TestClosingTheHandlerEndsEverySession(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	h, url := serve(t, l, WithInsecureNoAuth())
	_, a := groupLeader(t, dial(t, url), context.Background())
	_, b := groupLeader(t, dial(t, url), context.Background())
	execed := make(chan error, 1)
	go func() {
		_, err := dial(t, url).Exec(context.Background(), agentworkspace.Command{Args: []string{"sh", "-c", "echo $$ > pgid; sleep 30"}})
		execed <- err
	}()
	var c int
	within(t, 10*time.Second, "the pgid file", func() {
		for {
			b, _ := os.ReadFile(filepath.Join(dir, "pgid"))
			if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && n > 0 {
				c = n
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	within(t, 10*time.Second, "the handler's close", func() { h.Close() })
	groupGone(t, a)
	groupGone(t, b)
	groupGone(t, c)
	within(t, 10*time.Second, "the Exec's end", func() {
		if err := <-execed; err == nil {
			t.Error("an Exec the handler's close ended succeeded")
		}
	})
	if _, err := Dial(context.Background(), url); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Dial a closed handler: %v", err)
	}
}

// A process is reached only through the session that started it, by
// the principal that opened that: another principal, and another
// session of the same one, find no such process, and the process runs
// on.
func TestAnotherSessionCannotTouchAProcess(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithAuthenticator(tokens()))
	ctx := context.Background()
	mine := dial(t, url, WithBearerToken(tokAll))
	p, err := agentworkspace.Start(ctx, mine, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	route := p.(*process).route
	pid := route[strings.LastIndex(route, "/")+1:]
	other := clientOf(dial(t, url, WithBearerToken(tokAll)))
	theirs := other.sessionRoute() + "/processes/" + pid
	for _, r := range []struct{ method, suffix string }{
		{"GET", "/wait"},
		{"GET", "/stdout"},
		{"GET", "/stderr"},
		{"POST", "/stdin"},
		{"POST", "/signal"},
		{"POST", "/cancel"},
		{"DELETE", ""},
	} {
		body := func() *bytes.Reader { return bytes.NewReader([]byte(`{"signal":"KILL"}`)) }
		if status, code := raw(t, r.method, url+route+r.suffix, tokBob, body()); status != http.StatusNotFound || code != codeNoSession {
			t.Errorf("another principal's %s %s: %d %q, want no_session", r.method, r.suffix, status, code)
		}
		if status, code := raw(t, r.method, url+theirs+r.suffix, tokAll, body()); status != http.StatusNotFound || code != codeNoProcess {
			t.Errorf("another session's %s %s: %d %q, want no_process", r.method, r.suffix, status, code)
		}
	}
	select {
	case <-p.(*process).done:
		t.Error("the process ended")
	case <-time.After(100 * time.Millisecond):
	}
	if err := p.Signal(syscall.SIGCONT); err != nil {
		t.Errorf("its own session cannot signal it: %v", err)
	}
}

// A started process the start's context ended reports that context's
// error, a deadline included, as Local's does.
func TestTheStartsContextIsWhatWaitReports(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	p, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	within(t, 10*time.Second, "the deadline's end", func() {
		if status, err := p.Wait(); status != -1 || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Wait = %d, %v; want -1, DeadlineExceeded", status, err)
		}
	})
}
