package remote

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
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

// The handler's Close ends every session's processes, every Exec in
// flight and every process being closed, and returns only once each
// has ended: each group here is its leader alone, so it is gone the
// moment its leader is reaped.
func TestClosingTheHandlerEndsEverySession(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	h, url := serve(t, l, WithInsecureNoAuth())
	ctx := context.Background()
	_, a := alone(t, dial(t, url), ctx, "")
	_, b := alone(t, dial(t, url), ctx, "")
	// One that ignores its input's end and SIGTERM, whose close takes
	// both graces, is being closed when the handler is.
	closing, c := alone(t, dial(t, url), ctx, "trap '' TERM; ")
	go closing.Close()
	execed := make(chan error, 1)
	go func() {
		_, err := dial(t, url).Exec(ctx, agentworkspace.Command{Args: []string{"sh", "-c", "echo $$ > pgid; exec sleep 30"}})
		execed <- err
	}()
	e := pgidFile(t, filepath.Join(dir, "pgid"))
	time.Sleep(300 * time.Millisecond) // the close is under way
	within(t, 15*time.Second, "the handler's close", func() { h.Close() })
	for name, pgid := range map[string]int{"a session's": a, "another's": b, "a closing": c, "an Exec's": e} {
		if err := syscall.Kill(-pgid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("%s process group runs on after the handler's Close: %v", name, err)
		}
	}
	within(t, 10*time.Second, "the Exec's end", func() {
		if err := <-execed; err == nil {
			t.Error("an Exec the handler's close ended succeeded")
		}
	})
	if _, err := Dial(ctx, url); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Dial a closed handler: %v", err)
	}
}

// alone starts a process that is its group's only member, running
// prefix first, and returns it and the group's id, its first line.
func alone(t *testing.T, ws Workspace, ctx context.Context, prefix string) (agentworkspace.Process, int) {
	t.Helper()
	p, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"sh", "-c", prefix + "echo $$; exec sleep 30"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	var line string
	within(t, 10*time.Second, "the first line", func() {
		line, err = bufio.NewReader(p.Stdout()).ReadString('\n')
	})
	pgid, perr := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || perr != nil {
		t.Fatalf("first line %q: %v %v", line, err, perr)
	}
	return p, pgid
}

// pgidFile waits for a process to write its group's id to name.
func pgidFile(t *testing.T, name string) int {
	t.Helper()
	var pgid int
	within(t, 10*time.Second, "the pgid file", func() {
		for {
			b, _ := os.ReadFile(name)
			if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && n > 0 {
				pgid = n
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	return pgid
}

// A client whose server goes away loses its session once a whole lease
// passes unrenewed, by when the server has ended its processes: Wait
// returns -1 and ErrNoSession, and the streams fail with it.
func TestALostServerLosesTheSession(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	h, err := NewHandler(l, WithInsecureNoAuth(), WithLease(300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { h.Close() })
	ws := dial(t, srv.URL)
	p, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"cat"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	srv.Listener.Close()
	srv.CloseClientConnections()
	within(t, 5*time.Second, "Wait on a lost server", func() {
		if status, err := p.Wait(); status != -1 || !errors.Is(err, ErrNoSession) {
			t.Errorf("Wait = %d, %v; want -1, ErrNoSession", status, err)
		}
	})
	if _, err := p.Stdout().Read(make([]byte, 1)); !errors.Is(err, ErrNoSession) {
		t.Errorf("Stdout after the loss: %v", err)
	}
	if _, err := p.Stdin().Write([]byte("x")); !errors.Is(err, ErrNoSession) {
		t.Errorf("Stdin after the loss: %v", err)
	}
	if clientOf(ws).sessionRoute() != "" {
		t.Error("the lost session is still the client's")
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
	mine := dial(t, url, withToken(tokAll))
	p, err := agentworkspace.Start(ctx, mine, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	go p.Wait() // the client's own wait
	route := p.(*process).route
	pid := route[strings.LastIndex(route, "/")+1:]
	otherWS := dial(t, url, withToken(tokAll))
	q, err := agentworkspace.Start(ctx, otherWS, agentworkspace.Command{Args: []string{"true"}}) // opens its session
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	theirs := clientOf(otherWS).sessionRoute() + "/processes/" + pid
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

// cut fails every request once it is set, as a network that went away.
type cut struct {
	off  *atomic.Bool
	next http.RoundTripper
}

func (c cut) RoundTrip(r *http.Request) (*http.Response, error) {
	if c.off.Load() {
		return nil, errors.New("cut")
	}
	return c.next.RoundTrip(r)
}

// The handler's Close waits for a session that is ending because it
// lapsed, though the lapse took it out of the table first.
func TestClosingTheHandlerWaitsForALapse(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	const lease = 300 * time.Millisecond
	h, url := serve(t, l, WithInsecureNoAuth(), WithLease(lease))
	var off atomic.Bool
	ws := dial(t, url, WithHTTPClient(&http.Client{Transport: cut{&off, http.DefaultTransport}}))
	// It ignores its input's end and SIGTERM, so its end takes both
	// graces.
	_, pgid := alone(t, ws, context.Background(), "trap '' TERM; ")
	off.Store(true)
	time.Sleep(lease + 300*time.Millisecond) // lapsed, and ending
	within(t, 15*time.Second, "the handler's close", func() { h.Close() })
	if err := syscall.Kill(-pgid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("a lapsing session's process runs on after the handler's Close: %v", err)
	}
}

// A process being closed counts against WithMaxProcesses until it has
// exited, and a session being closed against WithMaxSessions until it
// has ended: until then they hold their streams, which the bounds
// count. (sleep ignores its input's close, so its close takes a grace.)
func TestAClosingProcessAndSessionCountUntilTheyEnd(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth(), WithMaxProcesses(1), WithMaxSessions(1))
	ctx := context.Background()
	ws := dial(t, url)
	sleep := agentworkspace.Command{Args: []string{"sleep", "30"}}

	p, err := agentworkspace.Start(ctx, ws, sleep)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { defer close(closed); p.Close() }()
	time.Sleep(300 * time.Millisecond)
	if q, err := agentworkspace.Start(ctx, ws, sleep); !errors.Is(err, ErrLimit) {
		if q != nil {
			q.Close()
		}
		t.Errorf("a start while the only process closes: %v, want ErrLimit", err)
	}
	within(t, 10*time.Second, "the close", func() { <-closed })
	if _, err := agentworkspace.Start(ctx, ws, sleep); err != nil {
		t.Fatalf("a start once the close has ended: %v", err)
	}

	// The client's Close closes its session, which ends q a grace later.
	ended := make(chan struct{})
	go func() { defer close(ended); ws.Close() }()
	time.Sleep(300 * time.Millisecond)
	second := dial(t, url)
	if r, err := agentworkspace.Start(ctx, second, sleep); !errors.Is(err, ErrLimit) {
		if r != nil {
			r.Close()
		}
		t.Errorf("a session opened while the only one ends: %v, want ErrLimit", err)
	}
	within(t, 10*time.Second, "the session's end", func() { <-ended })
	r, err := agentworkspace.Start(ctx, second, sleep)
	if err != nil {
		t.Fatalf("a session opened once the other has ended: %v", err)
	}
	r.Close()
}

// One wait is in flight for a process at a time, as one stream is for
// each pipe: another while one waits is refused, so a process holds at
// most four streams whatever its clients send.
func TestOneWaitAtATime(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	p, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// wait asks for the process's wait for d, and says how it was refused,
	// "" when it was not.
	wait := func(d time.Duration) string {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url+p.(*process).route+"/wait", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		var r errorReply
		decodeReply(resp, &r)
		return r.Error.Code
	}
	// The client's own wait, or this one, holds the claim from here on:
	// the client's until the process exits, this one for three seconds.
	go wait(3 * time.Second)
	time.Sleep(300 * time.Millisecond)
	if code := wait(200 * time.Millisecond); code != codeInvalid {
		t.Errorf("a second wait: %q, want %q", code, codeInvalid)
	}
}
