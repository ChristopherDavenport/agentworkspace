package mcpworkspace

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// A process started over the connection is an agentworkspace.Process:
// what is written to its input comes back from cat, a write larger than
// one request included, the end of its input ends it, and its status
// is its own. It runs at the workspace's root, with the directory and
// environment the start gave, and a directory outside is refused.
func TestAProcessOverTheConnectionEchoes(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	c := conn(cs)
	p := start(t, c, agentworkspace.Command{Args: []string{"cat"}})

	big := bytes.Repeat([]byte("0123456789abcdef"), (wire.MaxWriteBytes+wire.MaxWriteBytes/2)/16)
	var got bytes.Buffer
	read := make(chan error, 1)
	go func() {
		_, err := io.Copy(&got, p.Stdout())
		read <- err
	}()
	if n, err := p.Stdin().Write(big); err != nil || n != len(big) {
		t.Fatalf("write: %d, %v", n, err)
	}
	if err := p.Stdin().Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), big) {
		t.Errorf("read back %d bytes, want the %d written", got.Len(), len(big))
	}
	if status, err := p.Wait(); status != 0 || err != nil {
		t.Errorf("wait: %d, %v", status, err)
	}

	if err := os.Mkdir(filepath.Join(s.dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	q := start(t, c, agentworkspace.Command{Args: []string{"sh", "-c", "pwd; echo $MCPWS_TEST; exit 3"}, Dir: "sub", Env: []string{"MCPWS_TEST=here"}})
	out, err := io.ReadAll(q.Stdout())
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(s.ws.Root(), "sub") + "\nhere\n"; string(out) != want {
		t.Errorf("output %q, want %q", out, want)
	}
	if status, err := q.Wait(); status != 3 || err != nil {
		t.Errorf("wait: %d, %v", status, err)
	}
	if _, err := Start(context.Background(), c, agentworkspace.Command{Args: []string{"true"}, Dir: ".."}); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Errorf("a directory outside the workspace: %v", err)
	}
}

// Standard error is read as fd 2 and written to the start's stream, the
// part no read took coming with the close; without a stream it is not
// kept.
func TestAProcessStandardErrorReachesTheStream(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	var stream lockedBuffer
	p := start(t, conn(cs), agentworkspace.Command{Args: []string{"sh", "-c", "echo oops >&2; echo out"}, Stream: &stream})
	out, err := io.ReadAll(p.Stdout())
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "out\n" {
		t.Errorf("stdout %q", out)
	}
	p.Wait()
	p.Close()
	if got := stream.String(); got != "oops\n" {
		t.Errorf("stderr %q, want oops", got)
	}

	q := start(t, conn(cs), agentworkspace.Command{Args: []string{"sh", "-c", "echo oops >&2"}})
	q.Wait()
	if _, err := mcp.CallCustomMethod[*ReadParams, *ReadResult](context.Background(), cs, wire.MethodRead, &ReadParams{ReadArgs: wire.ReadArgs{ID: idOf(t, q), FD: 2}}); err == nil {
		t.Error("read fd 2 of a process started without a stream")
	}
}

// Close ends a process that ignores its input, and Signal reaches it;
// both leave Wait with how it ended. A signal the wire has no name for
// is refused.
func TestClosingAProcessKillsIt(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	p := start(t, conn(cs), sleeper)
	pid := pidOf(t, p)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	gone(t, pid)
	if status, _ := p.Wait(); status != -1 {
		t.Errorf("status %d, want -1 (a signal)", status)
	}

	q := start(t, conn(cs), sleeper)
	pid = pidOf(t, q)
	if err := q.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if status, err := q.Wait(); status != -1 || err != nil {
		t.Errorf("wait: %d, %v", status, err)
	}
	gone(t, pid)
	if err := q.Signal(syscall.SIGTERM); !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("signal after exit: %v", err)
	}
	if err := q.Signal(syscall.SIGHUP); err == nil {
		t.Error("SIGHUP was sent")
	}
}

// The end of the start's context ends the process, and Wait says so.
func TestAProcessEndsWithItsContext(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	ctx, cancel := context.WithCancel(context.Background())
	p, err := Start(ctx, conn(cs), sleeper)
	if err != nil {
		t.Fatal(err)
	}
	pid := pidOf(t, p)
	cancel()
	if _, err := p.Wait(); !errors.Is(err, context.Canceled) {
		t.Errorf("wait: %v, want canceled", err)
	}
	gone(t, pid)
}

// A session's end, the client gone, ends every process it started. The
// client ends Done first: a session's close waits for its requests in
// flight, and a process's wait is one.
func TestASessionsEndKillsItsProcesses(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	done, end := context.WithCancel(context.Background())
	c := conn(cs)
	c.Done = done
	pid := pidOf(t, start(t, c, sleeper))
	end()
	within(t, 15*time.Second, "the client's close", func() { cs.Close() })
	gone(t, pid)
}

// End and Close end the processes and the reads waiting on them, so a
// session's close, which waits for the requests in flight, returns; a
// start after Close is refused.
func TestCloseEndsTheProcessesAndTheirReads(t *testing.T) {
	for _, how := range []string{"End", "Close"} {
		t.Run(how, func(t *testing.T) {
			s := newTestServer(t)
			cs, ss := s.connect(t)
			p := start(t, conn(cs), sleeper)
			pid := pidOf(t, p)
			go io.Copy(io.Discard, p.Stdout()) // a read waiting for output
			time.Sleep(50 * time.Millisecond)
			if how == "End" {
				s.procs.End(ss)
			} else {
				s.procs.Close()
			}
			within(t, 15*time.Second, "the session's close", func() { ss.Close() })
			gone(t, pid)
			if how == "Close" {
				cs2, _ := s.connect(t)
				if _, err := Start(context.Background(), conn(cs2), agentworkspace.Command{Args: []string{"true"}}); err == nil || !strings.Contains(err.Error(), "stopping") {
					t.Errorf("a start after Close: %v", err)
				}
			}
		})
	}
}

// A session sees only its own processes: an id another session started
// is ErrNoProcess for every method, and the process goes on.
func TestAnotherSessionsProcessIsRefused(t *testing.T) {
	s := newTestServer(t)
	a, _ := s.connect(t)
	b, _ := s.connect(t)
	p := start(t, conn(a), agentworkspace.Command{Args: []string{"cat"}})
	id := idOf(t, p)
	ctx := context.Background()
	pid := wire.ProcessID{ID: id}
	for name, call := range map[string]func() error{
		wire.MethodWrite: func() error {
			_, err := mcp.CallCustomMethod[*WriteParams, *EmptyResult](ctx, b, wire.MethodWrite, &WriteParams{WriteArgs: wire.WriteArgs{ID: id, Data: []byte("x")}})
			return err
		},
		wire.MethodRead: func() error {
			_, err := mcp.CallCustomMethod[*ReadParams, *ReadResult](ctx, b, wire.MethodRead, &ReadParams{ReadArgs: wire.ReadArgs{ID: id, FD: 1}})
			return err
		},
		wire.MethodCloseStdin: func() error {
			_, err := mcp.CallCustomMethod[*IDParams, *EmptyResult](ctx, b, wire.MethodCloseStdin, &IDParams{ProcessID: pid})
			return err
		},
		wire.MethodSignal: func() error {
			_, err := mcp.CallCustomMethod[*SignalParams, *SignalResult](ctx, b, wire.MethodSignal, &SignalParams{SignalArgs: wire.SignalArgs{ID: id, Signal: wire.SignalKILL}})
			return err
		},
		wire.MethodWait: func() error {
			_, err := mcp.CallCustomMethod[*IDParams, *ExitResult](ctx, b, wire.MethodWait, &IDParams{ProcessID: pid})
			return err
		},
		wire.MethodClose: func() error {
			_, err := mcp.CallCustomMethod[*IDParams, *ExitResult](ctx, b, wire.MethodClose, &IDParams{ProcessID: pid})
			return err
		},
	} {
		if err := call(); err == nil || !strings.Contains(err.Error(), ErrNoProcess.Error()) {
			t.Errorf("%s of another session's process: %v", name, err)
		}
	}
	if _, err := p.Stdin().Write([]byte("still\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(p.Stdout()).ReadString('\n')
	if err != nil || line != "still\n" {
		t.Errorf("after the other session's calls: %q, %v", line, err)
	}
}

// A session has at most MaxProcesses open; a write is at most
// MaxWriteBytes and a read's reply at most MaxReadBytes, whatever it
// asks; and a start, a fd and a signal must be ones the methods know.
// The client writes no more than the server's bounds say.
func TestTheProcessMethodsAreBounded(t *testing.T) {
	s := newTestServer(t)
	cs, _ := s.connect(t)
	c := conn(cs)
	ctx := context.Background()
	var open []agentworkspace.Process
	for range wire.MaxProcesses {
		open = append(open, start(t, c, agentworkspace.Command{Args: []string{"cat"}}))
	}
	if _, err := Start(ctx, c, agentworkspace.Command{Args: []string{"cat"}}); err == nil || !strings.Contains(err.Error(), "32 processes") {
		t.Errorf("start %d: %v", wire.MaxProcesses+1, err)
	}
	if err := open[0].Close(); err != nil {
		t.Fatal(err)
	}
	p := start(t, c, agentworkspace.Command{Args: []string{"cat"}})
	for _, q := range open[1:] {
		q.Close()
	}

	id := idOf(t, p)
	if _, err := mcp.CallCustomMethod[*WriteParams, *EmptyResult](ctx, cs, wire.MethodWrite, &WriteParams{WriteArgs: wire.WriteArgs{ID: id, Data: make([]byte, wire.MaxWriteBytes+1)}}); err == nil {
		t.Error("a write over MaxWriteBytes")
	}
	if _, err := p.Stdin().Write(bytes.Repeat([]byte("x"), 3*wire.MaxReadBytes)); err != nil {
		t.Fatal(err)
	}
	res, err := mcp.CallCustomMethod[*ReadParams, *ReadResult](ctx, cs, wire.MethodRead, &ReadParams{ReadArgs: wire.ReadArgs{ID: id, FD: 1, Max: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Data) == 0 || len(res.Data) > wire.MaxReadBytes {
		t.Errorf("a read asking 1 MiB gave %d bytes, want 1 to %d", len(res.Data), wire.MaxReadBytes)
	}
	for name, call := range map[string]func() error{
		"fd 3": func() error {
			_, err := mcp.CallCustomMethod[*ReadParams, *ReadResult](ctx, cs, wire.MethodRead, &ReadParams{ReadArgs: wire.ReadArgs{ID: id, FD: 3}})
			return err
		},
		"HUP": func() error {
			_, err := mcp.CallCustomMethod[*SignalParams, *SignalResult](ctx, cs, wire.MethodSignal, &SignalParams{SignalArgs: wire.SignalArgs{ID: id, Signal: "HUP"}})
			return err
		},
		"no command": func() error {
			_, err := mcp.CallCustomMethod[*StartParams, *StartResult](ctx, cs, wire.MethodStart, &StartParams{})
			return err
		},
		"a negative timeout": func() error {
			_, err := mcp.CallCustomMethod[*StartParams, *StartResult](ctx, cs, wire.MethodStart, &StartParams{StartArgs: wire.StartArgs{Args: []string{"true"}, TimeoutMs: -1}})
			return err
		},
		"an environment entry without =": func() error {
			_, err := mcp.CallCustomMethod[*StartParams, *StartResult](ctx, cs, wire.MethodStart, &StartParams{StartArgs: wire.StartArgs{Args: []string{"true"}, Env: []string{"X"}}})
			return err
		},
	} {
		if err := call(); err == nil {
			t.Errorf("%s was taken", name)
		}
	}
	for name, cmd := range map[string]agentworkspace.Command{
		"Command.Stdin": {Args: []string{"cat"}, Stdin: []byte("x")},
		"no command":    {},
	} {
		if _, err := Start(ctx, c, cmd); err == nil {
			t.Errorf("a start with %s", name)
		}
	}
	if _, err := Start(ctx, Conn{Session: cs, Bounds: wire.Bounds{MaxProcesses: 1, MaxReadBytes: 1}}, agentworkspace.Command{Args: []string{"true"}}); err == nil {
		t.Error("a start under a bound of zero")
	}

	// A server that keeps smaller bounds than the constants gets writes
	// no larger: a write of 10 bytes under a bound of 3 is four requests.
	var mu sync.Mutex
	var writes []int
	srv := s.server(t)
	srv.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == wire.MethodWrite {
				var w wire.WriteArgs
				raw, _ := json.Marshal(req.GetParams())
				json.Unmarshal(raw, &w)
				mu.Lock()
				writes = append(writes, len(w.Data))
				mu.Unlock()
			}
			return next(ctx, method, req)
		}
	})
	small, _ := connectTo(t, srv)
	q := start(t, Conn{Session: small, Bounds: wire.Bounds{MaxProcesses: 1, MaxWriteBytes: 3, MaxReadBytes: 2}}, agentworkspace.Command{Args: []string{"cat"}})
	if _, err := q.Stdin().Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	q.Stdin().Close()
	out, err := io.ReadAll(q.Stdout())
	if err != nil || string(out) != "0123456789" {
		t.Errorf("read back %q, %v", out, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []int{3, 3, 3, 1}; !slices.Equal(writes, want) {
		t.Errorf("writes of %v bytes, want %v", writes, want)
	}
}

// A workspace that cannot start a process is refused with
// errors.ErrUnsupported, and nothing runs.
func TestAWorkspaceThatCannotStartRefuses(t *testing.T) {
	dir := t.TempDir()
	ws, err := agentworkspace.NewLocal(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	s := &testServer{dir: dir, ws: notStarter{ws}, procs: NewProcesses(notStarter{ws})}
	cs, _ := s.connect(t)
	sentinel := filepath.Join(dir, "started")
	_, err = Start(context.Background(), conn(cs), agentworkspace.Command{Args: []string{"touch", sentinel}})
	if err == nil || !strings.Contains(err.Error(), errors.ErrUnsupported.Error()) {
		t.Errorf("start: %v", err)
	}
	if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the sentinel ran: %v", err)
	}
}

// notStarter is a workspace that cannot start a process.
type notStarter struct{ agentworkspace.Workspace }

// When the connection's Done ends with its Lost cause, a read waiting
// on the process fails through Err, as the loss it is; when it ends
// with any other cause, the read is the process's close.
func TestTheConnectionsEndEndsTheRequests(t *testing.T) {
	lost := errors.New("lost")
	for _, tc := range []struct {
		name  string
		cause error
		want  func(error) bool
	}{
		{"lost", lost, func(err error) bool { return errors.Is(err, lost) && strings.Contains(err.Error(), "mapped") }},
		{"closed", errors.New("closed"), func(err error) bool { return errors.Is(err, os.ErrClosed) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t)
			cs, _ := s.connect(t)
			done, end := context.WithCancelCause(context.Background())
			c := conn(cs)
			c.Done, c.Lost = done, lost
			c.Err = func(err error) error {
				if errors.Is(context.Cause(done), lost) {
					return &mapped{err: lost}
				}
				return err
			}
			p := start(t, c, sleeper)
			pidOf(t, p)
			read := make(chan error, 1)
			go func() {
				_, err := p.Stdout().Read(make([]byte, 16))
				read <- err
			}()
			time.Sleep(50 * time.Millisecond)
			end(tc.cause)
			select {
			case err := <-read:
				if !tc.want(err) {
					t.Errorf("read: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the read waited on")
			}
		})
	}
}

type mapped struct{ err error }

func (m *mapped) Error() string { return "mapped: " + m.err.Error() }
func (m *mapped) Unwrap() error { return m.err }

// The SDK's parameters carry wire's shapes as they are: a client that
// speaks the JSON alone is understood.
func TestTheParamsAreTheWiresShapes(t *testing.T) {
	for _, tc := range []struct {
		v    any
		want string
	}{
		{&StartParams{StartArgs: wire.StartArgs{Args: []string{"cat"}, Dir: "d", TimeoutMs: 5, Stderr: true}}, `{"args":["cat"],"dir":"d","timeoutMs":5,"stderr":true}`},
		{&IDParams{ProcessID: wire.ProcessID{ID: "p1"}}, `{"id":"p1"}`},
		{&WriteParams{WriteArgs: wire.WriteArgs{ID: "p1", Data: []byte("hi")}}, `{"id":"p1","data":"aGk="}`},
		{&ReadParams{ReadArgs: wire.ReadArgs{ID: "p1", FD: 2, Max: 9}}, `{"id":"p1","fd":2,"max":9}`},
		{&SignalParams{SignalArgs: wire.SignalArgs{ID: "p1", Signal: "TERM"}}, `{"id":"p1","signal":"TERM"}`},
		{&ExitResult{Exit: wire.Exit{Status: 3, Ended: wire.EndedTimeout}}, `{"status":3,"ended":"timeout"}`},
	} {
		got, err := json.Marshal(tc.v)
		if err != nil || string(got) != tc.want {
			t.Errorf("%T: %s %v, want %s", tc.v, got, err, tc.want)
		}
	}
}
