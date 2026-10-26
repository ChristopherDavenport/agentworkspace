package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// A dial takes no session; the first Start opens one, and one
// principal holds at most its bound of them, so it cannot take every
// slot from another.
func TestSessionsAreLazyAndBoundPerPrincipal(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	h, url := serve(t, l, WithAuthenticator(tokens()), WithMaxSessionsPerPrincipal(1))
	ctx := context.Background()
	first := dial(t, url, WithBearerToken(tokAll))
	dial(t, url, WithBearerToken(tokAll))
	h.mu.Lock()
	n := len(h.sessions)
	h.mu.Unlock()
	if n != 0 {
		t.Errorf("%d sessions open after two dials", n)
	}
	p, err := agentworkspace.Start(ctx, first, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	second := dial(t, url, WithBearerToken(tokAll))
	if q, err := agentworkspace.Start(ctx, second, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, ErrLimit) {
		if q != nil {
			q.Close()
		}
		t.Errorf("a second session of one principal: %v", err)
	}
	if out, err := second.Exec(ctx, agentworkspace.Command{Args: []string{"echo", "hi"}}); err != nil || string(out.Stdout) != "hi\n" {
		t.Errorf("Exec, which needs no session: %v, %v", out, err)
	}
	bob := dial(t, url, WithBearerToken(tokBob))
	q, err := agentworkspace.Start(ctx, bob, agentworkspace.Command{Args: []string{"true"}})
	if err != nil {
		t.Errorf("another principal's session: %v", err)
	} else {
		q.Close()
	}
}

// An Exec without a Stream gives back at most the server's output
// bound of each stream; past it the command has run and Exec is
// ErrLimit. With a Stream, any amount arrives, in frames the client's
// bound takes.
func TestExecOutputIsBounded(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth(), WithMaxExecOutputBytes(10))
	ws := dial(t, url)
	ctx := context.Background()
	if out, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"printf", "0123456789"}}); err != nil || string(out.Stdout) != "0123456789" {
		t.Errorf("output at the bound: %v, %v", out, err)
	}
	_, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"sh", "-c", "printf 0123456789AB; exit 3"}})
	if !errors.Is(err, ErrLimit) || !strings.Contains(err.Error(), "status 3") {
		t.Errorf("output over the bound: %v", err)
	}
	var stream bytes.Buffer
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"head", "-c", "200000", "/dev/zero"}, Stream: &stream}); err != nil || stream.Len() != 200000 {
		t.Errorf("a stream of 200000 bytes got %d: %v", stream.Len(), err)
	}
}

// A frame longer than the server's bounds allow is ErrLimit on the
// client, read no further, whatever the server is.
func TestTheClientBoundsAFrame(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == routeInfo {
			writeJSON(w, infoReply{Root: "/w", Scopes: allScopes(), Limits: limits{LeaseMs: 1000, MaxProcesses: 1, MaxWriteBytes: 1, MaxOutputBytes: 10, MaxDirEntries: 1}})
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		huge := bytes.Repeat([]byte("x"), int(frameLimit(10))+1)
		w.Write([]byte(`{"data":"`))
		w.Write(huge)
	}))
	defer srv.Close()
	ws := dial(t, srv.URL)
	if _, err := ws.Exec(context.Background(), agentworkspace.Command{Args: []string{"true"}, Stream: new(bytes.Buffer)}); !errors.Is(err, ErrLimit) {
		t.Errorf("a frame over the bound: %v", err)
	}
}

// WriteFile passes the permission bits alone: no setuid, setgid or
// sticky bit crosses the wire, whatever the client sends.
func TestWriteFileTakesPermissionBitsAlone(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	special := fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky
	if err := ws.WriteFile(context.Background(), "client", []byte("x"), special|0o700); err != nil {
		t.Fatal(err)
	}
	perm := strconv.FormatUint(uint64(special|0o700), 10)
	if status, code := raw(t, "PUT", url+"/v1/fs/file?path=raw&perm="+perm, "", strings.NewReader("x")); status != http.StatusNoContent {
		t.Fatalf("a raw write: %d %q", status, code)
	}
	for _, n := range []string{"client", "raw"} {
		fi, err := os.Stat(filepath.Join(dir, n))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode()&special != 0 || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s was written %v", n, fi.Mode())
		}
	}
}

// A start names its process by the client's key: the same start again
// is the same process, never a second, and once closed, a start by its
// key starts nothing.
func TestAStartsKeyNamesItsProcess(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	h, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	p, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	route := clientOf(ws).sessionRoute() + "/processes"
	key := strings.Repeat("K", 26)
	body := `{"args":["sleep","30"],"key":"` + key + `"}`
	for range 2 {
		if status, code := raw(t, "POST", url+route, "", strings.NewReader(body)); status != http.StatusOK {
			t.Fatalf("a keyed start: %d %q", status, code)
		}
	}
	if n := processes(h); n != 1 {
		t.Errorf("%d processes after a start sent twice", n)
	}
	if status, code := raw(t, "DELETE", url+route+"/"+key, "", nil); status != http.StatusOK {
		t.Errorf("closing by the key: %d %q", status, code)
	}
	if _, code := raw(t, "POST", url+route, "", strings.NewReader(body)); code != codeNoProcess {
		t.Errorf("a start by a closed key: %q", code)
	}
	if n := processes(h); n != 0 {
		t.Errorf("%d processes after the key was closed", n)
	}
	for _, bad := range []string{"short", strings.Repeat("k", 65), "has-a-dash-0123456789"} {
		if _, code := raw(t, "POST", url+route, "", strings.NewReader(`{"args":["true"],"key":"`+bad+`"}`)); code != codeInvalid {
			t.Errorf("a start keyed %q: %q", bad, code)
		}
	}
}

// processes counts the processes open in every session of h.
func processes(h *Handler) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, s := range h.sessions {
		s.mu.Lock()
		n += len(s.procs)
		s.mu.Unlock()
	}
	return n
}

// A start whose reply is lost leaves nothing running: the client closes
// the process by its key.
func TestALostStartReplyLeavesNothingRunning(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	h, err := NewHandler(l, WithInsecureNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/processes") {
			h.ServeHTTP(httptest.NewRecorder(), r) // started, and its reply lost
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()
	ws := dial(t, srv.URL)
	if p, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"sh", "-c", "echo $$ > pgid; exec sleep 30"}}); err == nil {
		p.Close()
		t.Fatal("a start whose reply was lost succeeded")
	}
	if n := processes(h); n != 0 {
		t.Errorf("%d processes run unnamed", n)
	}
	if err := syscall.Kill(-pgidFile(t, filepath.Join(dir, "pgid")), 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("the unnamed process runs on: %v", err)
	}
}

// A Command.Stream whose Write never returns holds up neither Wait nor
// Close: what is left of standard error is dropped after pumpWait.
func TestABlockedStreamHoldsUpNothing(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	stuck := blocked(make(chan struct{}))
	defer close(stuck)
	p, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"sh", "-c", "echo to-stderr >&2"}, Stream: stuck})
	if err != nil {
		t.Fatal(err)
	}
	within(t, pumpWait+5*time.Second, "Wait and Close on a blocked stream", func() {
		if status, err := p.Wait(); status != 0 || err != nil {
			t.Errorf("Wait = %d, %v", status, err)
		}
		p.Close()
	})
}

type blocked chan struct{}

func (b blocked) Write(p []byte) (int, error) {
	<-b
	return len(p), nil
}

// An authenticator that names no principal is refused as one that
// resolved none: the name is what keeps sessions apart.
func TestAPrincipalWithNoNameIsRefused(t *testing.T) {
	l, _ := newLocal(t, nil)
	_, url := serve(t, l, WithAuthenticator(func(*http.Request) (Principal, error) {
		return Principal{Scopes: allScopes()}, nil
	}))
	if status, code := raw(t, "GET", url+"/v1/info", "", nil); status != http.StatusUnauthorized || code != codeUnauthenticated {
		t.Errorf("a nameless principal: %d %q", status, code)
	}
}

// A timeout a time.Duration cannot hold is refused, as a negative one
// is.
func TestATimeoutPastADurationIsRefused(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	p, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	for _, ms := range []string{"9223372036854775807", "9223372036855", "-1"} {
		for _, route := range []string{"/v1/exec", clientOf(ws).sessionRoute() + "/processes"} {
			if _, code := raw(t, "POST", url+route, "", strings.NewReader(`{"args":["true"],"timeoutMs":`+ms+`}`)); code != codeInvalid {
				t.Errorf("%s with a timeout of %s ms: %q", route, ms, code)
			}
		}
	}
}

// An unmapped errno is sent as its words alone, and any other error
// without the served Root or another absolute path's directories.
func TestUnmappedErrorsNameNoServerPath(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	fw := &failingWorkspace{Local: l}
	_, url := serve(t, fw, WithInsecureNoAuth())
	ws := dial(t, url)
	root := l.Root()
	for _, c := range []struct {
		served       error
		want, absent []string
	}{
		{&fs.PathError{Op: "open", Path: root + "/sub/x", Err: syscall.ENOSPC}, []string{"no space left on device"}, []string{root, "sub/x", "open"}},
		{fmt.Errorf("exec: %w", &os.PathError{Op: "fork/exec", Path: "/usr/libexec/tool", Err: syscall.ENOEXEC}), []string{"exec format error"}, []string{"/usr", "tool"}},
		{fmt.Errorf("cannot frob %s/sub/x or \"/etc/secret/y\", in %s", root, root), []string{"cannot frob sub/x", `".../y"`, "in ."}, []string{root, "/etc", "secret"}},
	} {
		fw.err = c.served
		err := ws.Remove(context.Background(), "x")
		if err == nil {
			t.Fatalf("%v: no error", c.served)
		}
		for _, w := range c.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%v: the client got %q, want %q in it", c.served, err, w)
			}
		}
		for _, a := range c.absent {
			if strings.Contains(err.Error(), a) {
				t.Errorf("%v: the client got %q, which holds %q", c.served, err, a)
			}
		}
	}
}

// The read route refuses a FIFO itself, at once, whatever the client
// asked before.
func TestTheReadRouteRefusesAFIFO(t *testing.T) {
	l, dir := newLocal(t, nil)
	if err := syscall.Mkfifo(filepath.Join(dir, "pipe"), 0o644); err != nil {
		t.Skipf("no FIFO here: %v", err)
	}
	_, url := serve(t, l, WithInsecureNoAuth())
	within(t, 5*time.Second, "a read of a FIFO", func() {
		resp, err := http.Get(url + "/v1/fs/file?path=pipe")
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		var r errorReply
		json.NewDecoder(resp.Body).Decode(&r)
		if resp.StatusCode/100 == 2 || !strings.Contains(r.Error.Message, "not a regular file") {
			t.Errorf("a read of a FIFO: %s %+v", resp.Status, r.Error)
		}
	})
}
