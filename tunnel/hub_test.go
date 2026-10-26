package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/coder/websocket"
)

// upgrade dials the rig's hub as a workspace end would, with header,
// and returns the connection, nil when refused, and the status.
func (r *rig) upgrade(t *testing.T, header http.Header) (*websocket.Conn, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, r.url, &websocket.DialOptions{HTTPHeader: header})
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	if err != nil {
		return nil, status
	}
	t.Cleanup(func() { c.CloseNow() })
	return c, status
}

func auth(tok string) http.Header { return http.Header{"Authorization": {"Bearer " + tok}} }

// An upgrade is refused before the 101 unless the enrollment names its
// workspace: without an enrollment every one is, a wrong token is, and
// any with an Origin is, a good token or not. Nothing refused is
// connected.
func TestUpgradesAreRefusedBeforeTheyAreAnswered(t *testing.T) {
	var called atomic.Int32
	counting := func(r *http.Request) (string, error) {
		called.Add(1)
		return enroll(tokens)(r)
	}
	none := newRig(t, WithEnrollment(nil))
	enrolled := newRig(t, WithEnrollment(counting))
	invalid := newRig(t, WithEnrollment(func(*http.Request) (string, error) { return "Not A Label", nil }))
	later := newRig(t, WithEnrollment(func(*http.Request) (string, error) {
		return "", fmt.Errorf("the store is starting: %w", ErrTryLater)
	}))
	for name, c := range map[string]struct {
		r      *rig
		header http.Header
		want   int
	}{
		"no enrollment":       {none, auth(tokA), http.StatusUnauthorized},
		"no token":            {enrolled, nil, http.StatusUnauthorized},
		"a wrong token":       {enrolled, auth("not-a-token"), http.StatusUnauthorized},
		"an Origin":           {enrolled, http.Header{"Authorization": {"Bearer " + tokA}, "Origin": {"https://evil.example"}}, http.StatusForbidden},
		"an invalid name":     {invalid, auth(tokA), http.StatusUnauthorized},
		"try later":           {later, auth(tokA), http.StatusServiceUnavailable},
		"a good token":        {enrolled, auth(tokA), http.StatusSwitchingProtocols},
		"the other good one":  {enrolled, auth(tokB), http.StatusSwitchingProtocols},
		"an empty Origin too": {enrolled, http.Header{"Authorization": {"Bearer " + tokA}, "Origin": {""}}, http.StatusForbidden},
	} {
		if _, status := c.r.host(t, c.header); status != c.want {
			t.Errorf("%s: %d, want %d", name, status, c.want)
		}
	}
	if len(none.hub.Connected()) != 0 || len(invalid.hub.Connected()) != 0 || len(later.hub.Connected()) != 0 {
		t.Error("a refused upgrade is connected")
	}
	eventually(t, 10*time.Second, "the good upgrades' connections", func() bool {
		return slices.Equal(enrolled.hub.Connected(), []string{"a", "b"})
	})
	if n := called.Load(); n != 4 {
		// The Origin is refused before the enrollment is asked.
		t.Errorf("the enrollment was asked %d times, want 4", n)
	}
}

// Serve stops at a refusal another dial would not change, and says
// which, with no token in its error.
func TestServeStopsAtARefusal(t *testing.T) {
	l := local(t, t.TempDir(), pathEnv())
	for name, c := range map[string]struct {
		opts []HubOption
		tok  string
		want error
	}{
		"no enrollment": {[]HubOption{WithEnrollment(nil)}, tokA, remote.ErrUnauthenticated},
		"a wrong token": {nil, "tok-wrong-0123456789", remote.ErrUnauthenticated},
	} {
		r := newRig(t, c.opts...)
		var err error
		within(t, 10*time.Second, name, func() {
			err = Serve(context.Background(), r.url, l, WithScopes(remote.ScopeRead), bearer(c.tok), fast)
		})
		if !errors.Is(err, c.want) || strings.Contains(err.Error(), c.tok) {
			t.Errorf("%s: %v, want %v without the token", name, err, c.want)
		}
	}
}

// Serve refuses what it cannot serve before it dials.
func TestServeRefuses(t *testing.T) {
	l := local(t, t.TempDir(), pathEnv())
	ctx := context.Background()
	scopes := WithScopes(remote.ScopeRead)
	for name, c := range map[string]struct {
		url  string
		ws   agentworkspace.Workspace
		opts []Option
	}{
		"no scopes":        {"ws://127.0.0.1:1/", l, nil},
		"empty scopes":     {"ws://127.0.0.1:1/", l, []Option{WithScopes()}},
		"an unknown scope": {"ws://127.0.0.1:1/", l, []Option{WithScopes("admin")}},
		"no workspace":     {"ws://127.0.0.1:1/", nil, []Option{scopes}},
		"ws to a host":     {"ws://example.com/", l, []Option{scopes}},
		"http":             {"http://127.0.0.1:1/", l, []Option{scopes}},
		"userinfo":         {"wss://user:" + tokA + "@example.com/", l, []Option{scopes}},
		"no host":          {"wss:///path", l, []Option{scopes}},
		"a bad redial":     {"ws://127.0.0.1:1/", l, []Option{scopes, WithRedial(time.Second, time.Millisecond)}},
		"no message":       {"ws://127.0.0.1:1/", l, []Option{scopes, WithMaxMessageBytes(0)}},
		"no ping":          {"ws://127.0.0.1:1/", l, []Option{scopes, WithPing(0, time.Second)}},
		"a client timeout": {"ws://127.0.0.1:1/", l, []Option{scopes, WithHTTPClient(&http.Client{Timeout: time.Second})}},
		"insecure handler": {"ws://127.0.0.1:1/", l, []Option{scopes, WithHandlerOptions(remote.WithInsecureNoAuth())}},
	} {
		var err error
		within(t, 5*time.Second, name, func() { err = Serve(ctx, c.url, c.ws, c.opts...) })
		if err == nil || strings.Contains(err.Error(), tokA) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, opts := range map[string][]HubOption{
		"no workspaces": {WithMaxWorkspaces(0)},
		"no message":    {WithMaxMessageBytes(-1)},
		"no ping":       {WithPing(time.Second, 0)},
	} {
		if _, err := NewHub(opts...); err == nil {
			t.Errorf("NewHub with %s: made one", name)
		}
	}
}

// The scopes are the workspace end's: read and write without exec
// reads and writes through the tunnel, its Exec is forbidden, and the
// client is no Starter, whatever the hub's client asks.
func TestTheScopesAreTheWorkspaceEnds(t *testing.T) {
	r := newRig(t)
	dir := t.TempDir()
	r.serve(t, local(t, dir, pathEnv()), WithScopes(remote.ScopeRead, remote.ScopeWrite), bearer(tokA), fast)
	ws := r.workspace(t, "a")
	ctx := context.Background()
	if _, ok := ws.(agentworkspace.Starter); ok {
		t.Error("a workspace served without exec is a Starter")
	}
	if env := ws.Env(); len(env) != 0 {
		t.Errorf("a workspace served without exec told its environment: %q", env)
	}
	if err := ws.WriteFile(ctx, "f.txt", []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	if b, err := fs.ReadFile(ws.FS(), "f.txt"); err != nil || string(b) != "hi" {
		t.Errorf("read back %q, %v", b, err)
	}
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, remote.ErrForbidden) {
		t.Errorf("Exec without exec: %v, want ErrForbidden", err)
	}
	if d := ws.Descriptor(); d.Kind != agentworkspace.KindRemote || d.Ref != "http://a.tunnel.invalid" {
		t.Errorf("Descriptor = %+v", d)
	}
}

// A second connection for a connected name replaces the first: the
// hub's Workspace is the newer host's from then on.
func TestASecondConnectionReplacesTheFirst(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	first, second := local(t, t.TempDir(), pathEnv()), local(t, t.TempDir(), pathEnv())
	first.WriteFile(ctx, "who", []byte("first"), 0o644)
	second.WriteFile(ctx, "who", []byte("second"), 0o644)
	// The first does not dial again while the test runs: it stands for a
	// host that restarted.
	r.serve(t, first, WithScopes(remote.ScopeRead), bearer(tokA), WithRedial(time.Minute, time.Minute))
	eventually(t, 10*time.Second, "the first's connection", func() bool { return r.current("a") != nil })
	old := r.current("a")
	r.serve(t, second, WithScopes(remote.ScopeRead), bearer(tokA), fast)
	r.reconnected(t, "a", old)
	if got := r.hub.Connected(); !slices.Equal(got, []string{"a"}) {
		t.Errorf("Connected = %q", got)
	}
	ws := r.workspace(t, "a")
	if b, err := fs.ReadFile(ws.FS(), "who"); err != nil || string(b) != "second" {
		t.Errorf("read %q, %v; want the second host's", b, err)
	}
}

// Workspace refuses a name that is not one, and waits no longer than
// its context for one that is not connected.
func TestWorkspaceWaitsBoundedByItsContext(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := r.hub.Workspace(ctx, "a"); !errors.Is(err, ErrNotConnected) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Workspace of no connection: %v", err)
	}
	for _, name := range []string{"", "A", "-a", "a-", "a.b", strings.Repeat("a", MaxNameBytes+1)} {
		if _, err := r.hub.Workspace(context.Background(), name); err == nil {
			t.Errorf("Workspace(%q) waited", name)
		}
	}
	if !ValidName(strings.Repeat("a", MaxNameBytes)) || !ValidName("build-1") {
		t.Error("a valid name is refused")
	}
}

// eofConn takes every write and reads end of file at once: a peer that
// hung up right after the 101.
type eofConn struct{ net.Conn }

func (eofConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (eofConn) Write(p []byte) (int, error)      { return len(p), nil }
func (eofConn) Close() error                     { return nil }
func (eofConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (eofConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (eofConn) SetDeadline(time.Time) error      { return nil }
func (eofConn) SetReadDeadline(time.Time) error  { return nil }
func (eofConn) SetWriteDeadline(time.Time) error { return nil }

// A connection that dies as it is taken never stays in the table,
// however its end races the table's other users.
func TestADeadConnectionNeverStaysInTheTable(t *testing.T) {
	h, err := NewHub(WithEnrollment(enroll(tokens)), WithMaxWorkspaces(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var stop atomic.Bool
	go func() { // the table's other users, as wait and Connected are
		for !stop.Load() {
			h.mu.Lock()
			_ = len(h.tunnels)
			h.mu.Unlock()
		}
	}()
	const n = 3000
	taken := 0
	for i := range n {
		if _, err := h.open(fmt.Sprintf("n%d", i), eofConn{}); err == nil {
			taken++
		}
	}
	stop.Store(true)
	time.Sleep(100 * time.Millisecond)
	if got := h.Connected(); taken != 0 || len(got) != 0 {
		t.Errorf("%d of %d dead connections were taken, %d are in the table", taken, n, len(got))
	}
}

// A connection that never speaks HTTP/2 does not replace a working one,
// which keeps serving.
func TestASilentConnectionReplacesNothing(t *testing.T) {
	r := newRig(t, withHandshake(300*time.Millisecond))
	l := local(t, t.TempDir(), pathEnv())
	l.WriteFile(context.Background(), "f", []byte("x"), 0o644)
	r.serve(t, l, WithScopes(remote.ScopeRead), bearer(tokA), fast)
	ws := r.workspace(t, "a")
	old := r.current("a")
	if _, status := r.upgrade(t, auth(tokA)); status != http.StatusSwitchingProtocols {
		t.Fatalf("the silent upgrade: %d", status)
	}
	time.Sleep(600 * time.Millisecond) // past the hub's wait for it
	if r.current("a") != old {
		t.Fatal("a connection that never spoke HTTP/2 replaced a working one")
	}
	if _, err := fs.Stat(ws.FS(), "f"); err != nil {
		t.Errorf("the working connection after: %v", err)
	}
}
