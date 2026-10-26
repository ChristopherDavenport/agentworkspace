package tunnel

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
)

// The tests' tokens: each names the workspace it enrolls. The package
// ships no authentication; enroll stands in for the application's.
const (
	tokA = "tok-a-0123456789"
	tokB = "tok-b-0123456789"
)

var tokens = map[string]string{tokA: "a", tokB: "b"}

// enroll is the tests' enrollment: an upgrade's bearer token names its
// workspace.
func enroll(tokens map[string]string) Enrollment {
	return func(r *http.Request) (string, error) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		name, found := tokens[tok]
		if !ok || !found {
			return "", errors.New("no such token")
		}
		return name, nil
	}
}

// bearer sends tok on each upgrade, as an application's credential.
func bearer(tok string) Option {
	return WithHeader(func(context.Context) (http.Header, error) {
		return http.Header{"Authorization": {"Bearer " + tok}}, nil
	})
}

// allScopes is every scope remote checks.
var allScopes = []remote.Scope{remote.ScopeRead, remote.ScopeWrite, remote.ScopeExec}

// fast redials at once, for the tests' drops.
var fast = WithRedial(10*time.Millisecond, 50*time.Millisecond)

// rig is a Hub served by httptest, whose upgrades a gate can refuse
// (503) for a while; it is closed when the test ends.
type rig struct {
	hub  *Hub
	srv  *httptest.Server
	url  string // the ws:// URL the workspace end dials
	shut atomic.Bool
}

func newRig(t *testing.T, opts ...HubOption) *rig {
	t.Helper()
	hub, err := NewHub(append([]HubOption{WithEnrollment(enroll(tokens))}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{hub: hub}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if r.shut.Load() {
			http.Error(w, "shut", http.StatusServiceUnavailable)
			return
		}
		hub.ServeHTTP(w, req)
	}))
	r.url = "ws" + strings.TrimPrefix(r.srv.URL, "http") + "/tunnel"
	t.Cleanup(r.srv.Close)
	t.Cleanup(func() { hub.Close() })
	return r
}

// serve runs Serve over ws until the test ends, and returns what Serve
// returns, once it has.
func (r *rig) serve(t *testing.T, ws agentworkspace.Workspace, opts ...Option) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, r.url, ws, opts...) }()
	var err error
	ended := false
	stop = func() error {
		if ended {
			return err
		}
		cancel()
		select {
		case err = <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("Serve did not return within 15s of its context's end")
		}
		ended = true
		return err
	}
	t.Cleanup(func() { stop() })
	return stop
}

// workspace is name's workspace from the hub, waited for at most ten
// seconds, and closed when the test ends.
func (r *rig) workspace(t *testing.T, name string, opts ...remote.DialOption) remote.Workspace {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ws, err := r.hub.Workspace(ctx, name, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	return ws
}

// current is name's tunnel now, nil if none.
func (r *rig) current(name string) *tunnel {
	r.hub.mu.Lock()
	defer r.hub.mu.Unlock()
	return r.hub.tunnels[name]
}

// drop cuts name's connection at the hub, and returns it.
func (r *rig) drop(t *testing.T, name string) *tunnel {
	t.Helper()
	old := r.current(name)
	if old == nil {
		t.Fatalf("%s is not connected", name)
	}
	old.close()
	return old
}

// eventually fails the test if cond does not hold within d.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// reconnected waits for name to be connected again, on another
// connection than old.
func (r *rig) reconnected(t *testing.T, name string, old *tunnel) {
	t.Helper()
	eventually(t, 10*time.Second, name+"'s reconnect", func() bool {
		c := r.current(name)
		return c != nil && c != old
	})
}

func local(t *testing.T, dir string, env []string) *agentworkspace.Local {
	t.Helper()
	l, err := agentworkspace.NewLocal(dir, env)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func pathEnv() []string { return []string{"PATH=" + os.Getenv("PATH")} }

// within fails the test if f takes longer than d.
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

// alive reports whether process pid is running.
func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }
