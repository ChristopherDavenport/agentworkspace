package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
)

// A redirect of the upgrade is never followed, whatever the
// application's client allows: a wss:// hub that answers 307 to a plain
// http:// one does not take the tunnel, or its credential, there.
func TestARedirectIsNeverFollowed(t *testing.T) {
	var asked atomic.Int32
	hub, err := NewHub(WithEnrollment(func(r *http.Request) (string, error) {
		asked.Add(1)
		return "a", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	plain := httptest.NewServer(hub)
	t.Cleanup(plain.Close)
	var redirected atomic.Int32
	front := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		http.Redirect(w, r, plain.URL+"/tunnel", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(front.Close)
	// The application's client trusts the front's certificate and, as
	// the zero value does, would follow any redirect.
	roots := front.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, "wss"+strings.TrimPrefix(front.URL, "https")+"/tunnel", local(t, t.TempDir(), pathEnv()),
			WithScopes(remote.ScopeRead), WithHTTPClient(hc), fast,
			WithHeader(func(context.Context) (http.Header, error) { return http.Header{"X-Token": {"secret"}}, nil }))
	}()
	eventually(t, 10*time.Second, "a few redialled upgrades", func() bool { return redirected.Load() >= 3 })
	cancel()
	within(t, 10*time.Second, "Serve's end", func() { <-done })
	if n := asked.Load(); n != 0 || len(hub.Connected()) != 0 {
		t.Errorf("the plain hub was asked %d times and has %q connected", n, hub.Connected())
	}
}

// Once Serve has connected, a refusal on a redial is tried again for as
// long as the lease, so a process outlives an enrollment that refused
// for a moment; a refusal that lasts past the lease since the drop ends
// Serve, with the refusal.
func TestARefusalAfterAConnectionIsTriedAgainWithinTheLease(t *testing.T) {
	const lease = 2 * time.Second
	var refuse atomic.Bool
	r := newRig(t, WithEnrollment(func(req *http.Request) (string, error) {
		if refuse.Load() {
			return "", errors.New("the enrollment's store is down")
		}
		return enroll(tokens)(req)
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, r.url, local(t, t.TempDir(), pathEnv()), WithScopes(allScopes...), bearer(tokA), fast,
			WithHandlerOptions(remote.WithLease(lease)))
	}()
	s := r.workspace(t, "a").(agentworkspace.Starter)
	_, _, pid := pidOf(t, s, "echo $$; exec sleep 60")

	refuse.Store(true)
	old := r.drop(t, "a")
	time.Sleep(lease / 2)
	if !alive(pid) {
		t.Fatal("a refused redial within the lease ended the process")
	}
	refuse.Store(false)
	r.reconnected(t, "a", old)
	if !alive(pid) {
		t.Fatal("the process did not outlive the refusal")
	}

	// Past the lease, the refusal stands.
	refuse.Store(true)
	r.drop(t, "a")
	select {
	case err := <-done:
		if !errors.Is(err, remote.ErrUnauthenticated) {
			t.Errorf("Serve = %v, want ErrUnauthenticated", err)
		}
	case <-time.After(4 * lease):
		t.Fatal("Serve went on past the lease at a lasting refusal")
	}
	if alive(pid) {
		t.Error("a process outlived Serve")
	}
	// The process's Close at the hub would wait its timeout for the
	// workspace to come back; a closed hub answers at once.
	r.hub.Close()
}

// An Enrollment's ErrTryLater is a 503, which the host dials again on,
// even before it has ever connected.
func TestTryLaterIsDialledAgain(t *testing.T) {
	var later atomic.Int32
	later.Store(3)
	r := newRig(t, WithEnrollment(func(req *http.Request) (string, error) {
		if later.Add(-1) >= 0 {
			return "", fmt.Errorf("not yet: %w", ErrTryLater)
		}
		return enroll(tokens)(req)
	}))
	r.serve(t, local(t, t.TempDir(), pathEnv()), WithScopes(remote.ScopeRead), bearer(tokA), fast)
	eventually(t, 10*time.Second, "the connection after three answers of try later", func() bool { return r.current("a") != nil })
}
