package tunnel

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/coder/websocket"
)

// A hub holds at most its bound of workspaces: one more is refused
// before the 101, and one that replaces a connected name is not.
func TestTheHubBoundsItsWorkspaces(t *testing.T) {
	r := newRig(t, WithMaxWorkspaces(1))
	if _, status := r.upgrade(t, auth(tokA)); status != http.StatusSwitchingProtocols {
		t.Fatalf("the first: %d", status)
	}
	eventually(t, 10*time.Second, "the first's connection", func() bool { return r.current("a") != nil })
	if _, status := r.upgrade(t, auth(tokB)); status != http.StatusServiceUnavailable {
		t.Errorf("one past the bound: %d, want 503", status)
	}
	if _, status := r.upgrade(t, auth(tokA)); status != http.StatusSwitchingProtocols {
		t.Errorf("a replacement at the bound: %d, want 101", status)
	}
}

// A message over the bound ends the tunnel; one end's writes are sent
// under it, so a file far larger than a message crosses whole.
func TestTheMessageBound(t *testing.T) {
	const bound = 4 << 10
	t.Run("over it", func(t *testing.T) {
		r := newRig(t, WithMaxMessageBytes(bound))
		c, status := r.upgrade(t, auth(tokA))
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("upgrade: %d", status)
		}
		eventually(t, 10*time.Second, "the connection", func() bool { return r.current("a") != nil })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c.CloseRead(ctx) // answers the hub's close, which then ends at once
		c.Write(ctx, websocket.MessageBinary, make([]byte, bound+1))
		eventually(t, 10*time.Second, "the tunnel's end", func() bool { return r.current("a") == nil })
	})
	t.Run("written under it", func(t *testing.T) {
		r := newRig(t, WithMaxMessageBytes(bound))
		r.serve(t, local(t, t.TempDir(), pathEnv()), WithScopes(remote.ScopeRead, remote.ScopeWrite), bearer(tokA), fast, WithMaxMessageBytes(bound))
		ws := r.workspace(t, "a")
		data := bytes.Repeat([]byte("0123456789abcdef"), 64<<10) // 1 MiB
		if err := ws.WriteFile(context.Background(), "big", data, 0o644); err != nil {
			t.Fatal(err)
		}
		if got, err := fs.ReadFile(ws.FS(), "big"); err != nil || !bytes.Equal(got, data) {
			t.Errorf("read back %d bytes, %v", len(got), err)
		}
	})
}

// A dead tunnel is noticed at each end within the ping's interval and
// timeout: the hub drops one whose other end answers nothing, and the
// workspace end dials again from a hub that answers nothing.
func TestADeadTunnelIsNoticed(t *testing.T) {
	ping := WithPing(100*time.Millisecond, 200*time.Millisecond)
	t.Run("at the hub", func(t *testing.T) {
		r := newRig(t, ping)
		if _, status := r.upgrade(t, auth(tokA)); status != http.StatusSwitchingProtocols {
			t.Fatalf("upgrade: %d", status)
		}
		eventually(t, 10*time.Second, "the connection", func() bool { return r.current("a") != nil })
		// The WebSocket is open and never read: no ping is answered.
		eventually(t, 5*time.Second, "the silent tunnel's end", func() bool { return r.current("a") == nil })
	})
	// A hub that takes the upgrade and then says nothing, or says
	// HTTP/2's preface and settings and then nothing.
	for name, greet := range map[string][]byte{
		"at the workspace end, before HTTP/2": nil,
		"at the workspace end": append([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"),
			0, 0, 0, 4, 0, 0, 0, 0, 0), // an empty SETTINGS frame
	} {
		t.Run(name, func(t *testing.T) {
			var accepted atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer c.CloseNow()
				accepted.Add(1)
				if greet != nil {
					c.Write(r.Context(), websocket.MessageBinary, greet)
				}
				// Held open, and never read: no ping is answered.
				<-r.Context().Done()
			}))
			t.Cleanup(srv.Close)
			opts := []Option{WithScopes(remote.ScopeRead), ping, fast}
			if greet == nil {
				// Before HTTP/2 there are no pings: the handshake's bound
				// keeps the watch.
				opts = append(opts, withHandshake(300*time.Millisecond))
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- Serve(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), local(t, t.TempDir(), pathEnv()), opts...)
			}()
			eventually(t, 10*time.Second, "a redial from a silent hub", func() bool { return accepted.Load() >= 2 })
			cancel()
			within(t, 10*time.Second, "Serve's end", func() { <-done })
		})
	}
}

// withHandshake is HandshakeTimeout, shortened.
func withHandshake(d time.Duration) LinkOption { return func(c *linkConfig) { c.handshake = d } }

// Serve's context ending ends every process the server started, and
// Serve returns the context's error once they have ended.
func TestServesEndEndsItsProcesses(t *testing.T) {
	r := newRig(t)
	stop := r.serve(t, local(t, t.TempDir(), pathEnv()), WithScopes(allScopes...), bearer(tokA), fast)
	s := r.workspace(t, "a").(agentworkspace.Starter)
	_, _, pid := pidOf(t, s, "echo $$; exec sleep 30")
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Serve = %v, want context.Canceled", err)
	}
	if alive(pid) {
		t.Error("a process outlived Serve")
	}
	eventually(t, 10*time.Second, "the hub's notice", func() bool { return r.current("a") == nil })
	// The process's Close at the hub would wait its timeout for the
	// workspace to come back; a closed hub answers at once.
	r.hub.Close()
}

// Hub.Close ends every connection, and a Workspace from it fails from
// then on; the host is refused until it stops.
func TestHubCloseEndsTheConnections(t *testing.T) {
	r := newRig(t)
	l := local(t, t.TempDir(), pathEnv())
	l.WriteFile(context.Background(), "f", []byte("x"), 0o644)
	r.serve(t, l, WithScopes(remote.ScopeRead), bearer(tokA), fast)
	ws := r.workspace(t, "a")
	r.hub.Close()
	if got := r.hub.Connected(); len(got) != 0 {
		t.Errorf("Connected after Close = %q", got)
	}
	if _, err := fs.Stat(ws.FS(), "f"); !errors.Is(err, ErrClosed) {
		t.Errorf("Stat after Close: %v, want ErrClosed", err)
	}
	if _, status := r.upgrade(t, auth(tokA)); status != http.StatusServiceUnavailable {
		t.Errorf("an upgrade after Close: %d, want 503", status)
	}
}
