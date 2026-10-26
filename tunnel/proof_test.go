package tunnel

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/coder/websocket"
)

// The tests' proof, as the package doc has it: an HMAC over a nonce the
// workspace end sends, keyed by a secret both ends hold and never sent.
// The package ships no proof; these stand in for the application's.

// mac is the hex HMAC-SHA256, under key, of label and nonce.
func mac(key []byte, label, nonce string) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(label + "\x00" + nonce))
	return hex.EncodeToString(m.Sum(nil))
}

// nonced sends tok and a fresh nonce on each upgrade.
func nonced(tok string) Option {
	return WithHeader(func(context.Context) (http.Header, error) {
		return http.Header{"Authorization": {"Bearer " + tok}, "X-Nonce": {rand.Text()}}, nil
	})
}

// checks is the workspace end's half: the hub's MAC of the nonce sent.
func checks(key []byte) Option {
	return WithHubProof(func(req *http.Request, resp *http.Response) error {
		nonce := req.Header.Get("X-Nonce")
		if nonce == "" || !hmac.Equal([]byte(resp.Header.Get("X-Hub-Proof")), []byte(mac(key, "hub", nonce))) {
			return errors.New("the hub's proof does not match")
		}
		return nil
	})
}

// proves is the hub's half: its MAC of the upgrade's nonce.
func proves(key []byte) HubOption {
	return WithProofHeader(func(r *http.Request, _ string) (http.Header, error) {
		return http.Header{"X-Hub-Proof": {mac(key, "hub", r.Header.Get("X-Nonce"))}}, nil
	})
}

var (
	theKey   = []byte("the shared secret, never sent 0123")
	otherKey = []byte("another secret, never sent 456789")
)

// A hub that proves itself with the workspace end's key is served; one
// without the proof, or with another key's, is refused before the
// connection serves anything, and Serve says so.
func TestTheHubIsProvedBeforeItIsServed(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "f.txt"), []byte("hi"), 0o644)
	l := local(t, dir, pathEnv())

	good := newRig(t, proves(theKey))
	good.serve(t, l, WithScopes(remote.ScopeRead), nonced(tokA), checks(theKey), fast)
	if b, err := fs.ReadFile(good.workspace(t, "a").FS(), "f.txt"); err != nil || string(b) != "hi" {
		t.Errorf("through a hub with the proof: %q, %v", b, err)
	}

	for name, opts := range map[string][]HubOption{
		"no proof":    nil,
		"another key": {proves(otherKey)},
	} {
		r := newRig(t, opts...)
		var err error
		within(t, 10*time.Second, name, func() {
			err = Serve(context.Background(), r.url, l, WithScopes(remote.ScopeRead), nonced(tokA), checks(theKey), fast)
		})
		if !errors.Is(err, ErrHubProof) || strings.Contains(err.Error(), tokA) {
			t.Errorf("%s: %v, want ErrHubProof without the token", name, err)
		}
		if c := r.hub.Connected(); len(c) != 0 {
			t.Errorf("%s: connected %q", name, c)
		}
	}
}

// An impostor that takes the upgrade and asks for the workspace's info
// over HTTP/2 at once is answered nothing when the workspace end checks
// a proof it does not give, and answered when the end checks none: the
// proof is checked before the first request is served.
func TestAnImpostorIsServedNothing(t *testing.T) {
	l := local(t, t.TempDir(), pathEnv())
	for _, c := range []struct {
		name   string
		proof  Option
		served bool
	}{
		{"no check", nil, true},
		{"a check", checks(theKey), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			served := make(chan error, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				served <- impostor(w, r)
			}))
			defer srv.Close()
			url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/tunnel"
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, url, l, WithScopes(remote.ScopeRead), nonced(tokA), c.proof, fast) }()
			var err error
			select {
			case err = <-served:
			case <-time.After(10 * time.Second):
				t.Fatal("the impostor was not dialled within 10s")
			}
			if c.served && err != nil {
				t.Errorf("the impostor was not served without a check: %v", err)
			}
			if !c.served && err == nil {
				t.Error("the impostor was served though the end checks a proof")
			}
			cancel()
			select {
			case err := <-done:
				if !c.served && !errors.Is(err, ErrHubProof) {
					t.Errorf("Serve: %v, want ErrHubProof", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("Serve did not return within 15s of its context's end")
			}
		})
	}
}

// impostor takes an upgrade, as anything bound to the hub's port can,
// and asks for the workspace's info over HTTP/2: nil when it was
// answered.
func impostor(w http.ResponseWriter, r *http.Request) error {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return err
	}
	nc := conn(context.Background(), c, DefaultMaxMessageBytes)
	defer nc.Close()
	tr := &http.Transport{
		Protocols:   h2c(),
		HTTP2:       defaultLink().http2(),
		DialContext: func(context.Context, string, string) (net.Conn, error) { return nc, nil },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc, err := tr.NewClientConn(ctx, "http", "a:80")
	if err != nil {
		return err
	}
	defer cc.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://a/v1/info", nil)
	if err != nil {
		return err
	}
	resp, err := cc.RoundTrip(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("answered %s", resp.Status)
	}
	return nil
}

// A hub whose proof cannot be made, or names a header of the
// handshake's own, refuses the upgrade (500) and connects nothing.
func TestAProofHeaderTheHubCannotSendRefuses(t *testing.T) {
	for name, f := range map[string]func(*http.Request, string) (http.Header, error){
		"an error": func(*http.Request, string) (http.Header, error) {
			return nil, errors.New("no key for the workspace")
		},
		"the accept": func(*http.Request, string) (http.Header, error) {
			return http.Header{"Sec-WebSocket-Accept": {"x"}}, nil
		},
		"a subprotocol": func(*http.Request, string) (http.Header, error) {
			return http.Header{"sec-websocket-protocol": {"x"}}, nil
		},
		"the upgrade":    func(*http.Request, string) (http.Header, error) { return http.Header{"Upgrade": {"h2c"}}, nil },
		"the connection": func(*http.Request, string) (http.Header, error) { return http.Header{"Connection": {"close"}}, nil },
		"its own header": func(*http.Request, string) (http.Header, error) { return http.Header{"X-Hub-Proof": {"x"}}, nil },
	} {
		r := newRig(t, WithProofHeader(f))
		want := http.StatusInternalServerError
		if name == "its own header" {
			want = http.StatusSwitchingProtocols
		}
		if _, status := r.upgrade(t, auth(tokA)); status != want {
			t.Errorf("%s: %d, want %d", name, status, want)
		}
	}
}
