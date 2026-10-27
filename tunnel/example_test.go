package tunnel_test

// This file is the package doc's "Proving the hub" in code: an HMAC,
// keyed by a secret both ends hold and never sent, over a nonce the hub
// issues. The package ships no proof; this one is the application's,
// and a sketch of it (no key management, one workspace).

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/ChristopherDavenport/agentworkspace/tunnel"
)

const (
	nonceLifetime = 30 * time.Second // by the hub's own clock
	maxTaken      = 1024             // nonces taken and still live
	maxUnanswered = 3                // upgrades in a row a hub would have answered

	workspaceLabel = "example tunnel workspace"
	hubLabel       = "example tunnel hub"
	nonceLabel     = "example tunnel nonce"
)

// mac is the hex HMAC-SHA256, under key, of label and parts, each ended
// by a zero byte, which none of them holds (plain).
func mac(key []byte, label string, parts ...string) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(label + "\x00"))
	for _, p := range parts {
		m.Write([]byte(p + "\x00"))
	}
	return hex.EncodeToString(m.Sum(nil))
}

// plain reports whether s is short and of letters, digits and dots
// alone, as a nonce and an instance id are.
func plain(s string) bool {
	return s != "" && len(s) <= 128 && strings.Trim(s, ".0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") == ""
}

// hubProof is the hub's half, for the one workspace name it enrolls.
type hubProof struct {
	key, secret []byte // the shared key; the hub's own, for its nonces
	name        string
	age         func() time.Duration // monotonic, from a random offset

	mu    sync.Mutex
	taken map[string]time.Duration // a nonce taken, and the age it expires at
	order []string                 // taken, oldest first
}

func newHubProof(key []byte, name string) *hubProof {
	secret := make([]byte, 32)
	rand.Read(secret)
	var b [8]byte
	rand.Read(b[:])
	// A nonce's age starts at a random offset, up to 2^60 ns, so it
	// does not say when the hub started.
	offset := time.Duration(binary.BigEndian.Uint64(b[:]) >> 4)
	start := time.Now()
	return &hubProof{
		key: key, secret: secret, name: name,
		age:   func() time.Duration { return offset + time.Since(start) },
		taken: map[string]time.Duration{},
	}
}

// nonce issues one: its age, random text, and the hub's MAC of both
// under its secret. It keeps nothing.
func (h *hubProof) nonce() string {
	body := strconv.FormatInt(int64(h.age()), 10) + "." + rand.Text()
	return body + "." + mac(h.secret, nonceLabel, body)[:32]
}

// enroll is the Enrollment: the upgrade's MAC of the name, a nonce this
// hub issued within its lifetime and the instance id, under the key,
// and the nonce taken, once.
func (h *hubProof) enroll(r *http.Request) (string, error) {
	nonce, instance := r.Header.Get("X-Nonce"), r.Header.Get("X-Instance")
	if !plain(nonce) || !plain(instance) ||
		!hmac.Equal([]byte(r.Header.Get("X-Proof")), []byte(mac(h.key, workspaceLabel, h.name, nonce, instance))) {
		return "", errors.New("not proved with the key")
	}
	i := strings.LastIndexByte(nonce, '.')
	if i < 0 || !hmac.Equal([]byte(nonce[i+1:]), []byte(mac(h.secret, nonceLabel, nonce[:i])[:32])) {
		return "", errors.New("over a nonce this hub did not issue")
	}
	ns, _, _ := strings.Cut(nonce, ".")
	at, _ := strconv.ParseInt(ns, 10, 64)
	now := h.age()
	if d := now - time.Duration(at); d < 0 || d > nonceLifetime {
		return "", errors.New("over a nonce past its lifetime")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for len(h.order) > 0 && h.taken[h.order[0]] < now {
		delete(h.taken, h.order[0])
		h.order = h.order[1:]
	}
	switch _, used := h.taken[nonce]; {
	case used:
		return "", errors.New("over a nonce taken before")
	case len(h.taken) >= maxTaken:
		return "", fmt.Errorf("%d nonces taken and live: %w", maxTaken, tunnel.ErrTryLater)
	}
	h.taken[nonce] = time.Duration(at) + nonceLifetime
	h.order = append(h.order, nonce)
	return h.name, nil
}

// answer is the hub's 101 (WithProofHeader): its MAC, under the other
// label, of what the enrollment took.
func (h *hubProof) answer(r *http.Request, name string) (http.Header, error) {
	return http.Header{"X-Hub-Proof": {mac(h.key, hubLabel, name, r.Header.Get("X-Nonce"), r.Header.Get("X-Instance"))}}, nil
}

// watch passes the workspace end's requests on, and stops it at the
// maxUnanswered-th upgrade in a row (each sent over a nonce) that got
// no answer a hub gives: a hub does not take an upgrade and drop it.
type watch struct {
	base       http.RoundTripper
	stop       context.CancelCauseFunc
	mu         sync.Mutex
	unanswered int
}

func (w *watch) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := w.base.RoundTrip(r)
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return resp, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err == nil && slices.Contains([]int{http.StatusSwitchingProtocols, http.StatusUnauthorized, http.StatusForbidden, http.StatusServiceUnavailable}, resp.StatusCode) {
		w.unanswered = 0
	} else if w.unanswered++; w.unanswered >= maxUnanswered {
		w.stop(fmt.Errorf("%d upgrades in a row went unanswered: %w", w.unanswered, tunnel.ErrHubProof))
	}
	return resp, err
}

// fetchNonce asks the hub for a nonce: a plain GET of its URL, through
// hc, which follows no redirect. No answer, or one a hub gives for a
// while (5xx, 429, 408), is an error to dial again on; any other answer
// without a nonce rules a hub out, and stops the workspace end.
func fetchNonce(ctx context.Context, hc *http.Client, hubURL string, stop context.CancelCauseFunc) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http"+strings.TrimPrefix(hubURL, "ws"), nil)
	if err != nil {
		return "", err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	nonce := resp.Header.Get("X-Nonce")
	switch c := resp.StatusCode; {
	case c == http.StatusOK && plain(nonce):
		return nonce, nil
	case c >= 500, c == http.StatusTooManyRequests, c == http.StatusRequestTimeout:
		return "", fmt.Errorf("no nonce yet: %s", resp.Status)
	}
	err = fmt.Errorf("no hub: its URL answered %s without a nonce", resp.Status)
	stop(err)
	return "", err
}

// The hub issues a nonce before each upgrade; the upgrade carries the
// workspace end's MAC over it, which the hub takes once, and the 101 the
// hub's, which the workspace end checks before it serves anything. A
// proof that fails, at any dial, ends Serve through its context.
func Example_hubProof() {
	key := []byte("a secret both ends hold, never sent")

	// The hub: the program's handler beside the Hub answers a plain GET
	// with a nonce, and hands the Hub the upgrades.
	hp := newHubProof(key, "build-1")
	hub, err := tunnel.NewHub(tunnel.WithEnrollment(hp.enroll), tunnel.WithProofHeader(hp.answer))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer hub.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.Header.Get("Upgrade") == "" {
			w.Header().Set("X-Nonce", hp.nonce())
			return
		}
		hub.ServeHTTP(w, r)
	}))
	defer srv.Close()
	hubURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/tunnel"

	// The workspace end: an instance id for its run, a nonce from the
	// hub and a MAC for each upgrade, and the hub's answer checked.
	dir, _ := os.MkdirTemp("", "hubproof")
	defer os.RemoveAll(dir)
	os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("served"), 0o644)
	local, _ := agentworkspace.NewLocal(dir, nil)
	defer local.Close()

	name, instance := "build-1", rand.Text()
	ctx, stop := context.WithCancelCause(context.Background())
	defer stop(nil)
	hc := &http.Client{
		Transport:     &watch{base: http.DefaultTransport, stop: stop},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	upgrades := make(chan *http.Request, 1)
	served := make(chan error, 1)
	go func() {
		served <- tunnel.Serve(ctx, hubURL, local,
			tunnel.WithScopes(remote.ScopeRead),
			tunnel.WithHTTPClient(hc),
			tunnel.WithHeader(func(ctx context.Context) (http.Header, error) {
				nonce, err := fetchNonce(ctx, hc, hubURL, stop)
				if err != nil {
					return nil, err
				}
				return http.Header{
					"X-Nonce":    {nonce},
					"X-Instance": {instance},
					"X-Proof":    {mac(key, workspaceLabel, name, nonce, instance)},
				}, nil
			}),
			tunnel.WithHubProof(func(req *http.Request, resp *http.Response) error {
				want := mac(key, hubLabel, name, req.Header.Get("X-Nonce"), instance)
				if !hmac.Equal([]byte(resp.Header.Get("X-Hub-Proof")), []byte(want)) {
					err := errors.New("the hub's proof does not match")
					// Serve would dial again within the lease after a
					// connection that served; stop at once instead.
					stop(fmt.Errorf("%w: %w", tunnel.ErrHubProof, err))
					return err
				}
				select {
				case upgrades <- req:
				default:
				}
				return nil
			}))
	}()

	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ws, err := hub.Workspace(wctx, name)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ws.Close()
	b, err := fs.ReadFile(ws.FS(), "hello.txt")
	fmt.Println(string(b), err)

	// The upgrade, sent again, is refused: its nonce was taken.
	_, err = hp.enroll(<-upgrades)
	fmt.Println("replayed:", err)

	// So is one over a nonce an impostor holding the port handed out.
	made := "1." + rand.Text() + ".0123456789abcdef0123456789abcdef"
	r := httptest.NewRequest(http.MethodGet, "/tunnel", nil)
	r.Header = http.Header{"X-Nonce": {made}, "X-Instance": {instance}, "X-Proof": {mac(key, workspaceLabel, name, made, instance)}}
	_, err = hp.enroll(r)
	fmt.Println("captured by an impostor:", err)

	stop(nil)
	<-served
	// Output:
	// served <nil>
	// replayed: over a nonce taken before
	// captured by an impostor: over a nonce this hub did not issue
}
