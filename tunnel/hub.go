package tunnel

// The server end: the hub workspaces dial, and a remote client over
// each.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"slices"
	"sync"

	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/coder/websocket"
)

// DefaultMaxWorkspaces bounds the workspaces connected to one Hub at
// once (WithMaxWorkspaces).
const DefaultMaxWorkspaces = 256

// MaxNameBytes bounds a workspace's name.
const MaxNameBytes = 63

// ErrNotConnected is a workspace that was not connected before the
// context waiting for it ended.
var ErrNotConnected = errors.New("tunnel: the workspace is not connected")

// ErrClosed is a Hub that was closed.
var ErrClosed = errors.New("tunnel: the hub is closed")

// ErrTryLater is an Enrollment's answer for an upgrade it cannot judge
// now (its store is not up yet, say): the Hub answers 503, and the
// workspace end dials again after its wait, as for a hub that is down.
// An Enrollment returns it, or an error that wraps it.
var ErrTryLater = errors.New("tunnel: the enrollment cannot answer now; try later")

// ValidName reports whether name may name a workspace: a DNS label, 1
// to MaxNameBytes lower-case letters, digits and hyphens, neither
// first nor last a hyphen. It is the host of the remote's base URL.
func ValidName(name string) bool {
	if len(name) == 0 || len(name) > MaxNameBytes || name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	for _, r := range []byte(name) {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}

// Enrollment says which workspace an upgrade is from: the
// application's authentication of the host that dialled, which this
// package does not implement. An error refuses the upgrade (401); its
// text is not sent. ErrTryLater asks the host to dial again later
// (503).
type Enrollment func(r *http.Request) (name string, err error)

type hubConfig struct {
	link          linkConfig
	enroll        Enrollment
	challenge     string // the WWW-Authenticate of a 401, "" for none
	maxWorkspaces int
}

type hubOption func(*hubConfig)

func (o hubOption) applyHub(c *hubConfig) { o(c) }

// WithEnrollment names the workspace each upgrade is from, or refuses
// it, before the upgrade is answered. A Hub without it refuses every
// upgrade.
func WithEnrollment(f Enrollment) HubOption { return hubOption(func(c *hubConfig) { c.enroll = f }) }

// WithChallenge sends value as the WWW-Authenticate header of every 401
// the Hub writes: an upgrade the enrollment refused or named no valid
// workspace for, and every one without an enrollment. value is the
// application's challenge, its scheme and any parameters, sent as
// given. Without it, or with "", a 401 carries no challenge: the
// authentication is the application's, and so is naming its scheme.
// It is remote.WithChallenge's, for the hub's own refusals.
func WithChallenge(value string) HubOption {
	return hubOption(func(c *hubConfig) { c.challenge = value })
}

// WithMaxWorkspaces bounds the workspaces connected at once; an upgrade
// past it is refused (503) unless it replaces a connected one. The
// default is DefaultMaxWorkspaces.
func WithMaxWorkspaces(n int) HubOption {
	return hubOption(func(c *hubConfig) { c.maxWorkspaces = n })
}

// Hub is the server end: an http.Handler the program mounts at the
// path its workspaces dial, which takes their WebSocket upgrades, and
// gives a remote.Workspace over each connected one (Workspace). It is
// the HTTP client of each tunnel; the workspace end serves.
//
// An upgrade is answered only after the enrollment has named its
// workspace: one carrying an Origin is refused (403) first, as no
// browser is a client, then one the enrollment refuses, or every one
// without an enrollment (401, with the application's challenge,
// WithChallenge), then one past the bound of workspaces (503). A second connection for a name that is connected replaces the
// first, which is closed: a host that restarted dials again before the
// hub has noticed that its old connection is dead, and the newest is
// the one that is alive. Requests in flight on the replaced connection
// fail. A new connection is taken only once it has answered a request
// over HTTP/2, within HandshakeTimeout, so a host that upgrades and
// never speaks HTTP/2 does not take the name from a working one.
type Hub struct {
	cfg  hubConfig
	ctx  context.Context // ends with Close, and every tunnel with it
	stop context.CancelFunc

	mu      sync.Mutex
	closed  bool
	tunnels map[string]*tunnel
	changed chan struct{} // closed and replaced at every change to tunnels
}

var _ http.Handler = (*Hub)(nil)

// NewHub makes a Hub. It refuses a bound that is not positive. One
// made without WithEnrollment refuses every upgrade.
func NewHub(opts ...HubOption) (*Hub, error) {
	cfg := hubConfig{link: defaultLink(), maxWorkspaces: DefaultMaxWorkspaces}
	for _, o := range opts {
		if o != nil {
			o.applyHub(&cfg)
		}
	}
	if err := cfg.link.valid(); err != nil {
		return nil, err
	}
	if cfg.maxWorkspaces <= 0 {
		return nil, errors.New("tunnel: a bound of workspaces that is not positive")
	}
	h := &Hub{cfg: cfg, tunnels: map[string]*tunnel{}, changed: make(chan struct{})}
	h.ctx, h.stop = context.WithCancel(context.Background())
	return h, nil
}

// tunnel is one workspace's connection: the HTTP/2 client over it.
type tunnel struct {
	h    *Hub
	name string
	nc   net.Conn
	cc   *http.ClientConn
	dead bool // under h.mu: closing or closed, and never put in the table again
	once sync.Once
}

// close takes the tunnel out of the table, if it is there, marks it dead
// so that open never puts it there, and then ends the connection. It
// may be called more than once, and from anywhere.
func (t *tunnel) close() {
	t.h.mu.Lock()
	t.dead = true
	if t.h.tunnels[t.name] == t {
		delete(t.h.tunnels, t.name)
		t.h.notify()
	}
	t.h.mu.Unlock()
	t.once.Do(func() {
		t.cc.Close()
		t.nc.Close()
		t.h.cfg.link.log.Info("tunnel: disconnected", "workspace", t.name)
	})
}

// usable reports whether a request can still be sent on the tunnel: it
// is not closed, and if it has no stream free it is because every one
// is in use, not because the connection was told to go away or has
// used up its stream ids. It relies on the workspace end allowing
// MaxStreams streams, as Serve does.
func (t *tunnel) usable() bool {
	return t.cc.Err() == nil && (t.cc.Available() > 0 || t.cc.InFlight() >= MaxStreams)
}

// notify wakes whoever waits on a change; h.mu is held.
func (h *Hub) notify() {
	close(h.changed)
	h.changed = make(chan struct{})
}

// ServeHTTP takes one workspace's upgrade, refused before it is
// answered unless the enrollment names the workspace, and holds the
// connection until it ends, it is replaced, or the Hub closes.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	refuse := func(status int, msg string) {
		h.cfg.link.log.Info("tunnel: refused an upgrade", "from", r.RemoteAddr, "status", status, "why", msg)
		if status == http.StatusUnauthorized && h.cfg.challenge != "" {
			w.Header().Set("WWW-Authenticate", h.cfg.challenge)
		}
		http.Error(w, "tunnel: "+msg, status)
	}
	if len(r.Header.Values("Origin")) > 0 {
		refuse(http.StatusForbidden, "an upgrade with an Origin is refused: no browser is a client")
		return
	}
	if h.cfg.enroll == nil {
		refuse(http.StatusUnauthorized, "no enrollment is configured")
		return
	}
	name, err := h.cfg.enroll(r)
	switch {
	case errors.Is(err, ErrTryLater):
		refuse(http.StatusServiceUnavailable, "the enrollment asks to try later")
		return
	case err != nil:
		refuse(http.StatusUnauthorized, "unauthenticated")
		return
	case !ValidName(name):
		refuse(http.StatusUnauthorized, "the enrollment named no valid workspace")
		return
	}
	h.mu.Lock()
	_, replaces := h.tunnels[name]
	closed, full := h.closed, len(h.tunnels) >= h.cfg.maxWorkspaces && !replaces
	h.mu.Unlock()
	switch {
	case closed:
		refuse(http.StatusServiceUnavailable, "the hub is closed")
		return
	case full:
		refuse(http.StatusServiceUnavailable, fmt.Sprintf("%d workspaces are connected, the most there may be", h.cfg.maxWorkspaces))
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept answered it
	}
	nc := conn(h.ctx, c, h.cfg.link.maxMessage)
	if _, err := h.open(name, nc); err != nil {
		h.cfg.link.log.Info("tunnel: the connection was not taken", "workspace", name, "err", err)
		nc.Close()
		return
	}
	// The connection is held by its HTTP/2 client from here; the
	// handler returning leaves it open.
	h.cfg.link.log.Info("tunnel: connected", "workspace", name)
}

// open starts the HTTP/2 client over nc, waits for it to answer a
// request, and puts it in the table, closing the one it replaces. The
// bound of workspaces is checked again here, as upgrades are answered
// at once.
func (h *Hub) open(name string, nc net.Conn) (*tunnel, error) {
	tr := &http.Transport{
		Protocols: h2c(),
		HTTP2:     h.cfg.link.http2(),
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nc, nil
		},
	}
	ctx, cancel := context.WithTimeout(h.ctx, h.cfg.link.handshake)
	defer cancel()
	cc, err := tr.NewClientConn(ctx, "http", name+":80")
	if err != nil {
		return nil, err
	}
	t := &tunnel{h: h, name: name, nc: nc, cc: cc}
	cc.SetStateHook(func(cc *http.ClientConn) {
		if cc.Err() != nil {
			go t.close()
		}
	})
	if err := t.probe(ctx); err != nil {
		t.close()
		return nil, err
	}
	h.mu.Lock()
	old := h.tunnels[name]
	switch {
	case t.dead:
		h.mu.Unlock()
		return nil, errors.New("tunnel: the connection ended before it was taken")
	case h.closed:
		h.mu.Unlock()
		t.close()
		return nil, ErrClosed
	case old == nil && len(h.tunnels) >= h.cfg.maxWorkspaces:
		// Another upgrade took the last place since this one was
		// answered.
		h.mu.Unlock()
		t.close()
		return nil, fmt.Errorf("tunnel: %d workspaces are connected, the most there may be", h.cfg.maxWorkspaces)
	}
	h.tunnels[name] = t
	h.notify()
	h.mu.Unlock()
	if old != nil {
		h.cfg.link.log.Info("tunnel: a new connection replaces the old", "workspace", name)
		old.close()
	}
	return t, nil
}

// probe sends one request over the tunnel and takes any answer as proof
// that the other end speaks HTTP/2: the remote handler answers a path
// it has no route for with an error of its own. NewClientConn does not
// wait for the peer's settings; this does.
func (t *tunnel) probe(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+t.name+".tunnel.invalid/", nil)
	if err != nil {
		return err
	}
	resp, err := t.cc.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("tunnel: the connection answered no request over HTTP/2: %w", err)
	}
	resp.Body.Close()
	return nil
}

// wait is name's tunnel, once there is one, or ErrNotConnected when
// ctx ends first.
func (h *Hub) wait(ctx context.Context, name string) (*tunnel, error) {
	for {
		h.mu.Lock()
		closed, t, changed := h.closed, h.tunnels[name], h.changed
		h.mu.Unlock()
		switch {
		case closed:
			return nil, ErrClosed
		case t != nil:
			return t, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, fmt.Errorf("tunnel: %s: %w (%w)", name, ErrNotConnected, ctx.Err())
		}
	}
}

// Workspace is the workspace name, over its tunnel: remote.Dial's
// result, with opts, through an HTTP client whose every request goes to
// name's connection. It waits for name to connect, as long as ctx
// allows (ErrNotConnected). As remote.Dial's, the workspace is an
// agentworkspace.Starter when the workspace end granted exec and serves
// a Starter, and an agentworkspace.ServerProvider when it granted
// servers and serves one.
//
// It keeps working across reconnects: a request made while name is not
// connected waits, bounded by its own context, for the next
// connection, so the client's renewals carry its session over a drop
// shorter than the lease. Requests in flight when a connection drops
// fail, and are not sent again.
//
// The workspace is the first connection's: its Root, Env, Scopes,
// bounds and whether it is a Starter or a ServerProvider are what that
// host's info said at the dial.
// When another host takes the name (a second connection replaces the
// first), requests go to the new host, but the workspace still
// describes the first, and its session, if it has one, is lost there
// (remote.ErrNoSession). A program that lets a name move between hosts
// dials again (Workspace) after a replacement, and does not write
// through a workspace dialled before it. Its Descriptor's Ref is
// http://<name>.tunnel.invalid, a host that resolves nowhere, as the
// tunnel is the only way there. A remote.WithHTTPClient among opts is
// replaced.
func (h *Hub) Workspace(ctx context.Context, name string, opts ...remote.DialOption) (remote.Workspace, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("tunnel: %q is not a workspace's name", name)
	}
	if _, err := h.wait(ctx, name); err != nil {
		return nil, err
	}
	hc := &http.Client{
		Transport:     route{h, name},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return remote.Dial(ctx, "http://"+name+".tunnel.invalid", append(slices.Clone(opts), remote.WithHTTPClient(hc))...)
}

// route sends every request to its workspace's connection, the one
// there is when it is sent, waiting for one as long as the request's
// context allows.
type route struct {
	h    *Hub
	name string
}

func (rt route) RoundTrip(req *http.Request) (*http.Response, error) {
	for {
		t, err := rt.h.wait(req.Context(), rt.name)
		if err != nil {
			if req.Body != nil {
				req.Body.Close()
			}
			return nil, err
		}
		if err := t.cc.Reserve(); err != nil && !t.usable() {
			// Unusable before this request was sent on it: it leaves
			// the table at once, so the next wait blocks for the next
			// connection rather than finding this one, and the request
			// may go on that.
			t.close()
			continue
		}
		// Reserved, or the connection is at its bound of streams, and
		// RoundTrip waits for one.
		resp, err := t.cc.RoundTrip(req)
		if err != nil && req.Context().Err() == nil && !t.usable() {
			// Told to go away, or out of stream ids, while it lasted:
			// closed, so the host dials again.
			t.close()
		}
		return resp, err
	}
}

// Connected is the names of the workspaces connected now, sorted.
func (h *Hub) Connected() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Sorted(maps.Keys(h.tunnels))
}

// Close closes every connection and refuses every upgrade after; a
// workspace Workspace returned fails every request from now on. It
// does not close those workspaces, which are the caller's.
func (h *Hub) Close() error {
	h.mu.Lock()
	h.closed = true
	all := slices.Collect(maps.Values(h.tunnels))
	h.notify()
	h.mu.Unlock()
	for _, t := range all {
		t.close()
	}
	h.stop()
	return nil
}
