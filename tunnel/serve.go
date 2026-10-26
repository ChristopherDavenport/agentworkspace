package tunnel

// The workspace end: it dials the server, and serves its workspace
// over the connection.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
	"github.com/coder/websocket"
)

// The defaults of the workspace end's redial (WithRedial).
const (
	// DefaultRedialMin is the first wait before a redial, and the wait
	// after a connection that lasted.
	DefaultRedialMin = 500 * time.Millisecond
	// DefaultRedialMax bounds the wait between redials, which doubles
	// with each that fails.
	DefaultRedialMax = 30 * time.Second
)

// stable is how long a connection lasts for the next redial to wait
// the least again.
const stable = 10 * time.Second

type serveConfig struct {
	link      linkConfig
	scopes    []remote.Scope
	scoped    bool
	hc        *http.Client
	header    func(context.Context) (http.Header, error)
	handler   []remote.Option
	redialMin time.Duration
	redialMax time.Duration
	jitter    func(time.Duration) time.Duration // a wait, jittered
}

type serveOption func(*serveConfig)

func (o serveOption) applyServe(c *serveConfig) { o(c) }

// WithScopes is what the server may do with the workspace: every
// request that comes through the tunnel is from one principal, named
// after the server's host, holding these scopes and no others. It is
// required: Serve refuses to serve without it, and refuses an empty
// list. exec is implied by neither read nor write, as in remote.
func WithScopes(scopes ...remote.Scope) Option {
	return serveOption(func(c *serveConfig) { c.scopes, c.scoped = slices.Clone(scopes), true })
}

// WithHTTPClient makes the WebSocket upgrade through hc, which is where
// the application's credentials and its TLS go: a Transport that sets
// the request's Authorization header, a TLS config with a client
// certificate or the server's CA. This package sends no credentials of
// its own. hc must be an http.Client whose Transport returns a writable
// body for a 101 (http.Transport does); a redirect is followed only as
// hc's CheckRedirect allows. The default is a client of its own over
// http.DefaultTransport, which verifies the server's certificate
// against the system's roots and follows no redirect.
func WithHTTPClient(hc *http.Client) Option {
	return serveOption(func(c *serveConfig) { c.hc = hc })
}

// WithHeader adds the header f returns to each upgrade, f called once
// for each, so a credential that expires is fetched fresh on every
// redial. An error from f is a failed dial, tried again later; its
// text is logged, so it must hold no credential.
func WithHeader(f func(ctx context.Context) (http.Header, error)) Option {
	return serveOption(func(c *serveConfig) { c.header = f })
}

// WithHandlerOptions passes opts to the remote.Handler the workspace is
// served by: its lease and its bounds. An authenticator among them is
// replaced by the tunnel's, and WithInsecureNoAuth is refused.
func WithHandlerOptions(opts ...remote.Option) Option {
	return serveOption(func(c *serveConfig) { c.handler = append(c.handler, opts...) })
}

// WithRedial is the wait between redials: min at first and after a
// connection that lasted, doubling after each failure up to max, each
// wait jittered to between half of it and all of it. The defaults are
// DefaultRedialMin and DefaultRedialMax.
func WithRedial(min, max time.Duration) Option {
	return serveOption(func(c *serveConfig) { c.redialMin, c.redialMax = min, max })
}

// Serve dials the hub at serverURL and serves ws to it through the
// connection until ctx ends, and dials again whenever the connection
// drops. serverURL is wss://, or ws:// to a loopback host alone, with
// no userinfo; its path is the one the server mounted its Hub at.
//
// The workspace is served by one remote.Handler for all of Serve's
// life, so a session the server opened, and the processes it started,
// outlive a drop shorter than the session's lease: the server's client
// renews the session through the next connection. Past the lease they
// end as remote's do. When ctx ends, Serve closes the connection and
// the handler, which ends every process the server started, and
// returns ctx's error once they have ended.
//
// It returns early with an error when it cannot serve at all: a URL it
// will not dial, no WithScopes, an option out of bounds, or an upgrade
// the hub refuses as unauthenticated (401, remote.ErrUnauthenticated)
// or forbidden (403, remote.ErrForbidden), which another dial would not
// change. Any other failure to dial, a hub that is full or not up yet
// included, is tried again after the redial's wait.
func Serve(ctx context.Context, serverURL string, ws agentworkspace.Workspace, opts ...Option) error {
	cfg := serveConfig{
		link:      defaultLink(),
		redialMin: DefaultRedialMin,
		redialMax: DefaultRedialMax,
		jitter:    func(d time.Duration) time.Duration { return d/2 + rand.N(d/2+1) },
	}
	for _, o := range opts {
		if o != nil {
			o.applyServe(&cfg)
		}
	}
	u, err := dialURL(serverURL)
	if err != nil {
		return err
	}
	switch {
	case ws == nil:
		return errors.New("tunnel: no workspace to serve")
	case !cfg.scoped || len(cfg.scopes) == 0:
		return errors.New("tunnel: no scopes; say what the server may do with WithScopes")
	case cfg.redialMin <= 0 || cfg.redialMax < cfg.redialMin:
		return errors.New("tunnel: a redial wait that is not positive, or a max below the min")
	}
	for _, s := range cfg.scopes {
		if s != remote.ScopeRead && s != remote.ScopeWrite && s != remote.ScopeExec {
			return fmt.Errorf("tunnel: no scope %q", s)
		}
	}
	if err := cfg.link.valid(); err != nil {
		return err
	}
	if cfg.hc == nil {
		cfg.hc = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if cfg.hc.Timeout != 0 {
		return errors.New("tunnel: an HTTP client with a Timeout, which would end every tunnel at it")
	}
	// Every request through the tunnel is the server's: the tunnel is
	// the authentication, the server having been dialled and its
	// upgrade accepted.
	p := remote.Principal{Name: u.Host, Scopes: cfg.scopes}
	grant := func(*http.Request) (remote.Principal, error) {
		return remote.Principal{Name: p.Name, Scopes: slices.Clone(p.Scopes)}, nil
	}
	h, err := remote.NewHandler(ws, append(slices.Clone(cfg.handler), remote.WithAuthenticator(grant))...)
	if err != nil {
		return err
	}
	defer h.Close()
	s := &server{cfg: cfg, url: u.String(), host: u.Host, h: h}
	return s.run(ctx)
}

// dialURL is serverURL checked: wss, or ws to a loopback host, and no
// userinfo. Its errors do not quote it.
func dialURL(serverURL string) (*url.URL, error) {
	u, err := url.Parse(serverURL)
	switch {
	case err != nil:
		return nil, errors.New("tunnel: the server URL does not parse")
	case u.User != nil:
		return nil, errors.New("tunnel: a server URL with userinfo; credentials go in the HTTP client or a header (WithHTTPClient, WithHeader)")
	case u.Host == "":
		return nil, errors.New("tunnel: a server URL with no host")
	case u.Scheme == "wss":
	case u.Scheme == "ws" && loopback(u.Hostname()):
	case u.Scheme == "ws":
		return nil, errors.New("tunnel: ws:// to a host that is not loopback; use wss://")
	default:
		return nil, fmt.Errorf("tunnel: a server URL of scheme %q; want wss", u.Scheme)
	}
	u.Fragment, u.RawFragment = "", ""
	return u, nil
}

// loopback reports whether host is localhost or a loopback address.
func loopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// server is one Serve's: the handler every connection serves.
type server struct {
	cfg  serveConfig
	url  string
	host string
	h    *remote.Handler
}

// refusal is an upgrade the hub refused in a way another dial would
// not change.
type refusal struct{ err error }

func (r *refusal) Error() string { return r.err.Error() }
func (r *refusal) Unwrap() error { return r.err }

func (s *server) run(ctx context.Context) error {
	lg := s.cfg.link.log.With("server", s.host)
	wait := s.cfg.redialMin
	for {
		began := time.Now()
		err := s.once(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var r *refusal
		if errors.As(err, &r) {
			lg.Warn("tunnel: the server refused the workspace", "err", err)
			return r.err
		}
		if time.Since(began) >= stable {
			wait = s.cfg.redialMin
		}
		d := s.cfg.jitter(wait)
		lg.Info("tunnel: the connection ended; dialling again", "err", err, "in", d)
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		wait = min(2*wait, s.cfg.redialMax)
	}
}

// once dials, and serves the connection until it ends or ctx does.
func (s *server) once(ctx context.Context) error {
	dctx, cancel := context.WithTimeout(ctx, s.cfg.link.handshake)
	defer cancel()
	header := http.Header{}
	if s.cfg.header != nil {
		h, err := s.cfg.header(dctx)
		if err != nil {
			return fmt.Errorf("tunnel: the upgrade's header: %w", err)
		}
		for k, vs := range h {
			for _, v := range vs {
				header.Add(k, v)
			}
		}
	}
	c, resp, err := websocket.Dial(dctx, s.url, &websocket.DialOptions{HTTPClient: s.cfg.hc, HTTPHeader: header})
	if err != nil {
		return dialError(resp, err)
	}
	cancel()
	nc := conn(ctx, c, s.cfg.link.maxMessage)
	ended := make(chan struct{})
	var once sync.Once
	srv := &http.Server{
		Handler:   s.h,
		Protocols: h2c(),
		// Bounds the wait for the hub's HTTP/2 preface alone: past it,
		// HTTP/2's pings keep the watch.
		ReadHeaderTimeout: s.cfg.link.handshake,
		HTTP2:             s.cfg.link.http2(),
		ErrorLog:          log.New(errorLog(s.cfg.link.log), "", 0),
		ConnState: func(_ net.Conn, st http.ConnState) {
			if st == http.StateClosed || st == http.StateHijacked {
				once.Do(func() { close(ended) })
			}
		},
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		srv.Serve(listenOn(nc))
	}()
	s.cfg.link.log.Info("tunnel: connected", "server", s.host)
	select {
	case <-ended:
	case <-ctx.Done():
	}
	srv.Close()
	nc.Close()
	<-served
	return errors.New("tunnel: the connection ended")
}

// dialError is a failed upgrade's error, a refusal another dial would
// not change marked so, and with no URL in it.
func dialError(resp *http.Response, err error) error {
	if resp != nil {
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			return &refusal{fmt.Errorf("tunnel: the server refused the upgrade (%s): %w", resp.Status, remote.ErrUnauthenticated)}
		case http.StatusForbidden:
			return &refusal{fmt.Errorf("tunnel: the server refused the upgrade (%s): %w", resp.Status, remote.ErrForbidden)}
		}
		return fmt.Errorf("tunnel: the server answered the upgrade with %s", resp.Status)
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		// Its text quotes the URL.
		err = ue.Err
	}
	return fmt.Errorf("tunnel: dial: %w", err)
}
