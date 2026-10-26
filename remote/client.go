package remote

// The client half: a workspace at the other end of a Handler.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// Workspace is what Dial returns: an agentworkspace.Workspace at the
// other end of the wire, which also tells the served workspace's own
// descriptor. It is a *Client, or, when the client may start
// processes, a *StarterClient, so that a type assertion to
// agentworkspace.Starter tells the truth.
type Workspace interface {
	agentworkspace.Workspace
	// Served is the served workspace's own Descriptor, as against
	// Descriptor, which names this one: the remote at the base URL.
	Served() agentworkspace.Descriptor
}

// Client is a workspace served by a Handler, reached over HTTP. Its
// methods are safe for concurrent use.
type Client struct {
	base    url.URL // scheme, host and path; no userinfo, query or fragment
	http    *http.Client
	owned   bool // http is the client's own, whose idle connections Close closes
	token   string
	timeout time.Duration
	info    infoReply
	ctx     context.Context // ends with Close, and every request with it
	end     context.CancelFunc

	session string        // "" when the client starts nothing
	renewed chan struct{} // closed when the renewal ends

	mu     sync.Mutex
	closed bool
	lost   bool // the session lapsed
	procs  map[*process]struct{}
}

// StarterClient is a Client that may start processes: the served
// workspace is an agentworkspace.Starter and the principal holds exec.
type StarterClient struct{ *Client }

var (
	_ Workspace              = (*Client)(nil)
	_ agentworkspace.Starter = (*StarterClient)(nil)
)

// A DialOption configures Dial.
type DialOption func(*dialConfig)

type dialConfig struct {
	token   string
	http    *http.Client
	timeout time.Duration
}

// WithBearerToken sends tok on every request, as "Authorization: Bearer
// tok". The token is never put in a URL, an error or a Descriptor.
func WithBearerToken(tok string) DialOption { return func(c *dialConfig) { c.token = tok } }

// WithHTTPClient sends every request through hc. It must set no
// Timeout, which would cut Exec's frames and a process's streams short;
// the client bounds the requests that should be answered at once
// itself (WithTimeout). The default is a client of its own over
// http.DefaultTransport that follows no redirect.
func WithHTTPClient(hc *http.Client) DialOption { return func(c *dialConfig) { c.http = hc } }

// WithTimeout bounds each request that takes no context and should be
// answered at once (the file system's, a process's start, signal and
// close, a session's renewal); the default is 30 seconds. A stream, a
// wait and an Exec are bounded by their contexts alone.
func WithTimeout(d time.Duration) DialOption { return func(c *dialConfig) { c.timeout = d } }

// Dial connects to the Handler at baseURL: it asks for the served
// workspace's description and, when the workspace can start processes
// and the principal holds exec, opens the session the processes live
// in, which the client renews in the background until Close. ctx bounds
// the dial alone.
//
// The workspace returned is a *StarterClient in that case and a
// *Client otherwise. Its Root is the served workspace's, its Descriptor
// is KindRemote with the base URL as its Ref, and Served is the served
// workspace's own. A base URL with userinfo is refused (give a token
// with WithBearerToken); its query and fragment are dropped.
func Dial(ctx context.Context, baseURL string, opts ...DialOption) (Workspace, error) {
	u, err := url.Parse(baseURL)
	switch {
	case err != nil:
		// url's error quotes the URL, which may hold a credential.
		return nil, errors.New("remote: the base URL does not parse")
	case u.User != nil:
		return nil, errors.New("remote: a base URL with userinfo; give a token with WithBearerToken")
	case u.Scheme != "http" && u.Scheme != "https":
		return nil, fmt.Errorf("remote: a base URL of scheme %q; want http or https", u.Scheme)
	case u.Host == "":
		return nil, errors.New("remote: a base URL with no host")
	}
	u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment = "", false, "", ""
	u.Path, u.RawPath = strings.TrimSuffix(u.Path, "/"), ""
	cfg := dialConfig{timeout: 30 * time.Second}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	c := &Client{base: *u, http: cfg.http, token: cfg.token, timeout: cfg.timeout, procs: map[*process]struct{}{}}
	if c.http == nil {
		c.http = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		c.owned = true
	}
	if c.timeout <= 0 {
		return nil, errors.New("remote: a timeout that is not positive")
	}
	c.ctx, c.end = context.WithCancel(context.Background())
	if err := c.call(ctx, http.MethodGet, routeInfo, nil, nil, &c.info); err != nil {
		c.end()
		return nil, err
	}
	if c.info.Root == "" || !c.info.Limits.valid() {
		c.end()
		return nil, errors.New("remote: the server's description has no root or a bound that is not positive")
	}
	if !c.info.Start || !slices.Contains(c.info.Scopes, ScopeExec) {
		return c, nil
	}
	var s sessionReply
	if err := c.call(ctx, http.MethodPost, routeSessions, nil, struct{}{}, &s); err != nil {
		c.end()
		return nil, err
	}
	if !validID(s.ID) || s.LeaseMs <= 0 {
		c.end()
		return nil, errors.New("remote: the server opened a session with no id or no lease")
	}
	c.session, c.renewed = s.ID, make(chan struct{})
	go c.renew(time.Duration(s.LeaseMs) * time.Millisecond)
	return &StarterClient{c}, nil
}

// renew renews the session every third of its lease until Close, and
// marks it lost when the server says it lapsed.
func (c *Client) renew(lease time.Duration) {
	defer close(c.renewed)
	every := max(lease/3, time.Millisecond)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(c.ctx, min(every, c.timeout))
		err := c.call(ctx, http.MethodPost, c.sessionRoute()+"/renew", nil, struct{}{}, nil)
		cancel()
		if errors.Is(err, ErrNoSession) {
			c.mu.Lock()
			c.lost = true
			c.mu.Unlock()
			return
		}
		// Anything else is tried again on the next tick: a lease is three
		// renewals long.
	}
}

func (c *Client) sessionRoute() string { return routeSessions + "/" + c.session }

// Root is the served workspace's Root.
func (c *Client) Root() string { return c.info.Root }

// Env is the served workspace's Env, a copy, as it was at Dial; empty
// for a principal without exec, whom the server does not tell it.
func (c *Client) Env() []string { return slices.Clone(c.info.Env) }

// Descriptor names the remote: KindRemote, the base URL with no
// userinfo, query or fragment as its Ref, and the served Root.
func (c *Client) Descriptor() agentworkspace.Descriptor {
	return agentworkspace.Descriptor{Kind: agentworkspace.KindRemote, Ref: c.base.String(), Root: c.info.Root}
}

// Served is the served workspace's own Descriptor.
func (c *Client) Served() agentworkspace.Descriptor { return c.info.Served.workspace() }

// Close ends every process the client started, as Process.Close does,
// closes its session, which ends whatever it did not reach, and ends
// every request in flight. It may be called more than once.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	procs := make([]*process, 0, len(c.procs))
	for p := range c.procs {
		procs = append(procs, p)
	}
	c.mu.Unlock()
	var wg sync.WaitGroup
	for _, p := range procs {
		wg.Go(func() { p.Close() })
	}
	wg.Wait()
	var err error
	if c.session != "" {
		ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
		err = c.call(ctx, http.MethodDelete, c.sessionRoute(), nil, nil, nil)
		cancel()
		if errors.Is(err, ErrNoSession) {
			err = nil // lapsed, and so ended, already
		}
	}
	c.end()
	if c.renewed != nil {
		<-c.renewed
	}
	if c.owned {
		c.http.CloseIdleConnections()
	}
	return err
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// with is ctx, ended also by Close.
func (c *Client) with(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

// url is route under the base URL, with q.
func (c *Client) url(route string, q url.Values) string {
	u := c.base
	u.Path += route
	u.RawQuery = q.Encode()
	return u.String()
}

// request sends one request and returns the response when it is a
// success; a refusal is the server's error, mapped back.
func (c *Client) request(ctx context.Context, method, route string, q url.Values, body io.Reader, ctype string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.url(route, q), body)
	if err != nil {
		return nil, err
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		return nil, refusal(resp, method, route)
	}
	return resp, nil
}

// refusal is a response that is not a success as an error: the
// server's, when it sent one, and the status otherwise.
func refusal(resp *http.Response, method, route string) error {
	var r errorReply
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&r); err == nil && r.Error.Code != "" {
		return r.Error.err()
	}
	return fmt.Errorf("remote: %s %s: %s", method, route, resp.Status)
}

// call sends in, when not nil, as JSON and decodes the reply into out,
// when not nil.
func (c *Client) call(ctx context.Context, method, route string, q url.Values, in, out any) error {
	var body io.Reader
	ctype := ""
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body, ctype = bytes.NewReader(data), "application/json"
	}
	resp, err := c.request(ctx, method, route, q, body, ctype)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, MaxReplyBytes))
		return err
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, MaxReplyBytes)).Decode(out); err != nil {
		return fmt.Errorf("remote: %s %s: the reply: %w", method, route, err)
	}
	return nil
}

// WriteFile creates or replaces a regular file in the served workspace,
// at most the server's write bound of it (ErrLimit).
func (c *Client) WriteFile(ctx context.Context, name string, data []byte, perm fs.FileMode) error {
	if err := valid("write", name); err != nil {
		return err
	}
	if c.isClosed() {
		return &fs.PathError{Op: "write", Path: name, Err: fs.ErrClosed}
	}
	if n := int64(len(data)); n > c.info.Limits.MaxWriteBytes {
		return &fs.PathError{Op: "write", Path: name, Err: fmt.Errorf("%w: %d bytes, over the server's %d", ErrLimit, n, c.info.Limits.MaxWriteBytes)}
	}
	ctx, done := c.with(ctx)
	defer done()
	q := url.Values{"path": {name}, "perm": {strconv.FormatUint(uint64(perm), 10)}}
	resp, err := c.request(ctx, http.MethodPut, routeFile, q, bytes.NewReader(data), "application/octet-stream")
	if err != nil {
		return &fs.PathError{Op: "write", Path: name, Err: err}
	}
	resp.Body.Close()
	return nil
}

// Remove deletes a file or an empty directory in the served workspace.
func (c *Client) Remove(ctx context.Context, name string) error {
	if err := valid("remove", name); err != nil {
		return err
	}
	if c.isClosed() {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrClosed}
	}
	ctx, done := c.with(ctx)
	defer done()
	if err := c.call(ctx, http.MethodDelete, routeFile, url.Values{"path": {name}}, nil, nil); err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}
	return nil
}

// Exec runs one command in the served workspace. The request's context
// is the process's on the server: ctx's end, or Close, ends it and
// everything it started. Command.Timeout is sent, and the server ends
// the process at it. Command.Stream gets the output as the server sends
// it; when writing to it fails, the command is ended and Exec returns
// that error.
func (c *Client) Exec(ctx context.Context, cmd agentworkspace.Command) (*agentworkspace.Output, error) {
	switch {
	case len(cmd.Args) == 0:
		return nil, errors.New("remote: no command")
	case int64(len(cmd.Stdin)) > c.info.Limits.MaxWriteBytes:
		return nil, fmt.Errorf("remote: exec %s: %w: stdin of %d bytes, over the server's %d", cmd.Args[0], ErrLimit, len(cmd.Stdin), c.info.Limits.MaxWriteBytes)
	case c.isClosed():
		return nil, fmt.Errorf("remote: exec %s: %w", cmd.Args[0], fs.ErrClosed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rctx, done := c.with(ctx)
	defer done()
	body, err := json.Marshal(execRequest{
		Args: cmd.Args, Dir: cmd.Dir, Env: cmd.Env, Stdin: cmd.Stdin,
		TimeoutMs: millis(cmd.Timeout), Stream: cmd.Stream != nil,
	})
	if err != nil {
		return nil, err
	}
	// ended is the error of a request that ended under ctx or Close.
	ended := func(err error) error {
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case c.ctx.Err() != nil:
			return fmt.Errorf("remote: exec %s: %w", cmd.Args[0], fs.ErrClosed)
		}
		return fmt.Errorf("remote: exec %s: %w", cmd.Args[0], err)
	}
	resp, err := c.request(rctx, http.MethodPost, routeExec, nil, bytes.NewReader(body), "application/json")
	if err != nil {
		return nil, ended(err)
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	for {
		var f execFrame
		if err := dec.Decode(&f); err != nil {
			return nil, ended(fmt.Errorf("the reply ended without an outcome: %w", err))
		}
		switch {
		case f.Error != nil:
			return nil, fmt.Errorf("remote: exec %s: %w", cmd.Args[0], f.Error.err())
		case f.Output != nil:
			o := f.Output
			return &agentworkspace.Output{Stdout: o.Stdout, Stderr: o.Stderr, ExitCode: o.ExitCode, TimedOut: o.TimedOut}, nil
		case len(f.Data) > 0 && cmd.Stream != nil:
			if _, err := cmd.Stream.Write(f.Data); err != nil {
				return nil, err
			}
		case len(f.Data) > 0:
			return nil, fmt.Errorf("remote: exec %s: output sent for a stream not asked for", cmd.Args[0])
		}
	}
}
