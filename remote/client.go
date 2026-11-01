package remote

// The client half: a workspace at the other end of a Handler.

import (
	"bufio"
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
// descriptor. It is a *Client; a *StarterClient when the client may
// start processes; a *ServerClient when it may start the servers the
// served workspace offers; or a *StarterServerClient when it may do
// both: so that a type assertion to agentworkspace.Starter, and one to
// agentworkspace.ServerProvider, each tells the truth.
type Workspace interface {
	agentworkspace.Workspace
	// Served is the served workspace's own Descriptor, as against
	// Descriptor, which names this one: the remote at the base URL.
	Served() agentworkspace.Descriptor
	// Scopes are the scopes the server said, at Dial, the principal
	// holds; never empty.
	Scopes() []Scope
}

// Client is a workspace served by a Handler, reached over HTTP. Its
// methods are safe for concurrent use.
type Client struct {
	base    url.URL // scheme, host and path; no userinfo, query or fragment
	http    *http.Client
	owned   bool // http is the client's own, whose idle connections Close closes
	timeout time.Duration
	info    infoReply
	ctx     context.Context // ends with Close, and every request with it
	end     context.CancelFunc

	smu      sync.Mutex     // held while a session is opened
	renewers sync.WaitGroup // the renewals, which Close waits for

	mu      sync.Mutex
	closed  bool
	session string // "" until the first Start, and again once it is lost
	procs   map[*process]struct{}
}

// StarterClient is a Client that may start processes: the served
// workspace is an agentworkspace.Starter and the principal holds exec.
type StarterClient struct{ *Client }

// ServerClient is a Client that may start the MCP servers the served
// workspace offers and no process of its own: the served workspace is
// an agentworkspace.ServerProvider and the principal holds servers,
// and either the served workspace is no Starter or the principal lacks
// exec.
type ServerClient struct{ *Client }

// StarterServerClient is a Client that may do both: start processes,
// as a StarterClient, and the servers the served workspace offers, as
// a ServerClient.
type StarterServerClient struct{ *Client }

var (
	_ Workspace                     = (*Client)(nil)
	_ agentworkspace.Starter        = (*StarterClient)(nil)
	_ agentworkspace.ServerProvider = (*ServerClient)(nil)
	_ agentworkspace.Starter        = (*StarterServerClient)(nil)
	_ agentworkspace.ServerProvider = (*StarterServerClient)(nil)
)

// A DialOption configures Dial.
type DialOption func(*dialConfig)

type dialConfig struct {
	http    *http.Client
	timeout time.Duration
}

// WithHTTPClient sends every request through hc, which is where the
// program's credentials go: a Transport that sets a request's
// Authorization header, or a TLS config with a client certificate.
// This package sends none of its own, and puts nothing of hc's in a
// URL, an error or a Descriptor. hc must set no Timeout, which would
// cut Exec's frames and a process's streams short; the client bounds
// the requests that should be answered at once itself (WithTimeout),
// and should follow no redirect, which would send the credentials on.
// The default is a client of its own over http.DefaultTransport that
// follows no redirect and sends no credentials.
func WithHTTPClient(hc *http.Client) DialOption { return func(c *dialConfig) { c.http = hc } }

// WithTimeout bounds each request that takes no context and should be
// answered at once (the file system's, a session's opening and
// renewal, a process's start, signal and close); the default is 30
// seconds. A stream, a wait and an Exec are bounded by their contexts
// alone.
func WithTimeout(d time.Duration) DialOption { return func(c *dialConfig) { c.timeout = d } }

// Dial connects to the Handler at baseURL: it asks for the served
// workspace's description, and takes nothing on the server. ctx bounds
// the dial alone.
//
// The workspace returned is an agentworkspace.Starter (a *StarterClient)
// when the served workspace can start processes and the principal holds
// exec; an agentworkspace.ServerProvider (a *ServerClient) when the
// served workspace offers MCP servers and the principal holds servers; a
// *StarterServerClient, both, when both hold; and a *Client otherwise.
// Each opens the session its processes live in at its first Start or
// StartServer, and renews it in the background until Close; a session
// lost (lapsed on the server, or a whole lease unrenewed) ends its
// processes on this side too, and the next start opens another. Its Root is the served workspace's, its Descriptor
// is KindRemote with the base URL as its Ref, and Served is the served
// workspace's own. A base URL with userinfo is refused (credentials go
// in the HTTP client, WithHTTPClient); its query and fragment are
// dropped.
func Dial(ctx context.Context, baseURL string, opts ...DialOption) (Workspace, error) {
	u, err := url.Parse(baseURL)
	switch {
	case err != nil:
		// url's error quotes the URL, which may hold a credential.
		return nil, errors.New("remote: the base URL does not parse")
	case u.User != nil:
		return nil, errors.New("remote: a base URL with userinfo; credentials go in the HTTP client (WithHTTPClient)")
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
	c := &Client{base: *u, http: cfg.http, timeout: cfg.timeout, procs: map[*process]struct{}{}}
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
	if c.info.Root == "" || !c.info.Limits.valid() || len(c.info.Scopes) == 0 {
		c.end()
		return nil, errors.New("remote: the server's description has no root, no scopes or a bound that is not positive")
	}
	start := c.info.Start && slices.Contains(c.info.Scopes, ScopeExec)
	servers := c.info.Servers && slices.Contains(c.info.Scopes, ScopeServers)
	switch {
	case start && servers:
		return &StarterServerClient{c}, nil
	case start:
		return &StarterClient{c}, nil
	case servers:
		return &ServerClient{c}, nil
	}
	return c, nil
}

// openSession is the client's session, opened now if it has none.
func (c *Client) openSession(ctx context.Context) (string, error) {
	c.smu.Lock()
	defer c.smu.Unlock()
	c.mu.Lock()
	id, closed := c.session, c.closed
	c.mu.Unlock()
	switch {
	case closed:
		return "", fs.ErrClosed
	case id != "":
		return id, nil
	}
	var s sessionReply
	if err := c.call(ctx, http.MethodPost, routeSessions, nil, struct{}{}, &s); err != nil {
		return "", err
	}
	if !validID(s.ID) || s.LeaseMs <= 0 {
		return "", errors.New("remote: the server opened a session with no id or no lease")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.call(ctx, http.MethodDelete, routeSessions+"/"+s.ID, nil, nil, nil)
		return "", fs.ErrClosed
	}
	c.session = s.ID
	c.renewers.Add(1)
	c.mu.Unlock()
	go c.renew(s.ID, time.Duration(s.LeaseMs)*time.Millisecond)
	return s.ID, nil
}

// renew renews session id every third of its lease until Close, and
// loses it when the server says it is gone or a whole lease passes
// without a renewal that succeeded: by then the server has ended its
// processes.
func (c *Client) renew(id string, lease time.Duration) {
	defer c.renewers.Done()
	every := max(lease/3, time.Millisecond)
	t := time.NewTicker(every)
	defer t.Stop()
	renewed := time.Now()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(c.ctx, min(every, c.timeout))
		err := c.call(ctx, http.MethodPost, routeSessions+"/"+id+"/renew", nil, struct{}{}, nil)
		cancel()
		switch {
		case c.ctx.Err() != nil:
			return
		case err == nil:
			renewed = time.Now()
		case errors.Is(err, ErrNoSession):
			c.lose(id, fmt.Errorf("remote: the session ended on the server: %w", ErrNoSession))
			return
		case time.Since(renewed) >= lease:
			c.lose(id, fmt.Errorf("remote: the session went its lease (%v) unrenewed: %w", lease, ErrNoSession))
			return
		}
		// Anything else is tried again on the next tick: a lease is three
		// renewals long.
	}
}

// lose ends session id on this side: every process in it is finished
// with err, its streams failing with it, and the next Start opens
// another session.
func (c *Client) lose(id string, err error) {
	c.mu.Lock()
	if c.session != id {
		c.mu.Unlock()
		return
	}
	c.session = ""
	var lost []*process
	for p := range c.procs {
		if p.session == id {
			lost = append(lost, p)
			delete(c.procs, p)
		}
	}
	c.mu.Unlock()
	for _, p := range lost {
		p.lose(err)
	}
}

// sessionRoute is the route of the client's session, "" without one.
func (c *Client) sessionRoute() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session == "" {
		return ""
	}
	return routeSessions + "/" + c.session
}

// Root is the served workspace's Root; for a principal without read,
// whom the server does not tell its path, its last element alone.
func (c *Client) Root() string { return c.info.Root }

// Env is the served workspace's Env, a copy, as it was at Dial; empty
// for a principal without exec, whom the server does not tell it.
func (c *Client) Env() []string { return slices.Clone(c.info.Env) }

// Descriptor names the remote: KindRemote, the base URL with no
// userinfo, query or fragment as its Ref, and the served Root.
func (c *Client) Descriptor() agentworkspace.Descriptor {
	return agentworkspace.Descriptor{Kind: agentworkspace.KindRemote, Ref: c.base.String(), Root: c.info.Root}
}

// Served is the served workspace's own Descriptor, its Root as Root
// gives it.
func (c *Client) Served() agentworkspace.Descriptor { return c.info.Served.workspace() }

// Scopes are the scopes the server said the principal holds when it
// answered Dial, a copy, in the server's order: what the program's
// Authenticator granted the credentials then. They are not asked
// again, so a grant changed since is not in them, and the server
// checks every request against the principal it resolves for it,
// whatever they say. They are never empty: Dial refuses a description
// that names none (a Handler's info route itself takes read or
// servers). They tell a program what a refusal will be before it asks:
// without ScopeWrite, WriteFile and Remove are ErrForbidden.
func (c *Client) Scopes() []Scope { return slices.Clone(c.info.Scopes) }

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
	// A Start opening a session now finds the client closed.
	c.smu.Lock()
	if route := c.sessionRoute(); route != "" {
		ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
		err = c.call(ctx, http.MethodDelete, route, nil, nil, nil)
		cancel()
		if errors.Is(err, ErrNoSession) {
			err = nil // lapsed, and so ended, already
		}
	}
	c.smu.Unlock()
	c.end()
	c.renewers.Wait()
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
	// The permission bits alone, as the server takes them.
	q := url.Values{"path": {name}, "perm": {strconv.FormatUint(uint64(perm&fs.ModePerm), 10)}}
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
// everything it started. Ended by ctx, Exec returns an Output with
// ExitCode -1 and Interrupted beside ctx.Err(), whose Stdout and Stderr
// are empty: the wire carries nothing past the hang-up but what a
// Command.Stream was given. Command.Timeout is sent, and the server ends
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
	// fail answers a request that ended. One ctx ended is interrupted:
	// what of its output the Stream was given is all the wire carries
	// past a hang-up, so the Output holds none (#5's wire change is to
	// come).
	fail := func(err error) (*agentworkspace.Output, error) {
		if err = ended(err); ctx.Err() != nil && err == ctx.Err() {
			return &agentworkspace.Output{ExitCode: -1, Interrupted: true}, err
		}
		return nil, err
	}
	resp, err := c.request(rctx, http.MethodPost, routeExec, nil, bytes.NewReader(body), "application/json")
	if err != nil {
		return fail(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	limit := frameLimit(c.info.Limits.MaxOutputBytes)
	for {
		line, err := readFrame(br, limit)
		if errors.Is(err, ErrLimit) {
			return nil, fmt.Errorf("remote: exec %s: %w", cmd.Args[0], err)
		}
		if err != nil {
			return fail(fmt.Errorf("the reply ended without an outcome: %w", err))
		}
		var f execFrame
		if err := json.Unmarshal(line, &f); err != nil {
			return nil, fmt.Errorf("remote: exec %s: a frame of the reply: %w", cmd.Args[0], err)
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

// readFrame reads one line of an NDJSON reply, at most limit bytes of
// it: a longer one is ErrLimit, read no further.
func readFrame(br *bufio.Reader, limit int64) ([]byte, error) {
	var line []byte
	for {
		chunk, err := br.ReadSlice('\n')
		if int64(len(line)+len(chunk)) > limit {
			return nil, fmt.Errorf("%w: a frame over the %d bytes the server's bounds allow", ErrLimit, limit)
		}
		line = append(line, chunk...)
		switch {
		case err == nil:
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case err == io.EOF && len(line) > 0:
			return nil, io.ErrUnexpectedEOF
		default:
			return nil, err
		}
	}
}
