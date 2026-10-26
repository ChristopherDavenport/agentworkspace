package remote

// The client half of the processes: a process started in the session,
// as an agentworkspace.Process.

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// pumpWait is how long a process's end waits for the rest of its
// standard error to reach Command.Stream, as Local's WaitDelay does: a
// Stream that does not take it by then is left behind.
const pumpWait = 5 * time.Second

// Start starts cmd in the served workspace, in the client's session
// (opened now, if this is the first Start or the last session was
// lost), and returns at once. The process lives no longer than its
// owner: Process.Close, the end of ctx, cmd.Timeout (sent, and kept by
// the server), the client's Close and the session's loss each end it
// and everything it started. Its standard input is a stream sent as it
// is written, opened on the first write or close; its standard output
// is a stream opened on the first read; and its standard error, when
// cmd.Stream is set, is streamed there in the background, StderrBytes
// of it held by the server while the stream is behind and the rest
// dropped. A stream cut short is not resumed. cmd.Stdin is refused;
// write to the process's input instead.
//
// The start is sent with a key of the client's choosing, which names
// the process: when its reply is lost, the client closes the process
// by the key, so nothing runs that no one can name.
func (s *StarterClient) Start(ctx context.Context, cmd agentworkspace.Command) (agentworkspace.Process, error) {
	return s.Client.start(ctx, cmd)
}

// start is Start, for each client type that may start processes.
func (c *Client) start(ctx context.Context, cmd agentworkspace.Command) (agentworkspace.Process, error) {
	switch {
	case cmd.Stdin != nil:
		return nil, errors.New("remote: Start takes no Command.Stdin; write to Process.Stdin")
	case len(cmd.Args) == 0:
		return nil, errors.New("remote: no command")
	}
	return c.launch(ctx, "start "+cmd.Args[0], "/processes", cmd.Stream, func(key string) any {
		return startRequest{
			Args: cmd.Args, Dir: cmd.Dir, Env: cmd.Env, TimeoutMs: millis(cmd.Timeout), Stderr: cmd.Stream != nil, Key: key,
		}
	})
}

// launch starts a process in the client's session, opened now if it
// has none, by a POST to route under the session with the body request
// makes of the start's key, and returns it at once, watching ctx; what
// names the start in an error, and stream, when set, is where its
// standard error goes. A reply that is lost, or is not one, has the
// process closed by its key, so nothing runs that no one can name.
func (c *Client) launch(ctx context.Context, what, route string, stream io.Writer, request func(key string) any) (agentworkspace.Process, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fail := func(err error) (agentworkspace.Process, error) {
		return nil, fmt.Errorf("remote: %s: %w", what, err)
	}
	// Not under ctx: a start that ctx cut short might have started a
	// process no one could name. The end of ctx is watched once the
	// process has an id.
	rctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	defer cancel()
	sid, err := c.openSession(rctx)
	if err != nil {
		return fail(err)
	}
	key := rand.Text()
	proute := routeSessions + "/" + sid + "/processes/" + key
	var st startedReply
	err = c.call(rctx, http.MethodPost, routeSessions+"/"+sid+route, nil, request(key), &st)
	if err == nil && st.ID != key {
		err = fmt.Errorf("the server named the process %q, not by its key", st.ID)
	}
	if err != nil {
		var refused *remoteError
		if !errors.As(err, &refused) {
			// The reply was lost, or is not one: the process may be
			// running, so it is closed by its key.
			dctx, dcancel := context.WithTimeout(c.ctx, c.timeout)
			c.call(dctx, http.MethodDelete, proute, nil, nil, nil)
			dcancel()
		}
		return fail(err)
	}
	p := &process{c: c, session: sid, route: proute, stream: stream, done: make(chan struct{})}
	p.ctx, p.cancel = context.WithCancel(c.ctx)
	p.stdin = &stdinPipe{p: p}
	p.stdout = &stdoutPipe{p: p}
	if p.stream != nil {
		p.pumped = make(chan struct{})
		var pctx context.Context
		pctx, p.unpump = context.WithCancel(p.ctx)
		go p.pump(pctx)
	}
	// Watched before anyone else can reach it, so that a Close racing
	// in stops the watch too.
	p.mu.Lock()
	p.unwatch = context.AfterFunc(ctx, func() { p.ended(ctx) })
	p.mu.Unlock()
	c.mu.Lock()
	switch {
	case c.closed:
		c.mu.Unlock()
		p.Close()
		return fail(fs.ErrClosed)
	case c.session != sid:
		c.mu.Unlock()
		err := fmt.Errorf("the session was lost: %w", ErrNoSession)
		p.lose(err)
		return fail(err)
	}
	c.procs[p] = struct{}{}
	c.mu.Unlock()
	go p.poll()
	return p, nil
}

// process is a process the client started.
type process struct {
	c       *Client
	session string // the id of the session it is in
	route   string
	ctx     context.Context // its requests'; ended by Close
	cancel  context.CancelFunc
	stdin   *stdinPipe
	stdout  *stdoutPipe
	stream  io.Writer
	pumped  chan struct{} // closed when the stream of standard error ends; nil without one
	unpump  context.CancelFunc

	mu      sync.Mutex
	ending  error // the start's context's error, once it began ending the process
	lost    error // why the session was lost, once it was
	closing bool
	unwatch func() bool

	finishOnce sync.Once
	done       chan struct{} // closed once status and err are set
	status     int
	err        error

	closeOnce sync.Once
	closeErr  error
}

func (p *process) Stdin() io.WriteCloser { return p.stdin }
func (p *process) Stdout() io.ReadCloser { return p.stdout }

func (p *process) Wait() (int, error) {
	<-p.done
	return p.status, p.err
}

// Signal sends sig, by name, to the process and to everything it
// started. A signal the wire does not name is refused.
func (p *process) Signal(sig os.Signal) error {
	s, ok := sig.(syscall.Signal)
	if !ok {
		return fmt.Errorf("remote: cannot send %v", sig)
	}
	name, ok := signalName(s)
	if !ok {
		return fmt.Errorf("remote: cannot send %v", sig)
	}
	select {
	case <-p.done:
		return os.ErrProcessDone
	default:
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.c.timeout)
	defer cancel()
	return p.c.call(ctx, http.MethodPost, p.route+"/signal", nil, signalRequest{Signal: name}, nil)
}

// Close ends the process, if it is still running, and everything it
// started, as the served workspace's Process.Close does, and releases
// its streams. It returns once the process has exited, and may be
// called more than once.
func (p *process) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closing = true
		p.mu.Unlock()
		// The server's close waits for the process, a grace or two.
		ctx, cancel := context.WithTimeout(p.c.ctx, p.c.timeout+10*time.Second)
		var e exitReply
		err := p.c.call(ctx, http.MethodDelete, p.route, nil, nil, &e)
		cancel()
		if err != nil {
			p.cancel()
			p.finish(exitReply{}, err)
		} else {
			p.finish(e, nil)
			p.cancel()
		}
		p.stdout.Close()
		p.c.mu.Lock()
		delete(p.c.procs, p)
		p.c.mu.Unlock()
		if err != nil && p.err != err {
			err = nil // it had ended already, and its end is what Wait says
		}
		p.closeErr = err
	})
	return p.closeErr
}

// lose finishes the process with err, its session lost, and ends its
// requests: its streams fail with err from now on.
func (p *process) lose(err error) {
	p.mu.Lock()
	p.lost = err
	p.mu.Unlock()
	p.cancel()
	p.finish(exitReply{}, err)
}

// gone is the error of a stream the process's end cut: why its session
// was lost, or os.ErrClosed after Close.
func (p *process) gone() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lost != nil {
		return p.lost
	}
	return os.ErrClosed
}

// ended is the end of the start's context: the process is ended as the
// served workspace ends one whose context ended, and Wait reports that
// context's error.
func (p *process) ended(ctx context.Context) {
	p.mu.Lock()
	select {
	case <-p.done:
		p.mu.Unlock()
		return
	default:
	}
	p.ending = ctx.Err()
	p.mu.Unlock()
	rctx, cancel := context.WithTimeout(p.ctx, p.c.timeout)
	defer cancel()
	p.c.call(rctx, http.MethodPost, p.route+"/cancel", nil, struct{}{}, nil)
}

// poll waits for the process on the server, again after a request
// that failed, until it has an answer or the process is closed.
func (p *process) poll() {
	delay := 50 * time.Millisecond
	for {
		var e exitReply
		err := p.c.call(p.ctx, http.MethodGet, p.route+"/wait", nil, nil, &e)
		if err == nil {
			p.finish(e, nil)
			return
		}
		p.mu.Lock()
		closing := p.closing
		p.mu.Unlock()
		if closing || p.ctx.Err() != nil {
			return // Close says how it ended
		}
		if errors.Is(err, ErrNoSession) || errors.Is(err, ErrNoProcess) || errors.Is(err, ErrUnauthenticated) || errors.Is(err, ErrForbidden) {
			p.finish(exitReply{}, err)
			return
		}
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-p.ctx.Done():
			t.Stop()
			return
		}
		delay = min(2*delay, 2*time.Second)
	}
}

// finish records how the process ended, from the server's answer or
// the failure that stands for one, once standard error has reached
// Command.Stream, and stops watching the start's context.
func (p *process) finish(e exitReply, failed error) {
	p.finishOnce.Do(func() {
		status, err := e.Status, error(nil)
		p.mu.Lock()
		switch {
		case failed != nil:
			status, err = -1, failed
		case e.Ended == endedTimeout:
			err = context.DeadlineExceeded
		case e.Ended == endedCanceled && p.ending != nil:
			err = p.ending
		case e.Ended == endedCanceled:
			err = context.Canceled
		case e.Error != "":
			err = errors.New("remote: " + e.Error)
		}
		if p.unwatch != nil {
			p.unwatch()
		}
		p.mu.Unlock()
		if p.pumped != nil {
			t := time.NewTimer(pumpWait)
			select {
			case <-p.pumped:
			case <-t.C:
				// What is left goes nowhere; a Write that never returns
				// keeps its goroutine, not Wait.
				p.unpump()
			}
			t.Stop()
		}
		p.status, p.err = status, err
		close(p.done)
	})
}

// pump copies standard error to Command.Stream until it ends.
func (p *process) pump(ctx context.Context) {
	defer close(p.pumped)
	resp, err := p.c.request(ctx, http.MethodGet, p.route+"/stderr", nil, nil, "")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if _, err := io.Copy(p.stream, resp.Body); err != nil {
		// Keep reading, so the server's copy keeps up with the process.
		io.Copy(io.Discard, resp.Body)
	}
}

// stdoutPipe is the process's standard output: a stream opened on the
// first read.
type stdoutPipe struct {
	p      *process
	mu     sync.Mutex
	body   io.ReadCloser
	err    error // the open's failure
	closed bool
}

func (s *stdoutPipe) Read(b []byte) (int, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, os.ErrClosed
	}
	if s.body == nil && s.err == nil {
		resp, err := s.p.c.request(s.p.ctx, http.MethodGet, s.p.route+"/stdout", nil, nil, "")
		if err != nil {
			s.err = err
		} else {
			s.body = resp.Body
		}
	}
	body, err := s.body, s.err
	s.mu.Unlock()
	if err != nil {
		if s.p.ctx.Err() != nil {
			err = s.p.gone()
		}
		return 0, err
	}
	n, err := body.Read(b)
	if err != nil && err != io.EOF && s.p.ctx.Err() != nil {
		err = s.p.gone()
	}
	return n, err
}

func (s *stdoutPipe) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.body != nil {
		s.body.Close()
	}
	return nil
}

// stdinPipe is the process's standard input: one request whose body is
// what is written, opened on the first write or close. Its end is the
// process's end of file.
type stdinPipe struct {
	p      *process
	mu     sync.Mutex
	pw     *io.PipeWriter
	done   chan struct{} // closed when the request has its answer
	err    error         // the answer: nil, or why the stream ended
	closed bool
}

// open starts the request; s.mu is held.
func (s *stdinPipe) open() {
	pr, pw := io.Pipe()
	s.pw, s.done = pw, make(chan struct{})
	go func() {
		defer close(s.done)
		resp, err := s.p.c.request(s.p.ctx, http.MethodPost, s.p.route+"/stdin", nil, pr, "application/octet-stream")
		if err == nil {
			resp.Body.Close()
		}
		s.err = err
		pr.CloseWithError(err)
	}()
}

func (s *stdinPipe) Write(b []byte) (int, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, os.ErrClosed
	}
	if s.pw == nil {
		s.open()
	}
	pw, done := s.pw, s.done
	s.mu.Unlock()
	n, err := pw.Write(b)
	if err != nil {
		// The stream ended; say why it did.
		select {
		case <-done:
			if s.err != nil {
				err = s.err
			}
		case <-s.p.ctx.Done():
		}
		if s.p.ctx.Err() != nil {
			err = s.p.gone()
		}
	}
	return n, err
}

// Close ends the stream and returns once the server has closed the
// process's input.
func (s *stdinPipe) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return os.ErrClosed
	}
	s.closed = true
	if s.pw == nil {
		s.open()
	}
	pw, done := s.pw, s.done
	s.mu.Unlock()
	pw.Close()
	select {
	case <-done:
		if s.err != nil && s.p.ctx.Err() != nil {
			return s.p.gone()
		}
		return s.err
	case <-s.p.ctx.Done():
		return s.p.gone()
	}
}
