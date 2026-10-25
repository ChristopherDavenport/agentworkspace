package mcpworkspace

// The client half of the processes: a process started over a
// connection, as an agentworkspace.Process.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// requestTimeout bounds a process request that should be answered at
// once (a start, a signal, the close of input, a close). A var for the
// tests.
var requestTimeout = 30 * time.Second

// A Conn is the connection Start starts a process over, and what the
// program knows of it.
type Conn struct {
	// Session is the client session the process's requests go on; its
	// client has the methods (RegisterClient).
	Session *mcp.ClientSession
	// Bounds are what the server keeps, as it said them; a write and a
	// read ask for no more than these, nor than wire's constants. Every
	// bound must be positive.
	Bounds wire.Bounds
	// Done, when not nil, ends when the connection is lost or closed:
	// the process's requests in flight end with it. A program ends it
	// before it closes the session, since the SDK's close waits for the
	// requests in flight, and a process's wait, or a read waiting for
	// output, is one until the process ends. Nil is a connection whose
	// end the program does not watch: the requests then end with the
	// processes, or when the session is lost.
	Done context.Context
	// Lost is the cause Done ends with when the connection was lost, as
	// against closed: a read or a wait that ends under it is reported
	// through Err, as the loss it is, and one that ends under any other
	// cause as the process's close.
	Lost error
	// Err, when not nil, is what a failed request's error becomes for
	// the process's caller, such as a program's word for a lost
	// connection; nil leaves it as it is.
	Err func(error) error
}

// Start starts cmd in the workspace at the other end of c and returns at
// once: agentworkspace.Starter's Start, over the connection. The
// process's standard input and output are requests, a write at a time
// and a read at a time; its standard error, when cmd.Stream is set, is
// read in the background and written to cmd.Stream as it arrives. It
// ends with Close, the end of ctx, cmd.Timeout, the end of c.Done or the
// server's. It lives on c alone: when the connection ends, the server
// ends the process with its session, and the process's requests fail.
// cmd.Stdin is refused; write to the process's input instead.
func Start(ctx context.Context, c Conn, cmd agentworkspace.Command) (agentworkspace.Process, error) {
	switch {
	case len(cmd.Args) == 0:
		return nil, errors.New("mcpworkspace: no command")
	case cmd.Stdin != nil:
		return nil, errors.New("mcpworkspace: Start takes no Command.Stdin; write to Process.Stdin")
	case c.Session == nil:
		return nil, errors.New("mcpworkspace: a Conn with no session")
	case !c.Bounds.Valid():
		return nil, fmt.Errorf("mcpworkspace: a bound that is not positive: %+v", c.Bounds)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.Done == nil {
		c.Done = context.Background()
	}
	if c.Err == nil {
		c.Err = func(err error) error { return err }
	}
	sctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	res, err := mcp.CallCustomMethod[*StartParams, *StartResult](sctx, c.Session, wire.MethodStart, &StartParams{StartArgs: wire.StartArgs{
		Args: cmd.Args, Dir: cmd.Dir, Env: cmd.Env, TimeoutMs: cmd.Timeout.Milliseconds(), Stderr: cmd.Stream != nil,
	}})
	if err != nil {
		return nil, fmt.Errorf("start %s: %w", cmd.Args[0], c.Err(err))
	}
	p := &process{
		c: c, id: res.ID,
		maxWrite: min(wire.MaxWriteBytes, c.Bounds.MaxWriteBytes), maxRead: min(wire.MaxReadBytes, c.Bounds.MaxReadBytes),
		waited: make(chan struct{}), pumped: make(chan struct{}),
	}
	p.ctx, p.cancel = context.WithCancel(c.Done)
	p.in.p, p.out.p = p, p
	go p.waitFor()
	if p.stream = cmd.Stream; p.stream != nil {
		go p.pump()
	} else {
		close(p.pumped)
	}
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		select {
		case <-p.waited:
		default:
			p.ending = ctx.Err()
		}
		p.mu.Unlock()
		p.Close()
	})
	p.mu.Lock()
	p.stopWatch = stop
	p.mu.Unlock()
	return p, nil
}

// ProcessID is the id the server gave p, a process Start started, and
// false for any other process.
func ProcessID(p agentworkspace.Process) (string, bool) {
	q, ok := p.(*process)
	if !ok {
		return "", false
	}
	return q.id, true
}

// process is a process the server started over one connection: it ends
// with that connection's session, and its requests fail once the
// connection is lost, never sent on another.
type process struct {
	c                 Conn
	id                string
	maxWrite, maxRead int

	ctx    context.Context // the polls'; ended by Close and the connection's
	cancel context.CancelFunc

	in  stdin
	out stdout

	stream io.Writer     // the start's Command.Stream
	pumped chan struct{} // closed once standard error's pump is done

	mu        sync.Mutex
	stopWatch func() bool // stops the watch on the start's context
	ending    error       // the start context's error, once it ended the process
	waited    chan struct{}
	waitOne   sync.Once
	status    int
	err       error

	closeOne sync.Once
	closeErr error
}

var _ agentworkspace.Process = (*process)(nil)

func (p *process) Stdin() io.WriteCloser { return &p.in }
func (p *process) Stdout() io.ReadCloser { return &p.out }

// lost reports whether p's requests ended because the connection was
// lost, rather than closed or the process closed.
func (p *process) lost() bool {
	return p.c.Lost != nil && errors.Is(context.Cause(p.ctx), p.c.Lost)
}

// finish records how the process ended, the first time.
func (p *process) finish(r *ExitResult, err error) {
	p.waitOne.Do(func() {
		switch {
		case err != nil:
			p.status, p.err = -1, err
		default:
			p.status = r.Status
			switch r.Ended {
			case wire.EndedTimeout:
				p.err = context.DeadlineExceeded
			case wire.EndedCanceled:
				p.err = context.Canceled
			case "":
				if r.Error != "" {
					p.err = errors.New(r.Error)
				}
			}
		}
		close(p.waited)
	})
}

// waitFor waits in the server for the process to exit.
func (p *process) waitFor() {
	r, err := mcp.CallCustomMethod[*IDParams, *ExitResult](p.ctx, p.c.Session, wire.MethodWait, &IDParams{ProcessID: wire.ProcessID{ID: p.id}})
	if err != nil && p.ctx.Err() != nil && !p.lost() {
		// Close ended the wait; its reply says how the process ended.
		return
	}
	if err != nil {
		// The connection is lost, and the process with it.
		err = p.c.Err(err)
	}
	p.finish(r, err)
}

// pump writes standard error to the stream as it arrives.
func (p *process) pump() {
	defer close(p.pumped)
	for {
		r, err := mcp.CallCustomMethod[*ReadParams, *ReadResult](p.ctx, p.c.Session, wire.MethodRead, &ReadParams{ReadArgs: wire.ReadArgs{ID: p.id, FD: 2, Max: p.maxRead}})
		if err != nil {
			return
		}
		if len(r.Data) > 0 {
			p.stream.Write(r.Data)
		}
		if r.EOF {
			return
		}
	}
}

func (p *process) Wait() (int, error) {
	<-p.waited
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ending != nil {
		return p.status, p.ending
	}
	return p.status, p.err
}

// Signal sends INT, TERM or KILL; any other signal is refused. A process
// that has exited is os.ErrProcessDone.
func (p *process) Signal(sig os.Signal) error {
	s, ok := sig.(syscall.Signal)
	var name string
	if ok {
		name, ok = wire.SignalName(s)
	}
	if !ok {
		return fmt.Errorf("mcpworkspace: cannot send %v", sig)
	}
	ctx, cancel := context.WithTimeout(p.ctx, requestTimeout)
	defer cancel()
	r, err := mcp.CallCustomMethod[*SignalParams, *SignalResult](ctx, p.c.Session, wire.MethodSignal, &SignalParams{SignalArgs: wire.SignalArgs{ID: p.id, Signal: name}})
	if err != nil {
		return p.c.Err(err)
	}
	if r.Done {
		return os.ErrProcessDone
	}
	return nil
}

// Close ends the process in the server, which closes its input, gives
// it a moment, then terminates and kills it, and returns once it has
// exited there and its standard error is written to the stream; then
// the reads and the wait still in flight end.
func (p *process) Close() error {
	p.closeOne.Do(func() {
		p.mu.Lock()
		if p.stopWatch != nil {
			p.stopWatch()
		}
		p.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(p.ctx), requestTimeout)
		r, err := mcp.CallCustomMethod[*IDParams, *ExitResult](ctx, p.c.Session, wire.MethodClose, &IDParams{ProcessID: wire.ProcessID{ID: p.id}})
		cancel()
		if err != nil {
			p.closeErr = p.c.Err(err)
			p.cancel()
		}
		p.finish(r, err)
		// The pump ends once the server has closed the process; what
		// it had not read yet came with the reply, after what it read.
		<-p.pumped
		if err == nil && len(r.Stderr) > 0 && p.stream != nil {
			p.stream.Write(r.Stderr)
		}
		p.cancel()
	})
	return p.closeErr
}

// stdin is the process's standard input: each Write is requests of at
// most maxWrite bytes, in order.
type stdin struct {
	p      *process
	mu     sync.Mutex
	closed bool
}

func (w *stdin) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	n := 0
	for len(b) > 0 {
		k := min(len(b), w.p.maxWrite)
		if _, err := mcp.CallCustomMethod[*WriteParams, *EmptyResult](w.p.ctx, w.p.c.Session, wire.MethodWrite, &WriteParams{WriteArgs: wire.WriteArgs{ID: w.p.id, Data: b[:k]}}); err != nil {
			return n, w.p.c.Err(err)
		}
		n += k
		b = b[k:]
	}
	return n, nil
}

// Close is end of file for the process.
func (w *stdin) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	ctx, cancel := context.WithTimeout(w.p.ctx, requestTimeout)
	defer cancel()
	if _, err := mcp.CallCustomMethod[*IDParams, *EmptyResult](ctx, w.p.c.Session, wire.MethodCloseStdin, &IDParams{ProcessID: wire.ProcessID{ID: w.p.id}}); err != nil {
		return w.p.c.Err(err)
	}
	return nil
}

// stdout is the process's standard output: each Read is a request that
// waits for output.
type stdout struct {
	p      *process
	mu     sync.Mutex // one read at a time
	eof    bool
	closed atomic.Bool
}

func (r *stdout) Read(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		switch {
		case r.closed.Load():
			return 0, os.ErrClosed
		case r.eof:
			return 0, io.EOF
		case len(b) == 0:
			return 0, nil
		}
		res, err := mcp.CallCustomMethod[*ReadParams, *ReadResult](r.p.ctx, r.p.c.Session, wire.MethodRead, &ReadParams{ReadArgs: wire.ReadArgs{ID: r.p.id, FD: 1, Max: min(len(b), r.p.maxRead)}})
		if err != nil {
			if r.p.ctx.Err() != nil && !r.p.lost() {
				return 0, os.ErrClosed
			}
			return 0, r.p.c.Err(err)
		}
		if len(res.Data) > len(b) {
			return 0, fmt.Errorf("mcpworkspace: a read of %d bytes gave %d", len(b), len(res.Data))
		}
		r.eof = res.EOF
		if n := copy(b, res.Data); n > 0 {
			return n, nil
		}
	}
}

// Close stops later reads; one in flight ends with the process.
func (r *stdout) Close() error {
	r.closed.Store(true)
	return nil
}
