package mcpworkspace

// The server half of the processes: the process methods answered for
// one workspace, on every MCP server they are registered on, each
// session's processes apart.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// outputBytes is how much of a process's output, on each of its pipes,
// the server holds that no read has taken yet: standard output waits
// for room past it, and standard error loses what does not fit.
const outputBytes = 64 << 10

// ErrNoProcess is a process id the session did not start, or closed.
var ErrNoProcess = errors.New("no such process in this session")

// Processes answers the process methods for one workspace, on every
// server Register is given, keeping each session's processes apart: an
// id another session started is ErrNoProcess. A session's processes end
// when it does (its Wait returns), on End, and on Close.
type Processes struct {
	ws   agentworkspace.Workspace // an agentworkspace.Starter
	next atomic.Uint64

	mu      sync.Mutex
	stopped bool
	tables  map[*mcp.ServerSession]*processTable
}

// NewProcesses answers the process methods, starting processes in ws,
// which should be an agentworkspace.Starter: on one that is not, every
// start fails with errors.ErrUnsupported, so a server whose workspace
// cannot start a process does better to register none. When ws is also
// an agentworkspace.ServerProvider, it answers the server methods too,
// each server it starts a process of the session's like any other.
func NewProcesses(ws agentworkspace.Workspace) *Processes {
	return &Processes{ws: ws, tables: map[*mcp.ServerSession]*processTable{}}
}

// Register has srv answer the process methods and, when the workspace
// offers servers (an agentworkspace.ServerProvider), the server
// methods.
func (s *Processes) Register(srv *mcp.Server) error {
	var servers error
	if _, ok := s.ws.(agentworkspace.ServerProvider); ok {
		servers = errors.Join(
			mcp.AddReceivingCustomMethod(srv, wire.MethodServersList, s.servers),
			mcp.AddReceivingCustomMethod(srv, wire.MethodServersStart, s.startServer),
		)
	}
	return errors.Join(
		servers,
		mcp.AddReceivingCustomMethod(srv, wire.MethodStart, s.start),
		mcp.AddReceivingCustomMethod(srv, wire.MethodWrite, s.write),
		mcp.AddReceivingCustomMethod(srv, wire.MethodRead, s.read),
		mcp.AddReceivingCustomMethod(srv, wire.MethodCloseStdin, s.closeStdin),
		mcp.AddReceivingCustomMethod(srv, wire.MethodSignal, s.signal),
		mcp.AddReceivingCustomMethod(srv, wire.MethodWait, s.wait),
		mcp.AddReceivingCustomMethod(srv, wire.MethodClose, s.close),
	)
}

// End ends ss's processes and the requests waiting on them, and refuses
// another start in ss. A session's close waits for its requests in
// flight, and a read or a wait on a process that runs on waits for it;
// over a transport where nothing cancels such a request when its client
// goes (streamable HTTP), the close would never come. A server calls End
// before it closes a session for that reason; ss's processes end with
// its own end otherwise.
func (s *Processes) End(ss *mcp.ServerSession) {
	s.mu.Lock()
	t := s.tables[ss]
	s.mu.Unlock()
	if t != nil {
		t.close()
	}
}

// Close ends every session's processes and the requests waiting on
// them, and refuses another start; it returns once they have ended.
func (s *Processes) Close() {
	s.mu.Lock()
	s.stopped = true
	tables := slices.Collect(maps.Values(s.tables))
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, t := range tables {
		wg.Go(t.close)
	}
	wg.Wait()
}

// processTable is one session's processes.
type processTable struct {
	ctx    context.Context // every process's; ended with the table
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	procs  map[string]*started
}

// started is a process the server started.
type started struct {
	p       agentworkspace.Process
	writeMu sync.Mutex // one write at a time, and the close of stdin after them
	stdout  *outputBuffer
	stderr  *outputBuffer // nil when the session did not ask for it
	gone    chan struct{} // closed when the process is closed, ending the polls
	goneOne sync.Once

	exited chan struct{} // closed once status and err are set
	status int
	err    error
}

// table is ss's table, made on its first start; a session's end closes
// it.
func (s *Processes) table(ss *mcp.ServerSession) (*processTable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, errors.New("the server is stopping")
	}
	if t, ok := s.tables[ss]; ok {
		return t, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &processTable{ctx: ctx, cancel: cancel, procs: map[string]*started{}}
	s.tables[ss] = t
	go func() {
		ss.Wait()
		s.mu.Lock()
		delete(s.tables, ss)
		s.mu.Unlock()
		t.close()
	}()
	return t, nil
}

// lookup is ss's process id.
func (s *Processes) lookup(ss *mcp.ServerSession, id string) (*started, error) {
	s.mu.Lock()
	t := s.tables[ss]
	s.mu.Unlock()
	if t != nil {
		t.mu.Lock()
		sp := t.procs[id]
		t.mu.Unlock()
		if sp != nil {
			return sp, nil
		}
	}
	return nil, fmt.Errorf("process %q: %w", id, ErrNoProcess)
}

// close ends every process in t and refuses another.
func (t *processTable) close() {
	t.mu.Lock()
	t.closed = true
	procs := slices.Collect(maps.Values(t.procs))
	clear(t.procs)
	t.mu.Unlock()
	var wg sync.WaitGroup
	for _, sp := range procs {
		wg.Go(sp.close)
	}
	wg.Wait()
	t.cancel()
}

// close ends the process and the polls waiting on it.
func (sp *started) close() {
	sp.goneOne.Do(func() { close(sp.gone) })
	sp.p.Close()
	<-sp.exited
}

func (s *Processes) start(_ context.Context, ss *mcp.ServerSession, p *StartParams) (*StartResult, error) {
	switch {
	case p == nil || len(p.Args) == 0 || p.Args[0] == "":
		return nil, errors.New("no command")
	case p.TimeoutMs < 0:
		return nil, errors.New("a negative timeout")
	}
	for _, e := range p.Env {
		if !strings.Contains(e, "=") {
			return nil, fmt.Errorf("environment entry %q is not NAME=value", e)
		}
	}
	cmd := agentworkspace.Command{Args: p.Args, Dir: p.Dir, Env: p.Env, Timeout: time.Duration(p.TimeoutMs) * time.Millisecond}
	return s.launch(ss, p.Stderr, func(ctx context.Context, stderr io.Writer) (agentworkspace.Process, error) {
		if stderr != nil {
			cmd.Stream = stderr
		}
		return agentworkspace.Start(ctx, s.ws, cmd)
	})
}

func (s *Processes) servers(ctx context.Context, _ *mcp.ServerSession, _ *ServersParams) (*ServersResult, error) {
	list, err := s.ws.(agentworkspace.ServerProvider).Servers(ctx)
	if err != nil {
		return nil, err
	}
	res := &ServersResult{ServerList: wire.ServerList{Servers: make([]wire.ServerInfo, 0, len(list))}}
	for _, sv := range list {
		res.Servers = append(res.Servers, wire.ServerInfo{Name: sv.Name, Description: sv.Description})
	}
	return res, nil
}

func (s *Processes) startServer(_ context.Context, ss *mcp.ServerSession, p *ServerStartParams) (*StartResult, error) {
	if p == nil || !agentworkspace.ValidServerName(p.Name) {
		return nil, errors.New("no server name, or one agentworkspace.ValidServerName refuses")
	}
	sp := s.ws.(agentworkspace.ServerProvider)
	res, err := s.launch(ss, p.Stderr, func(ctx context.Context, stderr io.Writer) (agentworkspace.Process, error) {
		return sp.StartServer(ctx, p.Name, stderr)
	})
	if errors.Is(err, agentworkspace.ErrNoServer) {
		return nil, &jsonrpc.Error{Code: wire.CodeNoServer, Message: err.Error()}
	}
	return res, err
}

// launch starts a process in ss's table with run, which is handed the
// table's context and the writer standard error goes to (nil when the
// session did not ask for it), and returns its id.
func (s *Processes) launch(ss *mcp.ServerSession, stderr bool, run func(context.Context, io.Writer) (agentworkspace.Process, error)) (*StartResult, error) {
	t, err := s.table(ss)
	if err != nil {
		return nil, err
	}
	sp := &started{stdout: newOutputBuffer(), gone: make(chan struct{}), exited: make(chan struct{})}
	var stream io.Writer
	if stderr {
		sp.stderr = newOutputBuffer()
		stream = sp.stderr
	}
	// The table is held across the start, which returns at once, so
	// the count and a session's end see every process.
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.closed:
		return nil, errors.New("the session is ending")
	case len(t.procs) >= wire.MaxProcesses:
		return nil, fmt.Errorf("%d processes are open in this session, the most there may be; close one first", wire.MaxProcesses)
	}
	proc, err := run(t.ctx, stream)
	if err != nil {
		return nil, err
	}
	sp.p = proc
	go sp.stdout.fill(proc.Stdout(), sp.gone)
	go func() {
		status, err := proc.Wait()
		sp.status, sp.err = status, err
		if sp.stderr != nil {
			// Wait returns once standard error is copied.
			sp.stderr.end()
		}
		close(sp.exited)
	}()
	id := fmt.Sprintf("p%d", s.next.Add(1))
	t.procs[id] = sp
	return &StartResult{Started: wire.Started{ID: id}}, nil
}

func (s *Processes) write(_ context.Context, ss *mcp.ServerSession, p *WriteParams) (*EmptyResult, error) {
	if p == nil {
		return nil, errors.New("no parameters")
	}
	if len(p.Data) > wire.MaxWriteBytes {
		return nil, fmt.Errorf("a write of %d bytes; at most %d", len(p.Data), wire.MaxWriteBytes)
	}
	sp, err := s.lookup(ss, p.ID)
	if err != nil {
		return nil, err
	}
	// A write waits while the process does not read, as on a pipe; the
	// process's close ends it.
	sp.writeMu.Lock()
	defer sp.writeMu.Unlock()
	if _, err := sp.p.Stdin().Write(p.Data); err != nil {
		return nil, err
	}
	return &EmptyResult{}, nil
}

func (s *Processes) read(ctx context.Context, ss *mcp.ServerSession, p *ReadParams) (*ReadResult, error) {
	if p == nil {
		return nil, errors.New("no parameters")
	}
	sp, err := s.lookup(ss, p.ID)
	if err != nil {
		return nil, err
	}
	var b *outputBuffer
	switch p.FD {
	case 1:
		b = sp.stdout
	case 2:
		if b = sp.stderr; b == nil {
			return nil, errors.New("standard error was not asked for at the start")
		}
	default:
		return nil, fmt.Errorf("fd %d: want 1 or 2", p.FD)
	}
	n := p.Max
	if n <= 0 || n > wire.MaxReadBytes {
		n = wire.MaxReadBytes
	}
	data, eof, err := b.take(ctx, n, sp.gone)
	if err != nil {
		return nil, err
	}
	return &ReadResult{ReadReply: wire.ReadReply{Data: data, EOF: eof}}, nil
}

func (s *Processes) closeStdin(_ context.Context, ss *mcp.ServerSession, p *IDParams) (*EmptyResult, error) {
	if p == nil {
		return nil, errors.New("no parameters")
	}
	sp, err := s.lookup(ss, p.ID)
	if err != nil {
		return nil, err
	}
	sp.writeMu.Lock()
	defer sp.writeMu.Unlock()
	if err := sp.p.Stdin().Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return nil, err
	}
	return &EmptyResult{}, nil
}

func (s *Processes) signal(_ context.Context, ss *mcp.ServerSession, p *SignalParams) (*SignalResult, error) {
	if p == nil {
		return nil, errors.New("no parameters")
	}
	sig, ok := wire.SignalOf(p.Signal)
	if !ok {
		return nil, fmt.Errorf("signal %q: want INT, TERM or KILL", p.Signal)
	}
	sp, err := s.lookup(ss, p.ID)
	if err != nil {
		return nil, err
	}
	if err := sp.p.Signal(sig); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return &SignalResult{Signaled: wire.Signaled{Done: true}}, nil
		}
		return nil, err
	}
	return &SignalResult{}, nil
}

func (s *Processes) wait(ctx context.Context, ss *mcp.ServerSession, p *IDParams) (*ExitResult, error) {
	if p == nil {
		return nil, errors.New("no parameters")
	}
	sp, err := s.lookup(ss, p.ID)
	if err != nil {
		return nil, err
	}
	select {
	case <-sp.exited:
		return sp.result(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Processes) close(_ context.Context, ss *mcp.ServerSession, p *IDParams) (*ExitResult, error) {
	if p == nil {
		return nil, errors.New("no parameters")
	}
	s.mu.Lock()
	t := s.tables[ss]
	s.mu.Unlock()
	var sp *started
	if t != nil {
		t.mu.Lock()
		sp = t.procs[p.ID]
		delete(t.procs, p.ID)
		t.mu.Unlock()
	}
	if sp == nil {
		return nil, fmt.Errorf("process %q: %w", p.ID, ErrNoProcess)
	}
	sp.close()
	r := sp.result()
	if sp.stderr != nil {
		r.Stderr = sp.stderr.rest()
	}
	return r, nil
}

// result is how the process ended, once it has.
func (sp *started) result() *ExitResult {
	r := &ExitResult{Exit: wire.Exit{Status: sp.status}}
	switch {
	case errors.Is(sp.err, context.DeadlineExceeded):
		r.Ended = wire.EndedTimeout
	case errors.Is(sp.err, context.Canceled):
		r.Ended = wire.EndedCanceled
	case sp.err != nil:
		r.Error = sp.err.Error()
	}
	return r
}

// outputBuffer is what a process wrote to one of its pipes that the
// session has not read yet, at most outputBytes: standard output fills
// it and waits for room (fill), standard error writes to it and loses
// what does not fit (Write).
type outputBuffer struct {
	mu      sync.Mutex
	buf     []byte
	eof     bool
	changed chan struct{} // closed and replaced at every change
}

func newOutputBuffer() *outputBuffer {
	return &outputBuffer{changed: make(chan struct{})}
}

// notify wakes whoever waits on a change; b.mu is held.
func (b *outputBuffer) notify() {
	close(b.changed)
	b.changed = make(chan struct{})
}

// fill copies r into b until r ends or gone closes, waiting for room.
func (b *outputBuffer) fill(r io.Reader, gone <-chan struct{}) {
	defer b.end()
	tmp := make([]byte, 32<<10)
	for {
		b.mu.Lock()
		for len(b.buf) >= outputBytes {
			ch := b.changed
			b.mu.Unlock()
			select {
			case <-ch:
			case <-gone:
				return
			}
			b.mu.Lock()
		}
		room := outputBytes - len(b.buf)
		b.mu.Unlock()
		n, err := r.Read(tmp[:min(room, len(tmp))])
		if n > 0 {
			b.mu.Lock()
			b.buf = append(b.buf, tmp[:n]...)
			b.notify()
			b.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

// Write is standard error's: what does not fit is dropped.
func (b *outputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := outputBytes - len(b.buf); room > 0 {
		b.buf = append(b.buf, p[:min(room, len(p))]...)
		b.notify()
	}
	return len(p), nil
}

// end is end of file after what b holds.
func (b *outputBuffer) end() {
	b.mu.Lock()
	b.eof = true
	b.notify()
	b.mu.Unlock()
}

// rest takes what b holds.
func (b *outputBuffer) rest() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.buf
	b.buf = nil
	b.notify()
	return out
}

// take waits for output or its end, and returns at most n bytes of it,
// and whether that is all there will be.
func (b *outputBuffer) take(ctx context.Context, n int, gone <-chan struct{}) ([]byte, bool, error) {
	for {
		b.mu.Lock()
		if len(b.buf) > 0 || b.eof {
			k := min(n, len(b.buf))
			out := slices.Clone(b.buf[:k])
			b.buf = append(b.buf[:0], b.buf[k:]...)
			eof := b.eof && len(b.buf) == 0
			b.notify()
			b.mu.Unlock()
			return out, eof, nil
		}
		ch := b.changed
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-gone:
			return nil, false, errors.New("the process was closed")
		}
	}
}
