package remote

// The server half of the sessions and their processes.

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// session is one client's: the processes it started, which live no
// longer than it does. It lapses when it goes a lease unrenewed, and
// ends when it lapses, when its client closes it, or when the handler
// does.
type session struct {
	id    string
	owner string // the principal's name
	ctx   context.Context
	stop  context.CancelFunc
	timer *time.Timer

	// Under the Handler's mu.
	deadline time.Time
	gone     bool // out of the table

	mu     sync.Mutex
	ended  bool
	procs  map[string]*proc
	closed keyRing // the keys of the processes closed, the latest

	endOnce sync.Once
}

// end closes every process in s and refuses another; it returns once
// they have ended, as does every call made while the first runs.
func (s *session) end() {
	s.endOnce.Do(func() {
		s.mu.Lock()
		s.ended = true
		procs := slices.Collect(maps.Values(s.procs))
		clear(s.procs)
		s.mu.Unlock()
		var wg sync.WaitGroup
		for _, sp := range procs {
			wg.Go(sp.close)
		}
		wg.Wait()
		s.stop()
	})
}

// proc is a process a session started.
type proc struct {
	// scope is the scope it was started under, which its routes take:
	// exec for a command's start, servers for a server's.
	scope Scope
	// server is the server it is, "" for a command.
	server  string
	p       agentworkspace.Process
	cancel  context.CancelFunc // ends it as its context's end does
	stderr  *stderrBuffer      // nil when the start did not ask for it
	streams [3]atomic.Bool     // stdin, stdout and stderr taken by a stream

	exited chan struct{} // closed once status and err are set
	status int
	err    error
}

// close ends the process, and everything it started, and releases it.
func (sp *proc) close() {
	sp.p.Close()
	<-sp.exited
	sp.cancel()
}

// exit is how the process ended, once it has, its error's words as
// errorOf gives them for root.
func (sp *proc) exit(root string) exitReply {
	r := exitReply{Status: sp.status}
	switch {
	case errors.Is(sp.err, context.DeadlineExceeded):
		r.Ended = endedTimeout
	case errors.Is(sp.err, context.Canceled):
		r.Ended = endedCanceled
	case sp.err != nil:
		r.Error = errorOf(root, sp.err).Message
	}
	return r
}

// keyRing is the latest keys of a session's closed processes, so a
// start repeated after its process closed starts nothing: at most
// closedKeys of them.
type keyRing struct {
	set   map[string]bool
	order []string
}

const closedKeys = 1024

func (k *keyRing) add(key string) {
	if k.set == nil {
		k.set = map[string]bool{}
	}
	if len(k.order) == closedKeys {
		delete(k.set, k.order[0])
		k.order = k.order[1:]
	}
	k.set[key] = true
	k.order = append(k.order, key)
}

// openSession opens a session for a principal that can start anything
// in it: one holding exec, of a Starter, or servers, of a
// ServerProvider.
func (h *Handler) openSession(w http.ResponseWriter, r *http.Request) {
	p := principalOf(r)
	_, starter := h.ws.(agentworkspace.Starter)
	_, provider := h.ws.(agentworkspace.ServerProvider)
	if !(starter && p.Has(ScopeExec)) && !(provider && p.Has(ScopeServers)) {
		h.fail(w, fmt.Errorf("a %s workspace starts nothing this principal may start: %w", h.ws.Descriptor().Kind, errors.ErrUnsupported))
		return
	}
	var req struct{}
	if !decode(w, r, MaxRequestBytes, &req) {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	s := &session{id: rand.Text(), owner: principalOf(r).Name, ctx: ctx, stop: stop, procs: map[string]*proc{}}
	h.mu.Lock()
	owned := 0
	for _, o := range h.sessions {
		if o.owner == s.owner {
			owned++
		}
	}
	switch {
	case h.closed:
		h.mu.Unlock()
		stop()
		writeError(w, wireError{Code: codeClosed, Message: "the server is closed"})
		return
	case len(h.sessions) >= h.maxSessions:
		h.mu.Unlock()
		stop()
		writeError(w, wireError{Code: codeLimit, Message: fmt.Sprintf("%d sessions are open, the most there may be", h.maxSessions)})
		return
	case owned >= h.maxPerOwner:
		h.mu.Unlock()
		stop()
		writeError(w, wireError{Code: codeLimit, Message: fmt.Sprintf("the principal has %d sessions open, the most one may have", h.maxPerOwner)})
		return
	}
	s.deadline = time.Now().Add(h.lease)
	s.timer = time.AfterFunc(h.lease, func() { h.lapse(s) })
	h.sessions[s.id] = s
	h.mu.Unlock()
	writeJSON(w, sessionReply{ID: s.id, LeaseMs: h.lease.Milliseconds()})
}

// lapse ends s if it went a lease unrenewed, and waits again for the
// rest of the lease if it was renewed.
func (h *Handler) lapse(s *session) {
	h.mu.Lock()
	if s.gone {
		h.mu.Unlock()
		return
	}
	if d := time.Until(s.deadline); d > 0 {
		s.timer.Reset(d)
		h.mu.Unlock()
		return
	}
	s.gone = true
	delete(h.sessions, s.id)
	// Out of the table, so Close does not find it: it waits for the end
	// here instead. Close has not begun, or it would have taken s.
	h.inflight.Add(1)
	h.mu.Unlock()
	defer h.inflight.Done()
	s.end()
}

// sessionOf is r's session, answering ErrNoSession itself for one that
// is not open or is another principal's: the two read the same.
func (h *Handler) sessionOf(w http.ResponseWriter, r *http.Request) *session {
	h.mu.Lock()
	s := h.sessions[r.PathValue("sid")]
	h.mu.Unlock()
	if s == nil || s.owner != principalOf(r).Name {
		writeError(w, wireError{Code: codeNoSession, Message: ErrNoSession.Error()})
		return nil
	}
	return s
}

func (h *Handler) renewSession(w http.ResponseWriter, r *http.Request) {
	s := h.sessionOf(w, r)
	if s == nil {
		return
	}
	h.mu.Lock()
	gone := s.gone
	s.deadline = time.Now().Add(h.lease)
	h.mu.Unlock()
	if gone {
		writeError(w, wireError{Code: codeNoSession, Message: ErrNoSession.Error()})
		return
	}
	writeJSON(w, sessionReply{LeaseMs: h.lease.Milliseconds()})
}

// closeSession ends the session and every process in it, whichever
// scope started each: a session is its principal's, by Name, not a
// scope's, so either scope closes it.
func (h *Handler) closeSession(w http.ResponseWriter, r *http.Request) {
	s := h.sessionOf(w, r)
	if s == nil {
		return
	}
	h.mu.Lock()
	if !s.gone {
		s.gone = true
		s.timer.Stop()
		delete(h.sessions, s.id)
		// As in lapse: Close waits for an end it does not find.
		h.inflight.Add(1)
		defer h.inflight.Done()
	}
	h.mu.Unlock()
	s.end()
	w.WriteHeader(http.StatusNoContent)
}

// procOf is r's process in r's session, answering the refusal itself
// for one the session did not start, or one started under a scope the
// principal does not hold.
func (h *Handler) procOf(w http.ResponseWriter, r *http.Request) *proc {
	s := h.sessionOf(w, r)
	if s == nil {
		return nil
	}
	s.mu.Lock()
	sp := s.procs[r.PathValue("pid")]
	s.mu.Unlock()
	switch {
	case sp == nil:
		writeError(w, wireError{Code: codeNoProcess, Message: ErrNoProcess.Error()})
		return nil
	case !mayUse(w, r, sp):
		return nil
	}
	return sp
}

// mayUse reports whether r's principal holds the scope sp was started
// under, answering the refusal itself when it does not: a process a
// command's start began is exec's, and a server is servers'.
func mayUse(w http.ResponseWriter, r *http.Request, sp *proc) bool {
	if principalOf(r).Has(sp.scope) {
		return true
	}
	writeError(w, wireError{Code: codeForbidden, Message: fmt.Sprintf("remote: the process was started under the %s scope, which the principal lacks", sp.scope)})
	return false
}

// start starts a command in the session, under a context of its own
// that the session's end, and a cancel, end.
func (h *Handler) start(w http.ResponseWriter, r *http.Request) {
	s := h.sessionOf(w, r)
	if s == nil {
		return
	}
	var req startRequest
	if !decode(w, r, MaxRequestBytes, &req) || !validTimeout(w, req.TimeoutMs) {
		return
	}
	cmd := agentworkspace.Command{Args: req.Args, Dir: req.Dir, Env: req.Env, Timeout: time.Duration(req.TimeoutMs) * time.Millisecond}
	h.launch(w, s, req.Key, req.Stderr, &proc{scope: ScopeExec}, func(ctx context.Context, stderr io.Writer) (agentworkspace.Process, error) {
		if stderr != nil {
			cmd.Stream = stderr
		}
		return agentworkspace.Start(ctx, h.ws, cmd)
	})
}

// startServer starts the server the route names in the session, as
// start starts a command: the served workspace's StartServer starts
// it, so an Offering's OutputFilter reads its output here, before any
// of it is sent.
func (h *Handler) startServer(w http.ResponseWriter, r *http.Request) {
	s := h.sessionOf(w, r)
	if s == nil {
		return
	}
	var req serverStartRequest
	if !decode(w, r, MaxRequestBytes, &req) {
		return
	}
	provider, ok := h.ws.(agentworkspace.ServerProvider)
	if !ok {
		h.fail(w, fmt.Errorf("a %s workspace offers no servers: %w", h.ws.Descriptor().Kind, errors.ErrUnsupported))
		return
	}
	name := r.PathValue("name")
	if !agentworkspace.ValidServerName(name) {
		writeError(w, wireError{Code: codeNoServer, Message: agentworkspace.ErrNoServer.Error()})
		return
	}
	h.launch(w, s, req.Key, req.Stderr, &proc{scope: ScopeServers, server: name}, func(ctx context.Context, stderr io.Writer) (agentworkspace.Process, error) {
		return provider.StartServer(ctx, name, stderr)
	})
}

// launch starts sp in s with run, which is given the process's context
// and where its standard error goes (nil when the start did not ask for
// it), and answers its id: key, or one of the server's choosing. A key
// already running is the same start again, answered with the same
// process when it is a start of the same kind, exec's of a command or
// servers' of the same server.
func (h *Handler) launch(w http.ResponseWriter, s *session, key string, stderr bool, sp *proc, run func(context.Context, io.Writer) (agentworkspace.Process, error)) {
	if key != "" && !validKey(key) {
		writeError(w, wireError{Code: codeInvalid, Message: "a start key is 16 to 64 letters and digits"})
		return
	}
	sp.exited = make(chan struct{})
	var stream io.Writer
	if stderr {
		sp.stderr = newStderrBuffer()
		stream = sp.stderr
	}
	// The session is held across the start, which returns at once, so
	// the count and the session's end see every process.
	s.mu.Lock()
	defer s.mu.Unlock()
	switch prior := s.procs[key]; {
	case s.ended:
		writeError(w, wireError{Code: codeNoSession, Message: ErrNoSession.Error()})
		return
	case prior != nil && (prior.scope != sp.scope || prior.server != sp.server):
		writeError(w, wireError{Code: codeInvalid, Message: "the key names a start of something else"})
		return
	case prior != nil:
		// The same start again: its reply was lost.
		writeJSON(w, startedReply{ID: key})
		return
	case s.closed.set[key]:
		writeError(w, wireError{Code: codeNoProcess, Message: "the process this key started is closed"})
		return
	case len(s.procs) >= h.maxProcesses:
		writeError(w, wireError{Code: codeLimit, Message: fmt.Sprintf("%d processes are open in this session, the most there may be; close one first", h.maxProcesses)})
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	p, err := run(ctx, stream)
	if err != nil {
		cancel()
		h.fail(w, err)
		return
	}
	sp.p, sp.cancel = p, cancel
	go func() {
		status, err := p.Wait()
		sp.status, sp.err = status, err
		if sp.stderr != nil {
			// Wait returns once standard error is copied.
			sp.stderr.end()
		}
		close(sp.exited)
	}()
	id := key
	if id == "" {
		id = rand.Text()
	}
	s.procs[id] = sp
	writeJSON(w, startedReply{ID: id})
}

// servers lists the servers the served workspace offers, in its order.
func (h *Handler) servers(w http.ResponseWriter, r *http.Request) {
	provider, ok := h.ws.(agentworkspace.ServerProvider)
	if !ok {
		h.fail(w, fmt.Errorf("a %s workspace offers no servers: %w", h.ws.Descriptor().Kind, errors.ErrUnsupported))
		return
	}
	list, err := provider.Servers(r.Context())
	if err != nil {
		h.fail(w, err)
		return
	}
	reply := serverList{Servers: make([]serverInfo, 0, len(list))}
	for _, sv := range list {
		reply.Servers = append(reply.Servers, serverInfo{Name: sv.Name, Description: sv.Description})
	}
	writeJSON(w, reply)
}

// take claims one of a process's streams for the request, refusing (and
// answering) a second.
func take(w http.ResponseWriter, sp *proc, fd int) bool {
	if sp.streams[fd].CompareAndSwap(false, true) {
		return true
	}
	writeError(w, wireError{Code: codeInvalid, Message: fmt.Sprintf("%s is already streamed", [3]string{"standard input", "standard output", "standard error"}[fd])})
	return false
}

// stdin copies the request's body to the process's input as it arrives
// and closes the input at its end: the body's end is the process's end
// of file, and so is a body cut short.
func (h *Handler) stdin(w http.ResponseWriter, r *http.Request) {
	sp := h.procOf(w, r)
	if sp == nil || !take(w, sp, 0) {
		return
	}
	in := sp.p.Stdin()
	_, err := io.Copy(in, r.Body)
	if cerr := in.Close(); err == nil && cerr != nil && !errors.Is(cerr, os.ErrClosed) {
		err = cerr
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// stdout streams the process's output as it arrives, until it ends. A
// client that goes closes it, as a reader closing a pipe does.
func (h *Handler) stdout(w http.ResponseWriter, r *http.Request) {
	sp := h.procOf(w, r)
	if sp == nil || !take(w, sp, 1) {
		return
	}
	out := sp.p.Stdout()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	rc.Flush()
	buf := make([]byte, 32<<10)
	for {
		n, err := out.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil || rc.Flush() != nil {
				out.Close()
				return
			}
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}

// stderr streams what the process wrote to its standard error, which
// the server holds (StderrBytes of it) until it is read.
func (h *Handler) stderr(w http.ResponseWriter, r *http.Request) {
	sp := h.procOf(w, r)
	if sp == nil {
		return
	}
	if sp.stderr == nil {
		writeError(w, wireError{Code: codeInvalid, Message: "standard error was not asked for at the start"})
		return
	}
	if !take(w, sp, 2) {
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	rc.Flush()
	for {
		data, eof, err := sp.stderr.take(r.Context(), 32<<10)
		if err != nil {
			return
		}
		if len(data) > 0 {
			if _, err := w.Write(data); err != nil || rc.Flush() != nil {
				return
			}
		}
		if eof {
			return
		}
	}
}

// wait waits for the process to exit, as long as the request does.
func (h *Handler) wait(w http.ResponseWriter, r *http.Request) {
	sp := h.procOf(w, r)
	if sp == nil {
		return
	}
	select {
	case <-sp.exited:
		writeJSON(w, sp.exit(h.ws.Root()))
	case <-r.Context().Done():
	}
}

func (h *Handler) signal(w http.ResponseWriter, r *http.Request) {
	sp := h.procOf(w, r)
	if sp == nil {
		return
	}
	var req signalRequest
	if !decode(w, r, MaxRequestBytes, &req) {
		return
	}
	sig, ok := signals[req.Signal]
	if !ok {
		writeError(w, wireError{Code: codeInvalid, Message: fmt.Sprintf("no signal named %q", bounded(req.Signal))})
		return
	}
	if err := sp.p.Signal(sig); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// cancel ends the process as the end of its context does, and returns
// at once; a wait says when it has ended.
func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	sp := h.procOf(w, r)
	if sp == nil {
		return
	}
	var req struct{}
	if !decode(w, r, MaxRequestBytes, &req) {
		return
	}
	sp.cancel()
	w.WriteHeader(http.StatusNoContent)
}

// closeProcess ends the process, as Process.Close does, and answers
// how it ended. The handler's Close waits for it, as it is out of the
// session's table while it ends.
func (h *Handler) closeProcess(w http.ResponseWriter, r *http.Request) {
	s := h.sessionOf(w, r)
	if s == nil {
		return
	}
	done, ok := h.track()
	if !ok {
		writeError(w, wireError{Code: codeClosed, Message: "the server is closed"})
		return
	}
	defer done()
	pid := r.PathValue("pid")
	s.mu.Lock()
	sp := s.procs[pid]
	if sp != nil && principalOf(r).Has(sp.scope) {
		delete(s.procs, pid)
		s.closed.add(pid)
	}
	s.mu.Unlock()
	switch {
	case sp == nil:
		writeError(w, wireError{Code: codeNoProcess, Message: ErrNoProcess.Error()})
		return
	case !mayUse(w, r, sp):
		return
	}
	sp.close()
	writeJSON(w, sp.exit(h.ws.Root()))
}

// stderrBuffer is what a process wrote to its standard error that the
// client has not read yet, at most StderrBytes: what does not fit is
// dropped, so the process's Command.Stream never waits.
type stderrBuffer struct {
	mu      sync.Mutex
	buf     []byte
	eof     bool
	changed chan struct{} // closed and replaced at every change
}

func newStderrBuffer() *stderrBuffer { return &stderrBuffer{changed: make(chan struct{})} }

// notify wakes whoever waits on a change; b.mu is held.
func (b *stderrBuffer) notify() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *stderrBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := StderrBytes - len(b.buf); room > 0 && len(p) > 0 {
		b.buf = append(b.buf, p[:min(room, len(p))]...)
		b.notify()
	}
	return len(p), nil
}

// end is end of file after what b holds.
func (b *stderrBuffer) end() {
	b.mu.Lock()
	b.eof = true
	b.notify()
	b.mu.Unlock()
}

// take waits for output or its end, and returns at most n bytes of it
// and whether that is all there will be.
func (b *stderrBuffer) take(ctx context.Context, n int) ([]byte, bool, error) {
	for {
		b.mu.Lock()
		if len(b.buf) > 0 || b.eof {
			k := min(n, len(b.buf))
			out := slices.Clone(b.buf[:k])
			b.buf = append(b.buf[:0], b.buf[k:]...)
			eof := b.eof && len(b.buf) == 0
			b.mu.Unlock()
			return out, eof, nil
		}
		ch := b.changed
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}
