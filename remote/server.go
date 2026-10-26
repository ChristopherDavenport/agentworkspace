package remote

// The server half: a workspace served over HTTP.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ChristopherDavenport/agentworkspace"
)

// Handler serves one workspace over HTTP, every route under /v1 of the
// URL it is mounted at (behind http.StripPrefix when that has a path).
// It is a plain http.Handler: TLS, and the timeouts of the server it is
// mounted on, are the program's, and a server with a ReadTimeout or a
// WriteTimeout cuts the streams (Exec's frames, a process's pipes, a
// wait) short.
//
// Every request is authenticated first, whatever its route, and
// refused without an authenticator unless the handler was made with
// WithInsecureNoAuth: it fails closed. Closing the handler ends every
// session's processes; the served workspace is the program's to close.
type Handler struct {
	ws            agentworkspace.Workspace
	auth          Authenticator
	insecure      bool
	lease         time.Duration
	maxSessions   int
	maxProcesses  int
	maxWrite      int64
	maxDirEntries int
	mux           *http.ServeMux
	ctx           context.Context // ends with Close, and every Exec with it
	stop          context.CancelFunc

	mu       sync.Mutex
	closed   bool
	sessions map[string]*session
}

var _ http.Handler = (*Handler)(nil)

// An Option configures NewHandler.
type Option func(*Handler)

// WithAuthenticator resolves each request's principal with a.
func WithAuthenticator(a Authenticator) Option { return func(h *Handler) { h.auth = a } }

// WithInsecureNoAuth serves every request, unauthenticated, as a
// principal named "insecure" that holds every scope. For tests, and for
// a program that has decided nothing reaches the handler but itself
// (a loopback listener it alone dials), explicitly.
func WithInsecureNoAuth() Option { return func(h *Handler) { h.insecure = true } }

// WithLease is how long a session lives without a renewal; the default
// is DefaultLease.
func WithLease(d time.Duration) Option { return func(h *Handler) { h.lease = d } }

// WithMaxSessions bounds the sessions open at once; the default is
// DefaultMaxSessions.
func WithMaxSessions(n int) Option { return func(h *Handler) { h.maxSessions = n } }

// WithMaxProcesses bounds the processes open at once in one session;
// the default is DefaultMaxProcesses.
func WithMaxProcesses(n int) Option { return func(h *Handler) { h.maxProcesses = n } }

// WithMaxWriteBytes bounds a WriteFile's data and an Exec's stdin; the
// default is DefaultMaxWriteBytes.
func WithMaxWriteBytes(n int64) Option { return func(h *Handler) { h.maxWrite = n } }

// WithMaxDirEntries bounds one listing: a directory holding more is
// ErrLimit. The default is DefaultMaxDirEntries.
func WithMaxDirEntries(n int) Option { return func(h *Handler) { h.maxDirEntries = n } }

// NewHandler serves ws. It refuses a nil workspace, a bound that is not
// positive, and both an authenticator and WithInsecureNoAuth.
func NewHandler(ws agentworkspace.Workspace, opts ...Option) (*Handler, error) {
	if ws == nil {
		return nil, errors.New("remote: no workspace to serve")
	}
	h := &Handler{
		ws:            ws,
		lease:         DefaultLease,
		maxSessions:   DefaultMaxSessions,
		maxProcesses:  DefaultMaxProcesses,
		maxWrite:      DefaultMaxWriteBytes,
		maxDirEntries: DefaultMaxDirEntries,
		sessions:      map[string]*session{},
	}
	for _, o := range opts {
		if o != nil {
			o(h)
		}
	}
	switch {
	case h.auth != nil && h.insecure:
		return nil, errors.New("remote: an authenticator and WithInsecureNoAuth; choose one")
	case h.lease < time.Millisecond:
		return nil, fmt.Errorf("remote: a lease of %v; at least a millisecond", h.lease)
	case h.maxSessions <= 0 || h.maxProcesses <= 0 || h.maxWrite <= 0 || h.maxDirEntries <= 0:
		return nil, errors.New("remote: a bound that is not positive")
	}
	m := http.NewServeMux()
	m.HandleFunc("GET "+routeInfo, h.need(ScopeRead, h.info))
	m.HandleFunc("GET "+routeStat, h.need(ScopeRead, h.stat))
	m.HandleFunc("GET "+routeLstat, h.need(ScopeRead, h.lstat))
	m.HandleFunc("GET "+routeReadLink, h.need(ScopeRead, h.readLink))
	m.HandleFunc("GET "+routeReadDir, h.need(ScopeRead, h.readDir))
	m.HandleFunc("GET "+routeFile, h.need(ScopeRead, h.read))
	m.HandleFunc("PUT "+routeFile, h.need(ScopeWrite, h.write))
	m.HandleFunc("DELETE "+routeFile, h.need(ScopeWrite, h.remove))
	m.HandleFunc("POST "+routeExec, h.need(ScopeExec, h.exec))
	m.HandleFunc("POST "+routeSessions, h.need(ScopeExec, h.openSession))
	m.HandleFunc("POST "+routeSessions+"/{sid}/renew", h.need(ScopeExec, h.renewSession))
	m.HandleFunc("DELETE "+routeSessions+"/{sid}", h.need(ScopeExec, h.closeSession))
	m.HandleFunc("POST "+routeSessions+"/{sid}/processes", h.need(ScopeExec, h.start))
	m.HandleFunc("POST "+routeSessions+"/{sid}/processes/{pid}/stdin", h.need(ScopeExec, h.stdin))
	m.HandleFunc("GET "+routeSessions+"/{sid}/processes/{pid}/stdout", h.need(ScopeExec, h.stdout))
	m.HandleFunc("GET "+routeSessions+"/{sid}/processes/{pid}/stderr", h.need(ScopeExec, h.stderr))
	m.HandleFunc("GET "+routeSessions+"/{sid}/processes/{pid}/wait", h.need(ScopeExec, h.wait))
	m.HandleFunc("POST "+routeSessions+"/{sid}/processes/{pid}/signal", h.need(ScopeExec, h.signal))
	m.HandleFunc("POST "+routeSessions+"/{sid}/processes/{pid}/cancel", h.need(ScopeExec, h.cancel))
	m.HandleFunc("DELETE "+routeSessions+"/{sid}/processes/{pid}", h.need(ScopeExec, h.closeProcess))
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, wireError{Code: codeNotExist, Message: "no such route"})
	})
	h.mux = m
	h.ctx, h.stop = context.WithCancel(context.Background())
	return h, nil
}

// ServeHTTP authenticates r, whatever its route, and serves it.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var p Principal
	switch {
	case h.insecure:
		p = Principal{Name: "insecure", Scopes: allScopes()}
	case h.auth == nil:
		unauthenticated(w, "no authenticator is configured")
		return
	default:
		var err error
		if p, err = h.auth(r); err != nil {
			unauthenticated(w, "unauthenticated")
			return
		}
	}
	h.mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
}

func unauthenticated(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeError(w, wireError{Code: codeUnauthenticated, Message: "remote: " + msg})
}

// Close ends every session, and every process each started, and every
// Exec in flight, and refuses every request after; it returns once the
// sessions' processes have ended. It does not close the served
// workspace.
func (h *Handler) Close() error {
	h.stop()
	h.mu.Lock()
	h.closed = true
	all := make([]*session, 0, len(h.sessions))
	for id, s := range h.sessions {
		all = append(all, s)
		s.gone = true
		s.timer.Stop()
		delete(h.sessions, id)
	}
	h.mu.Unlock()
	var wg sync.WaitGroup
	for _, s := range all {
		wg.Go(s.end)
	}
	wg.Wait()
	return nil
}

func (h *Handler) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// need serves f to a principal holding s, on a handler not closed.
func (h *Handler) need(s Scope, f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !principalOf(r).Has(s) {
			writeError(w, wireError{Code: codeForbidden, Message: fmt.Sprintf("remote: the principal lacks the %s scope", s)})
			return
		}
		if h.isClosed() {
			writeError(w, wireError{Code: codeClosed, Message: "the server is closed"})
			return
		}
		f(w, r)
	}
}

func writeError(w http.ResponseWriter, e wireError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusOf(e.Code))
	_ = json.NewEncoder(w).Encode(errorReply{Error: e})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// fail answers err, the served workspace's, as the wire carries it.
func fail(w http.ResponseWriter, err error) { writeError(w, errorOf(err)) }

// decode reads r's JSON body into v, at most max bytes, and answers the
// refusal itself when it cannot.
func decode(w http.ResponseWriter, r *http.Request, max int64, v any) bool {
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, max)).Decode(v)
	var tooBig *http.MaxBytesError
	switch {
	case err == nil:
		return true
	case errors.As(err, &tooBig):
		writeError(w, wireError{Code: codeLimit, Message: fmt.Sprintf("the body is over the %d-byte bound", max)})
	default:
		writeError(w, wireError{Code: codeInvalid, Message: "the body is not JSON of the route's shape"})
	}
	return false
}

func (h *Handler) info(w http.ResponseWriter, r *http.Request) {
	p := principalOf(r)
	_, start := h.ws.(agentworkspace.Starter)
	_, links := h.ws.FS().(fs.ReadLinkFS)
	reply := infoReply{
		Root:   h.ws.Root(),
		Served: descriptorOf(h.ws.Descriptor()),
		Start:  start,
		Links:  links,
		Scopes: p.Scopes,
		Limits: limits{
			LeaseMs:       h.lease.Milliseconds(),
			MaxProcesses:  h.maxProcesses,
			MaxWriteBytes: h.maxWrite,
			MaxDirEntries: h.maxDirEntries,
		},
	}
	if p.Has(ScopeExec) {
		reply.Env = h.ws.Env()
	}
	writeJSON(w, reply)
}

// name is r's one path, refused (and answered) when it is not a name
// in the workspace, whatever the served FS would make of it.
func name(w http.ResponseWriter, r *http.Request, op string) (string, bool) {
	names := r.URL.Query()["path"]
	if len(names) != 1 {
		writeError(w, wireError{Code: codeInvalid, Message: "want one path"})
		return "", false
	}
	if err := valid(op, names[0]); err != nil {
		fail(w, err)
		return "", false
	}
	return names[0], true
}

func (h *Handler) stat(w http.ResponseWriter, r *http.Request) {
	n, ok := name(w, r, "stat")
	if !ok {
		return
	}
	fi, err := fs.Stat(h.ws.FS(), n)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, infoOf(fi))
}

// links is the served FS as an fs.ReadLinkFS, refused (and answered)
// as errors.ErrUnsupported when it cannot read links.
func (h *Handler) links(w http.ResponseWriter) (fs.ReadLinkFS, bool) {
	l, ok := h.ws.FS().(fs.ReadLinkFS)
	if !ok {
		fail(w, fmt.Errorf("the workspace's file system cannot read links: %w", errors.ErrUnsupported))
	}
	return l, ok
}

func (h *Handler) lstat(w http.ResponseWriter, r *http.Request) {
	n, ok := name(w, r, "lstat")
	if !ok {
		return
	}
	l, ok := h.links(w)
	if !ok {
		return
	}
	fi, err := l.Lstat(n)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, infoOf(fi))
}

func (h *Handler) readLink(w http.ResponseWriter, r *http.Request) {
	n, ok := name(w, r, "readlink")
	if !ok {
		return
	}
	l, ok := h.links(w)
	if !ok {
		return
	}
	t, err := l.ReadLink(n)
	if err != nil {
		fail(w, err)
		return
	}
	reply := linkReply{Target: t}
	if !utf8.ValidString(t) {
		reply = linkReply{Raw: []byte(t)}
	}
	writeJSON(w, reply)
}

func (h *Handler) readDir(w http.ResponseWriter, r *http.Request) {
	n, ok := name(w, r, "readdir")
	if !ok {
		return
	}
	es, err := fs.ReadDir(h.ws.FS(), n)
	if err != nil {
		fail(w, err)
		return
	}
	if len(es) > h.maxDirEntries {
		writeError(w, wireError{Code: codeLimit, Message: fmt.Sprintf("%d entries, over the %d a listing gives", len(es), h.maxDirEntries)})
		return
	}
	reply := dirReply{Entries: make([]fileInfo, 0, len(es))}
	for _, e := range es {
		fi, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue // removed since the listing
		}
		if err != nil {
			fail(w, err)
			return
		}
		reply.Entries = append(reply.Entries, infoOf(fi))
	}
	writeJSON(w, reply)
}

// read streams a regular file's content. What is opened is checked, so
// a FIFO or a device in the file's place is refused, not read, and the
// open does not block on one when the served FS's does not.
func (h *Handler) read(w http.ResponseWriter, r *http.Request) {
	n, ok := name(w, r, "read")
	if !ok {
		return
	}
	f, err := h.ws.FS().Open(n)
	if err != nil {
		fail(w, err)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		fail(w, err)
		return
	}
	if !fi.Mode().IsRegular() {
		fail(w, fmt.Errorf("%s is not a regular file", n))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, f); err != nil {
		// Cut short, so the client reads a truncated stream as one and
		// not as the end of the file.
		panic(http.ErrAbortHandler)
	}
}

// fileModes are the bits of a WriteFile's perm passed on.
const fileModes = fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

func (h *Handler) write(w http.ResponseWriter, r *http.Request) {
	n, ok := name(w, r, "write")
	if !ok {
		return
	}
	perm, err := strconv.ParseUint(r.URL.Query().Get("perm"), 10, 32)
	if err != nil {
		writeError(w, wireError{Code: codeInvalid, Message: "perm is not a file mode"})
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxWrite))
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		writeError(w, wireError{Code: codeLimit, Message: fmt.Sprintf("a write over the %d-byte bound", h.maxWrite)})
		return
	case err != nil:
		writeError(w, wireError{Code: codeOther, Message: "the body could not be read"})
		return
	}
	if err := h.ws.WriteFile(r.Context(), n, data, fs.FileMode(perm)&fileModes); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) remove(w http.ResponseWriter, r *http.Request) {
	n, ok := name(w, r, "remove")
	if !ok {
		return
	}
	if err := h.ws.Remove(r.Context(), n); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// exec runs one command under the request's context, so a client that
// goes ends it and everything it started, as does the handler's Close,
// and answers in NDJSON frames: Command.Stream's output as it arrives,
// when the client asked for it, then the Output or the error.
func (h *Handler) exec(w http.ResponseWriter, r *http.Request) {
	var req execRequest
	if !decode(w, r, (h.maxWrite+2)/3*4+MaxRequestBytes, &req) {
		return
	}
	switch {
	case int64(len(req.Stdin)) > h.maxWrite:
		writeError(w, wireError{Code: codeLimit, Message: fmt.Sprintf("stdin over the %d-byte bound", h.maxWrite)})
		return
	case req.TimeoutMs < 0:
		writeError(w, wireError{Code: codeInvalid, Message: "a negative timeout"})
		return
	}
	cmd := agentworkspace.Command{
		Args:    req.Args,
		Dir:     req.Dir,
		Env:     req.Env,
		Stdin:   req.Stdin,
		Timeout: time.Duration(req.TimeoutMs) * time.Millisecond,
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	fr := &frames{enc: json.NewEncoder(w), rc: http.NewResponseController(w)}
	if req.Stream {
		cmd.Stream = fr
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer context.AfterFunc(h.ctx, cancel)()
	out, err := h.ws.Exec(ctx, cmd)
	if err != nil {
		e := errorOf(err)
		fr.send(execFrame{Error: &e})
		return
	}
	fr.send(execFrame{Output: &execOutput{Stdout: out.Stdout, Stderr: out.Stderr, ExitCode: out.ExitCode, TimedOut: out.TimedOut}})
}

// frames writes Exec's frames, each flushed as it is written. Its Write
// is Command.Stream's: one data frame per write.
type frames struct {
	mu  sync.Mutex
	enc *json.Encoder
	rc  *http.ResponseController
	err error
}

func (f *frames) send(fr execFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.err = f.enc.Encode(fr); f.err == nil {
		f.err = f.rc.Flush()
	}
	return f.err
}

func (f *frames) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := f.send(execFrame{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}
