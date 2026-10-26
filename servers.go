package agentworkspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNoServer is a server name a ServerProvider does not offer.
var ErrNoServer = errors.New("no such server in this workspace")

// Server is an MCP server a workspace offers: one it holds of its own,
// such as a server installed in a container's image or one a host runs
// beside a sandbox, rather than one the program names a command for.
type Server struct {
	// Name is what the server is called, as ValidServerName holds it.
	Name string
	// Description says what the server is for, for a person choosing
	// among them; empty when the workspace says nothing.
	Description string
}

// ValidServerName reports whether name may name a Server: one to 32
// lowercase letters, digits and inner hyphens, beginning and ending
// with a letter or a digit. It has no underscore, so a program can
// prefix a server's tools with its name and a separator of underscores
// (mcp__<name>__<tool>) without two servers' tools running together.
func ValidServerName(name string) bool {
	if len(name) == 0 || len(name) > 32 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case 'a' <= c && c <= 'z', '0' <= c && c <= '9':
		case c == '-' && i > 0 && i < len(name)-1:
		default:
			return false
		}
	}
	return true
}

// ServerProvider is a workspace that offers MCP servers of its own,
// each a stdio server it starts by name: the program lists them and
// connects to the ones it wants over the process's pipes, as it would
// to a server it started from a command. It is an interface of its own,
// as Starter is, so that a workspace offering none is still a
// Workspace; a caller asks for it with a type assertion. A client of a
// workspace elsewhere implements it too: mcpworkspace's, without being
// a Workspace, which is why it does not embed one, and remote's, when
// its principal holds the servers scope.
//
// Which of the servers a session uses is the program's to decide: a
// workspace offering a server does not make the program connect to it.
type ServerProvider interface {
	// Servers lists the servers the workspace offers, in a stable order.
	Servers(ctx context.Context) ([]Server, error)
	// StartServer starts the server called name and returns at once, as
	// Starter's Start does, with the same lifetime: Process.Close, the
	// end of ctx and the workspace's Close each end it and everything it
	// started, and a caller that is done with it calls Close. Its
	// standard input and output carry the MCP stream, and its standard
	// error goes to stderr, or nowhere when stderr is nil. A name the
	// workspace does not offer is an error that errors.Is ErrNoServer.
	StartServer(ctx context.Context, name string, stderr io.Writer) (Process, error)
}

// Offer returns ws offering servers, each started from its command in
// ws, at its Dir with its Env, as Start starts any command: a
// ServerProvider for a workspace whose servers are programs installed
// in it. It is also a Starter, as ws is, so a program that starts
// commands of its own in the workspace can keep doing so. A server's
// Command.Stdin is refused when it starts, as Start refuses it, and its
// Command.Stream is replaced by StartServer's stderr. A name that is
// not ValidServerName is an error, and so is a name given twice, and so
// is a nil ws. A server's Secrets must each name a variable its
// Command.Env sets exactly once, to a value that is not empty, and a
// server that names any is refused unless WithOutputFilter is given:
// nothing would keep them out of its output. A nil option is skipped.
// The commands are copied: changing the slices given changes nothing
// Offer offers.
//
// The filter covers the offered servers' own output and nothing else.
// An Offering embeds ws, so its Start and Exec run whatever they are
// given, unfiltered, as the same user as the servers; a client of a
// workspace that runs its commands (one served through mcpworkspace's
// NewProcesses answers both servers.start and process.start, and one
// served through remote to a principal holding exec runs its Exec and
// Start) can read a running server's environment, its secrets among
// it, from /proc/<pid>/environ. Offer secret-holding servers from a
// workspace that runs nothing else for the client, as dax's box host
// does with each server a unit in its own container, or serve the
// Offering through remote with the servers scope and not exec.
func Offer(ws Starter, servers []OfferedServer, opts ...OfferOption) (*Offering, error) {
	if ws == nil {
		return nil, errors.New("agentworkspace: Offer of no workspace")
	}
	o := &Offering{Starter: ws, servers: map[string]OfferedServer{}}
	for _, opt := range opts {
		if opt != nil {
			opt(&o.config)
		}
	}
	for _, s := range servers {
		if !ValidServerName(s.Name) {
			return nil, fmt.Errorf("agentworkspace: server name %q: want 1 to 32 lowercase letters, digits and inner hyphens", s.Name)
		}
		if _, ok := o.servers[s.Name]; ok {
			return nil, fmt.Errorf("agentworkspace: server %q offered twice", s.Name)
		}
		if len(s.Command.Args) == 0 {
			return nil, fmt.Errorf("agentworkspace: server %q has no command", s.Name)
		}
		if err := checkSecrets(s); err != nil {
			return nil, err
		}
		if len(s.Secrets) > 0 && o.config.filter == nil {
			return nil, fmt.Errorf("agentworkspace: server %q names secrets, but no WithOutputFilter keeps them out of its output", s.Name)
		}
		// The caller's slices are its own to change after Offer.
		s.Command.Args = slices.Clone(s.Command.Args)
		s.Command.Env = slices.Clone(s.Command.Env)
		s.Secrets = slices.Clone(s.Secrets)
		o.servers[s.Name] = s
	}
	return o, nil
}

// checkSecrets refuses a server whose Secrets name a variable that is
// not a variable's name, is named twice, or that its Env does not set
// exactly once, or sets to nothing: the value a filter is told of is
// then the value the server is given, and one a filter can look for.
func checkSecrets(s OfferedServer) error {
	seen := map[string]bool{}
	for _, name := range s.Secrets {
		if !validEnvName(name) {
			return fmt.Errorf("agentworkspace: server %q: secret %q is not a variable's name", s.Name, name)
		}
		if seen[name] {
			return fmt.Errorf("agentworkspace: server %q: secret %q named twice", s.Name, name)
		}
		seen[name] = true
		n, empty := 0, false
		for _, kv := range s.Command.Env {
			if v, ok := strings.CutPrefix(kv, name+"="); ok {
				n++
				empty = v == ""
			}
		}
		switch {
		case n == 0:
			return fmt.Errorf("agentworkspace: server %q: secret %q is not set in its Env", s.Name, name)
		case n > 1:
			return fmt.Errorf("agentworkspace: server %q: secret %q is set %d times in its Env", s.Name, name, n)
		case empty:
			return fmt.Errorf("agentworkspace: server %q: secret %q is empty", s.Name, name)
		}
	}
	return nil
}

// validEnvName reports whether name is a variable's name as a shell
// takes one: a letter or underscore, then letters, digits and
// underscores.
func validEnvName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_', 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z':
		case '0' <= c && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// An OfferOption changes how an Offering starts its servers. The
// options are this package's own, such as WithOutputFilter.
type OfferOption func(*offerConfig)

// offerConfig is what the options set.
type offerConfig struct {
	filter OutputFilter
}

// An OutputFilter is what a server's output is read through on its way
// out of an Offering: r is the server's standard output, or its
// standard error, and secrets are the values of the variables the
// server names as Secrets, by name, a copy of its own on each call. It
// is called twice for each start, once for each stream, and the two
// readers are read at once. The reader it returns must pass a line on
// as soon as it has it, since the MCP stream on standard output is a
// line at a time, and give what it holds back at r's end of file. A
// nil reader is refused: the start fails, and the server is ended.
//
// A secret is replaced wherever its value appears, so a very short one
// ("1", "true") is replaced inside the server's own JSON-RPC too, which
// breaks the conversation rather than leak; nothing here holds a value
// to a length. A value holding a newline, such as a PEM key, is not
// found on its raw spelling by a filter that works a line at a time, as
// agentredact's secrets does (its JSON spellings, with \n escaped, are
// found); such values are taken, not refused.
//
// agentredact's secrets module is the filter this is shaped for, each
// value replaced in every spelling a JSON encoder writes it in:
//
//	agentworkspace.WithOutputFilter(func(_ string, values map[string]string, r io.Reader) io.Reader {
//		return secrets.New(values).Reader(r, nil)
//	})
//
// It is a backstop, not a guarantee: a value the server encodes
// otherwise (base64, split across messages) passes through.
type OutputFilter func(server string, secrets map[string]string, r io.Reader) io.Reader

// WithOutputFilter has every server that names Secrets started with its
// standard output and standard error read through f. A server that
// names none is started as without it. It covers what the server
// writes, and nothing else the workspace runs: see Offer.
func WithOutputFilter(f OutputFilter) OfferOption {
	return func(c *offerConfig) { c.filter = f }
}

// OfferedServer is one server Offer offers: the Server a client is told
// of, and the command that starts it.
type OfferedServer struct {
	Server
	Command Command
	// Secrets name the variables of Command.Env whose values are
	// secrets, such as the server's token: an Offering with an
	// OutputFilter reads the server's output through it, told these
	// values. A client is never told of them.
	Secrets []string
}

// Offering is a Starter offering servers that are commands in it,
// which Offer makes.
type Offering struct {
	Starter
	servers map[string]OfferedServer
	config  offerConfig
}

var _ ServerProvider = (*Offering)(nil)

// Servers is the servers offered, by name.
func (o *Offering) Servers(context.Context) ([]Server, error) {
	out := make([]Server, 0, len(o.servers))
	for _, name := range slices.Sorted(maps.Keys(o.servers)) {
		out = append(out, o.servers[name].Server)
	}
	return out, nil
}

// StartServer starts the server called name from its command. With an
// OutputFilter, a server that names Secrets has its standard output and
// its standard error read through the filter.
//
// What the filter gives of standard error is written to stderr by the
// time Wait or Close returns. Nothing reaches stderr that the filter has
// not seen whole and in order: when standard error cannot be filtered
// so, it is cut, for the rest of the server's life, after one line of
// this package's own saying why ("agentworkspace: <server>: standard
// error cut (<reason>)"), and nothing of the server's comes after it.
// It is cut when the filter falls behind by more than stderrBacklog (a
// megabyte; with what the filter is reading, the lag can reach about
// twice that), so the workspace's writes never wait on the filter; when
// the filter still holds some of it a short grace after the process has
// exited, so a filter that holds on does not hold up the host; and when
// the filter fails or panics. When it is stderr itself that stalls past
// the grace, Wait and Close return without it, and the notice is
// written once stderr takes writes again. Standard error the workspace
// writes after
// its process's Wait returns is dropped (io.ErrClosedPipe); Local
// writes all of it before. Standard output is not cut: a filter's
// failure there is the read's error.
func (o *Offering) StartServer(ctx context.Context, name string, stderr io.Writer) (Process, error) {
	s, ok := o.servers[name]
	if !ok {
		return nil, fmt.Errorf("server %q: %w", name, ErrNoServer)
	}
	cmd := s.Command
	cmd.Args = slices.Clone(cmd.Args)
	cmd.Env = slices.Clone(cmd.Env)
	cmd.Stream = stderr
	filter := o.config.filter
	if filter == nil || len(s.Secrets) == 0 {
		return o.Start(ctx, cmd)
	}
	values := secretValues(s)
	var se *stderrFilter
	if stderr != nil {
		var err error
		if se, err = newStderrFilter(filter, name, values, stderr); err != nil {
			return nil, err
		}
		cmd.Stream = se.q
	}
	p, err := o.Start(ctx, cmd)
	if err != nil {
		se.end()
		return nil, err
	}
	stdout, err := applyFilter(filter, name, values, p.Stdout())
	if err != nil {
		p.Close()
		se.end()
		return nil, err
	}
	fp := &filteredProcess{Process: p, flushed: make(chan struct{})}
	fp.stdout = &filteredOutput{Reader: stdout, closer: p.Stdout()}
	go func() {
		// Once the process has exited, its standard error is all
		// written: the filter is given its end of file and what it
		// holds back is written out.
		p.Wait()
		se.end()
		close(fp.flushed)
	}()
	return fp, nil
}

// filterGrace is how long a filtered server's end waits for the filter
// to give the rest of its standard error: the grace Local gives a
// process's output.
const filterGrace = waitDelay

// stderrBacklog is how much of a filtered server's standard error may
// wait for the filter to read it; past it, standard error is cut.
const stderrBacklog = 1 << 20

// errStderrCut is what the stderr filter reads once standard error is
// cut, and errStderrDone once the copy has ended.
var (
	errStderrCut  = errors.New("agentworkspace: standard error cut")
	errStderrDone = errors.New("agentworkspace: standard error done")
)

// stderrFilter is a filtered server's standard error: the workspace
// writes to q, which never blocks; q feeds a pipe the filter reads; and
// what the filter gives is copied to the caller's writer through a gate
// that, once cut, forwards nothing more.
type stderrFilter struct {
	name    string
	q       *stderrQueue
	errR    *io.PipeReader
	w       io.Writer // the caller's
	drained chan struct{}

	mu     sync.Mutex
	cut    atomic.Bool
	reason string
}

func newStderrFilter(f OutputFilter, name string, values map[string]string, w io.Writer) (*stderrFilter, error) {
	errR, errW := io.Pipe()
	filtered, err := applyFilter(f, name, values, errR)
	if err != nil {
		errR.Close()
		return nil, err
	}
	se := &stderrFilter{name: name, errR: errR, w: w, drained: make(chan struct{})}
	se.q = &stderrQueue{ready: make(chan struct{}, 1), overflow: func() { se.cutFor("the filter fell more than a megabyte behind") }}
	go se.q.feed(errW)
	go se.copy(filtered)
	return se, nil
}

// cutFor cuts standard error for reason, the first time: nothing more is
// forwarded, the queue keeps nothing, and the filter's source ends. It
// never blocks, so the queue may call it from the workspace's write.
func (se *stderrFilter) cutFor(reason string) {
	se.mu.Lock()
	if se.cut.Load() {
		se.mu.Unlock()
		return
	}
	se.reason = reason
	se.cut.Store(true)
	se.mu.Unlock()
	se.q.discard()
	se.errR.CloseWithError(errStderrCut)
}

// copy copies what the filter gives to the caller's writer until the
// filter ends, then says why standard error was cut, if it was.
func (se *stderrFilter) copy(r io.Reader) {
	defer close(se.drained)
	buf := make([]byte, 32<<10)
	for {
		n, err := safeRead(r, buf)
		if err != nil && err != io.EOF {
			// What a failing filter gives with its error, such as a
			// line it held, is not forwarded.
			se.cutFor("the filter failed")
		}
		if n > 0 && !se.cut.Load() {
			if _, werr := se.w.Write(buf[:n]); werr != nil {
				se.cutFor("the writer failed")
			}
		}
		if err != nil {
			break
		}
	}
	if se.cut.Load() {
		se.mu.Lock()
		reason := se.reason
		se.mu.Unlock()
		if reason != "the writer failed" {
			fmt.Fprintf(se.w, "agentworkspace: %s: standard error cut (%s)\n", se.name, reason)
		}
	}
	// The filter may end before its source has: the feed is let go.
	se.errR.CloseWithError(errStderrDone)
}

// end gives the filter its end of file and waits, at most filterGrace,
// for what it holds back; past it, standard error is cut. A nil se is
// a server without standard error.
func (se *stderrFilter) end() {
	if se == nil {
		return
	}
	se.q.close()
	t := time.NewTimer(filterGrace)
	defer t.Stop()
	select {
	case <-se.drained:
	case <-t.C:
		se.cutFor("the filter held it past its grace")
	}
}

// stderrQueue is the Command.Stream of a filtered server: Write never
// blocks, keeping at most stderrBacklog bytes the filter has not read,
// and feed hands them to the filter's pipe. Past the backlog it calls
// overflow, and from then on keeps nothing.
type stderrQueue struct {
	mu       sync.Mutex
	buf      []byte
	closed   bool
	cut      bool
	ready    chan struct{}
	overflow func()
}

func (q *stderrQueue) Write(p []byte) (int, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if q.cut {
		q.mu.Unlock()
		return len(p), nil
	}
	if len(q.buf)+len(p) > stderrBacklog {
		q.cut, q.buf = true, nil
		q.signal()
		q.mu.Unlock()
		q.overflow()
		return len(p), nil
	}
	q.buf = append(q.buf, p...)
	q.signal()
	q.mu.Unlock()
	return len(p), nil
}

// close ends the queue: what it holds is fed, then the pipe's end of
// file, and a later Write is io.ErrClosedPipe.
func (q *stderrQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.signal()
}

// discard keeps nothing more: standard error is cut.
func (q *stderrQueue) discard() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cut, q.buf = true, nil
	q.signal()
}

// signal wakes feed; q.mu is held.
func (q *stderrQueue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// feed writes what q holds to w until q is closed and empty, then
// closes w. Once q is cut it feeds nothing more; a write w refuses, the
// copy over or standard error cut, ends it too.
func (q *stderrQueue) feed(w *io.PipeWriter) {
	for {
		q.mu.Lock()
		b, closed, cut := q.buf, q.closed, q.cut
		q.buf = nil
		q.mu.Unlock()
		if cut {
			return
		}
		if len(b) > 0 {
			if _, err := w.Write(b); err != nil {
				return
			}
			continue
		}
		if closed {
			w.Close()
			return
		}
		<-q.ready
	}
}

// applyFilter is f's reader of r, for the server called name, told its
// own copy of values; a nil reader, or a filter that panics, is an
// error.
func applyFilter(f OutputFilter, name string, values map[string]string, r io.Reader) (out io.Reader, err error) {
	defer func() {
		if v := recover(); v != nil {
			out, err = nil, fmt.Errorf("agentworkspace: server %q: the output filter panicked: %v", name, v)
		}
	}()
	if out = f(name, maps.Clone(values), r); out == nil {
		return nil, fmt.Errorf("agentworkspace: server %q: the output filter gave no reader", name)
	}
	return out, nil
}

// safeRead is r.Read, a panic its error.
func safeRead(r io.Reader, p []byte) (n int, err error) {
	defer func() {
		if v := recover(); v != nil {
			n, err = 0, fmt.Errorf("agentworkspace: the output filter panicked: %v", v)
		}
	}()
	return r.Read(p)
}

// secretValues is the value of each variable s names as a secret, by
// name, as its Env sets it (checkSecrets holds that it does, once).
func secretValues(s OfferedServer) map[string]string {
	values := make(map[string]string, len(s.Secrets))
	for _, name := range s.Secrets {
		for _, kv := range s.Command.Env {
			if v, ok := strings.CutPrefix(kv, name+"="); ok {
				values[name] = v
			}
		}
	}
	return values
}

// filteredProcess is a started server whose output an OutputFilter
// reads: Stdout is the filter's reader, and Wait and Close return once
// what the filter gave of standard error is written.
type filteredProcess struct {
	Process
	stdout  io.ReadCloser
	flushed chan struct{}
}

func (p *filteredProcess) Stdout() io.ReadCloser { return p.stdout }

func (p *filteredProcess) Wait() (int, error) {
	status, err := p.Process.Wait()
	<-p.flushed
	return status, err
}

func (p *filteredProcess) Close() error {
	err := p.Process.Close()
	<-p.flushed
	return err
}

// filteredOutput is a filter's reader of a process's standard output,
// closed as the output is. A filter's panic is the read's error, and
// every later read's, the filter not asked again; the output is closed,
// so the server's writes fail rather than pile up.
type filteredOutput struct {
	io.Reader
	closer io.Closer
	mu     sync.Mutex
	err    error
}

func (f *filteredOutput) Read(p []byte) (int, error) {
	f.mu.Lock()
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return 0, err
	}
	n, panicked := 0, false
	func() {
		defer func() {
			if v := recover(); v != nil {
				panicked, err = true, fmt.Errorf("agentworkspace: the output filter panicked: %v", v)
			}
		}()
		n, err = f.Reader.Read(p)
	}()
	if panicked {
		f.mu.Lock()
		f.err = err
		f.mu.Unlock()
		f.closer.Close()
		return 0, err
	}
	return n, err
}

func (f *filteredOutput) Close() error { return f.closer.Close() }
