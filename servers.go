package agentworkspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
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
// workspace elsewhere (mcpworkspace's) implements it too, without being
// a Workspace, which is why it does not embed one.
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
// Command.Env sets exactly once. The commands are copied: changing the
// slices given changes nothing Offer offers.
func Offer(ws Starter, servers []OfferedServer, opts ...OfferOption) (*Offering, error) {
	if ws == nil {
		return nil, errors.New("agentworkspace: Offer of no workspace")
	}
	o := &Offering{Starter: ws, servers: map[string]OfferedServer{}}
	for _, opt := range opts {
		opt(o)
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
// exactly once: the value a filter is told of is then the value the
// server is given.
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
		n := 0
		for _, kv := range s.Command.Env {
			if strings.HasPrefix(kv, name+"=") {
				n++
			}
		}
		switch n {
		case 0:
			return fmt.Errorf("agentworkspace: server %q: secret %q is not set in its Env", s.Name, name)
		case 1:
		default:
			return fmt.Errorf("agentworkspace: server %q: secret %q is set %d times in its Env", s.Name, name, n)
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

// An OfferOption changes how an Offering starts its servers.
type OfferOption func(*Offering)

// An OutputFilter is what a server's output is read through on its way
// out of an Offering: r is the server's standard output, or its
// standard error, and secrets are the values of the variables the
// server names as Secrets, by name. The reader it returns must pass a
// line on as soon as it has it, since the MCP stream on standard output
// is a line at a time, and give what it holds back at r's end of file.
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
// names none is started as without it, and without a filter nothing
// reads a server's output for its secrets, whatever it names.
func WithOutputFilter(f OutputFilter) OfferOption {
	return func(o *Offering) { o.filter = f }
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
	filter  OutputFilter
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
// its standard error read through the filter: what the filter gives of
// standard error is written to stderr by the time Wait or Close
// returns, for a workspace that, as Local does, has written all of a
// process's standard error to Command.Stream once its Wait returns.
func (o *Offering) StartServer(ctx context.Context, name string, stderr io.Writer) (Process, error) {
	s, ok := o.servers[name]
	if !ok {
		return nil, fmt.Errorf("server %q: %w", name, ErrNoServer)
	}
	cmd := s.Command
	cmd.Args = slices.Clone(cmd.Args)
	cmd.Env = slices.Clone(cmd.Env)
	cmd.Stream = stderr
	if o.filter == nil || len(s.Secrets) == 0 {
		return o.Start(ctx, cmd)
	}
	values := secretValues(s)
	fp := &filteredProcess{flushed: make(chan struct{})}
	var errW *io.PipeWriter
	drained := make(chan struct{})
	if stderr == nil {
		close(drained)
	} else {
		var errR *io.PipeReader
		errR, errW = io.Pipe()
		cmd.Stream = errW
		go func() {
			defer close(drained)
			if _, err := io.Copy(stderr, o.filter(name, values, errR)); err != nil {
				// Keep reading, or the process blocks writing.
				io.Copy(io.Discard, errR)
			}
		}()
	}
	p, err := o.Start(ctx, cmd)
	if err != nil {
		if errW != nil {
			errW.Close()
		}
		<-drained
		return nil, err
	}
	fp.Process = p
	fp.stdout = &filteredOutput{Reader: o.filter(name, values, p.Stdout()), closer: p.Stdout()}
	go func() {
		// Once the process has exited, its standard error is all
		// written: the filter is given its end of file and what it
		// holds back is written out.
		p.Wait()
		if errW != nil {
			errW.Close()
		}
		<-drained
		close(fp.flushed)
	}()
	return fp, nil
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
// closed as the output is.
type filteredOutput struct {
	io.Reader
	closer io.Closer
}

func (f *filteredOutput) Close() error { return f.closer.Close() }
