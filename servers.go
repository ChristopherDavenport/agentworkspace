package agentworkspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
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
// not ValidServerName is an error, and so is a name given twice.
func Offer(ws Starter, servers []OfferedServer) (*Offering, error) {
	o := &Offering{Starter: ws, servers: map[string]OfferedServer{}}
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
		o.servers[s.Name] = s
	}
	return o, nil
}

// OfferedServer is one server Offer offers: the Server a client is told
// of, and the command that starts it.
type OfferedServer struct {
	Server
	Command Command
}

// Offering is a Starter offering servers that are commands in it,
// which Offer makes.
type Offering struct {
	Starter
	servers map[string]OfferedServer
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

// StartServer starts the server called name from its command.
func (o *Offering) StartServer(ctx context.Context, name string, stderr io.Writer) (Process, error) {
	s, ok := o.servers[name]
	if !ok {
		return nil, fmt.Errorf("server %q: %w", name, ErrNoServer)
	}
	cmd := s.Command
	cmd.Args = slices.Clone(cmd.Args)
	cmd.Env = slices.Clone(cmd.Env)
	cmd.Stream = stderr
	return o.Start(ctx, cmd)
}
