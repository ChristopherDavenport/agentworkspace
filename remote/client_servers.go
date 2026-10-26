package remote

// The client half of the servers the served workspace offers: listed,
// and started as processes of the session.

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"

	"github.com/ChristopherDavenport/agentworkspace"
)

// Servers lists the MCP servers the served workspace offers, in the
// order it lists them. It is bounded by ctx and by WithTimeout.
func (s *ServerClient) Servers(ctx context.Context) ([]agentworkspace.Server, error) {
	return s.Client.servers(ctx)
}

// StartServer starts the server called name in the served workspace,
// in the client's session, and returns at once, as StarterClient's
// Start does, with the same lifetime: Process.Close, the end of ctx,
// the client's Close and the session's loss each end it and everything
// it started. Its standard input and output carry the MCP stream, and
// its standard error, when stderr is set, is streamed there as
// Command.Stream is for Start. The served workspace starts it, so what
// an agentworkspace.Offering's OutputFilter keeps out of its output is
// kept out before it is sent, and its secrets are never told to this
// side. A name the served workspace does not offer is an error that
// errors.Is agentworkspace.ErrNoServer.
func (s *ServerClient) StartServer(ctx context.Context, name string, stderr io.Writer) (agentworkspace.Process, error) {
	return s.Client.startServer(ctx, name, stderr)
}

// Start is StarterClient's.
func (s *StarterServerClient) Start(ctx context.Context, cmd agentworkspace.Command) (agentworkspace.Process, error) {
	return s.Client.start(ctx, cmd)
}

// Servers is ServerClient's.
func (s *StarterServerClient) Servers(ctx context.Context) ([]agentworkspace.Server, error) {
	return s.Client.servers(ctx)
}

// StartServer is ServerClient's.
func (s *StarterServerClient) StartServer(ctx context.Context, name string, stderr io.Writer) (agentworkspace.Process, error) {
	return s.Client.startServer(ctx, name, stderr)
}

func (c *Client) servers(ctx context.Context) ([]agentworkspace.Server, error) {
	if c.isClosed() {
		return nil, fmt.Errorf("remote: list servers: %w", fs.ErrClosed)
	}
	ctx, done := c.with(ctx)
	defer done()
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var list serverList
	if err := c.call(ctx, http.MethodGet, routeServers, nil, nil, &list); err != nil {
		return nil, fmt.Errorf("remote: list servers: %w", err)
	}
	out := make([]agentworkspace.Server, 0, len(list.Servers))
	for _, sv := range list.Servers {
		if !agentworkspace.ValidServerName(sv.Name) {
			return nil, fmt.Errorf("remote: list servers: the workspace offers a server called %q, which agentworkspace.ValidServerName refuses", bounded(sv.Name))
		}
		out = append(out, agentworkspace.Server{Name: sv.Name, Description: sv.Description})
	}
	return out, nil
}

func (c *Client) startServer(ctx context.Context, name string, stderr io.Writer) (agentworkspace.Process, error) {
	if !agentworkspace.ValidServerName(name) {
		// No server is called so, and the name goes in a path.
		return nil, fmt.Errorf("remote: start server %q: %w", bounded(name), agentworkspace.ErrNoServer)
	}
	return c.launch(ctx, "start server "+name, "/servers/"+name+"/start", stderr, func(key string) any {
		return serverStartRequest{Stderr: stderr != nil, Key: key}
	})
}
