package mcpworkspace

// The client half of the servers a workspace offers: listed and started
// over a connection, as an agentworkspace.ServerProvider.

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ListServers lists the servers the workspace at the other end of c
// offers. A server that does not answer the method offers none: the
// list is empty and the error nil. Only c.Session is used.
func ListServers(ctx context.Context, c Conn) ([]agentworkspace.Server, error) {
	if c.Session == nil {
		return nil, errors.New("mcpworkspace: a Conn with no session")
	}
	sctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	res, err := mcp.CallCustomMethod[*ServersParams, *ServersResult](sctx, c.Session, wire.MethodServersList, &ServersParams{})
	if err != nil {
		if notFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list servers: %w", err)
	}
	out := make([]agentworkspace.Server, 0, len(res.Servers))
	for _, s := range res.Servers {
		if !agentworkspace.ValidServerName(s.Name) {
			return nil, fmt.Errorf("list servers: the workspace offers a server called %q, which agentworkspace.ValidServerName refuses", s.Name)
		}
		out = append(out, agentworkspace.Server{Name: s.Name, Description: s.Description})
	}
	return out, nil
}

// StartServer starts the server called name in the workspace at the
// other end of c and returns at once, as Start does for a command, with
// the same lifetime and the same Conn rules: its standard input and
// output carry the MCP stream, and its standard error, when stderr is
// set, is written there as it arrives. A name the workspace does not
// offer, one that offers none, or a name agentworkspace.ValidServerName
// refuses, which no workspace offers and which is not sent, is an
// error that errors.Is agentworkspace.ErrNoServer.
func StartServer(ctx context.Context, c Conn, name string, stderr io.Writer) (agentworkspace.Process, error) {
	if !agentworkspace.ValidServerName(name) {
		return nil, fmt.Errorf("mcpworkspace: server name %q: agentworkspace.ValidServerName refuses it: %w", name, agentworkspace.ErrNoServer)
	}
	if err := checkConn(c); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	res, err := mcp.CallCustomMethod[*ServerStartParams, *StartResult](sctx, c.Session, wire.MethodServersStart, &ServerStartParams{ServerStartArgs: wire.ServerStartArgs{
		Name: name, Stderr: stderr != nil,
	}})
	if err != nil {
		var we *jsonrpc.Error
		switch {
		case notFound(err):
			return nil, fmt.Errorf("start server %s: the workspace offers no servers: %w", name, agentworkspace.ErrNoServer)
		case errors.As(err, &we) && we.Code == wire.CodeNoServer:
			return nil, fmt.Errorf("start server %s: %w", name, agentworkspace.ErrNoServer)
		}
		return nil, fmt.Errorf("start server %s: %w", name, err)
	}
	return attach(ctx, c, res.ID, stderr), nil
}

// notFound reports whether err is the other end's word that it does not
// answer the method.
func notFound(err error) bool {
	var we *jsonrpc.Error
	return errors.As(err, &we) && we.Code == jsonrpc.CodeMethodNotFound
}

// Servers is the workspace at the other end of c as an
// agentworkspace.ServerProvider: ListServers and StartServer over c,
// for a program whose connection does not change. A program that makes
// its connection again after it is lost implements ServerProvider over
// the two functions itself, with the Conn of the moment.
func Servers(c Conn) agentworkspace.ServerProvider { return connServers{c} }

type connServers struct{ c Conn }

func (s connServers) Servers(ctx context.Context) ([]agentworkspace.Server, error) {
	return ListServers(ctx, s.c)
}

func (s connServers) StartServer(ctx context.Context, name string, stderr io.Writer) (agentworkspace.Process, error) {
	return StartServer(ctx, s.c, name, stderr)
}
