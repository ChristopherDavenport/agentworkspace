package mcpworkspace

// MCP transports over a workspace's processes: a stdio server started
// in a workspace, from a command or by the name the workspace offers it
// under, spoken to over the process's pipes.

import (
	"context"
	"io"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// CommandTransport is an mcp.Transport to a stdio MCP server started
// from cmd in ws, at cmd.Dir with ws's environment and cmd.Env, its
// standard error to cmd.Stream: what mcp.CommandTransport is for this
// machine, for any workspace that can start a process, a remote one
// over Start among them. Each Connect starts the server anew, so a
// client that connects again after the server ended (agenttool's
// mcpclient.WithReconnect) gets a new one. The server lives as long as
// the connection, not the context Connect is given, as a server the
// SDK's command transport starts does: closing the connection ends it,
// its input closed first, as the stdio transport's shutdown asks, and
// so does closing the workspace.
func CommandTransport(ws agentworkspace.Starter, cmd agentworkspace.Command) mcp.Transport {
	return processTransport{start: func(ctx context.Context) (agentworkspace.Process, error) {
		return ws.Start(ctx, cmd)
	}}
}

// ServerTransport is an mcp.Transport to the server called name that sp
// offers, its standard error to stderr (nil discards it), with
// CommandTransport's lifetime: each Connect starts it anew, and it ends
// with the connection. sp may be a workspace here or one elsewhere
// (Servers).
func ServerTransport(sp agentworkspace.ServerProvider, name string, stderr io.Writer) mcp.Transport {
	return processTransport{start: func(ctx context.Context) (agentworkspace.Process, error) {
		return sp.StartServer(ctx, name, stderr)
	}}
}

// processTransport starts a process on each Connect and speaks MCP over
// its pipes.
type processTransport struct {
	start func(context.Context) (agentworkspace.Process, error)
}

func (t processTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	p, err := t.start(context.WithoutCancel(ctx))
	if err != nil {
		return nil, err
	}
	pipes := processPipes{p}
	return (&mcp.IOTransport{Reader: pipes, Writer: pipes}).Connect(ctx)
}

// processPipes is a started process's output to read and input to
// write. Closing it, as the connection does when it closes, ends the
// process (Process.Close: its input closed, a moment to leave, then
// terminated, then killed).
type processPipes struct{ p agentworkspace.Process }

func (r processPipes) Read(b []byte) (int, error)  { return r.p.Stdout().Read(b) }
func (r processPipes) Write(b []byte) (int, error) { return r.p.Stdin().Write(b) }
func (r processPipes) Close() error                { return r.p.Close() }
