// Package mcpworkspace carries an agentworkspace.Workspace's processes
// and files over an MCP connection, for a program whose tools act in a
// workspace at the other end of one (a sandbox, a container, another
// machine): the server half answers for a workspace, and the client half
// is that workspace's Starter and read-only file system on this side.
// The protocol is package wire's: custom JSON-RPC methods for the
// processes, never tools, and one resource template for the files.
//
// It rides a connection the program already has, rather than dialing one
// of its own, so a program that relays one MCP stream (stdio through a
// container's exec, a host in front of a sandbox) carries the workspace
// on it too. What the connection is, how it is authenticated, and
// whether it is made again after it is lost are the program's: the
// server half registers on an *mcp.Server it is handed, and the client
// half sends on the session a Conn or a Source names.
//
// The name: the SDK's package is mcp, as agenttool's adapters are
// mcpclient and mcpserver, so this one is mcpworkspace and can be
// imported beside both without an alias.
//
// # Server
//
// ServeFiles serves a file system read-only under wire.FilesURITemplate.
// NewProcesses answers the process methods for a workspace that is an
// agentworkspace.Starter, keeping each MCP session's processes apart and
// ending them with it, and the server methods for one that is an
// agentworkspace.ServerProvider.
//
// # Client
//
// RegisterClient adds the process and server methods to an *mcp.Client
// before it connects. Start starts a process over a Conn; NewFS reads
// the files over a Source; ListServers and StartServer reach the
// servers the workspace offers, and Servers is that workspace as an
// agentworkspace.ServerProvider.
//
// # Transports
//
// CommandTransport and ServerTransport are mcp.Transports to a stdio
// MCP server started in a workspace, from a command or by the name the
// workspace offers it under: on this machine, or at the other end of a
// connection through the client half.
package mcpworkspace
