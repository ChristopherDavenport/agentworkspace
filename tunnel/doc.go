// Package tunnel serves an agentworkspace.Workspace to a server from a
// host the server cannot dial: one behind NAT or a firewall. The
// workspace's host dials out, and the server uses the workspace
// through the connection as it would one remote serves, because it is
// one: the tunnel carries remote's wire unchanged.
//
// # The picture
//
//	workspace host                                   server
//	Serve(ctx, "wss://hub/path", ws) ── WebSocket ──▶ Hub, mounted at /path
//	HTTP/2 server: remote.NewHandler(ws)  ◀── h2c ──  HTTP/2 client: remote.Dial
//
// The workspace end is the WebSocket's client and the HTTP server: it
// serves remote.NewHandler(ws) over HTTP/2 without TLS, spoken with
// prior knowledge, on the WebSocket's bytes. The server end is the
// WebSocket's server and the HTTP client: Hub.Workspace is remote.Dial
// through an HTTP/2 client on that connection. One WebSocket carries
// every request and stream of the workspace's, multiplexed; TLS, when
// there is any, is the WebSocket's (wss://; ws:// is dialled to a
// loopback host alone).
//
// # Who may do what
//
// The package ships no authentication: it is the capability layer, and
// the application authenticates. At the server end, the application's
// Enrollment says which workspace an upgrade is from, or refuses it,
// before the upgrade is answered; a Hub without one refuses every
// upgrade, as does a Hub for any upgrade carrying an Origin (no browser
// is a client). At the workspace end, the application's credentials go
// in the HTTP client the upgrade is made through (WithHTTPClient) or a
// header made for each dial (WithHeader); this package puts none in a
// URL, an error or a log.
//
// What the server may do is the workspace end's to say: WithScopes is
// required, and every request that comes through the tunnel is from
// one principal, named after the server's host, that holds those scopes
// and no others. A workspace served with read and write alone is no
// Starter at the server, and its Exec is remote.ErrForbidden.
//
// # Reconnecting
//
// Serve dials again when the connection drops, after a jittered wait
// that doubles from DefaultRedialMin to DefaultRedialMax (WithRedial)
// and starts over after a connection that lasted. It serves every
// connection with one remote.Handler, so the sessions the server opened
// and the processes it started outlive a drop: the server's client
// renews its session through the next connection, and a process's Wait,
// Signal and Close work again on it. What was in flight when the
// connection dropped fails and is not sent again: a request, a wait
// (which the client polls again), and a process's open pipes. A pipe is
// one stream, which remote does not resume, so the standard input,
// output and standard error streamed before a drop fail at the server
// and are closed at the workspace (the process reads the end of its
// input, and a write to its output may fail); a pipe first opened
// after the reconnect works. A drop longer than the session's lease
// (remote.DefaultLease, remote.WithLease through WithHandlerOptions)
// ends the session's processes, and the server's client sees
// remote.ErrNoSession, as over HTTP.
//
// At the server end a Workspace keeps working across reconnects: a
// request made while its workspace is not connected waits for the next
// connection as long as its context allows, and is ErrNotConnected past
// that. remote bounds the requests that take no context by its timeout
// (remote.WithTimeout, 30 seconds), so while a workspace is away its
// file operations and a process's Close wait that long before they
// fail; Hub.Close fails them at once. A second connection for a name
// that is connected replaces the first, which is closed: two hosts
// that both dial as one name take the name from each other at every
// redial, so an enrollment names each host apart.
//
// # Liveness and bounds
//
// Both ends ping over HTTP/2 after DefaultPingInterval without a frame
// and close the tunnel when DefaultPingTimeout passes without the
// answer (WithPing), so a dead tunnel is noticed at both ends within
// the two together, and the workspace end dials again. A WebSocket
// message is at most DefaultMaxMessageBytes (WithMaxMessageBytes): a
// longer one read ends the tunnel, and each end writes in messages
// under its bound. A Hub holds at most DefaultMaxWorkspaces workspaces
// connected (WithMaxWorkspaces). A tunnel carries at most MaxStreams
// requests at once, and holds at most StreamWindow of one body and
// ConnWindow of all its bodies unread; an upgrade and its HTTP/2
// handshake take at most HandshakeTimeout. Serve's ctx ending closes the
// connection and the handler, which ends every process the server
// started; Hub.Close closes every connection.
//
// # Not carried
//
// What remote does not carry, the tunnel does not: the MCP servers a
// workspace offers (agentworkspace.ServerProvider) are not served.
// Everything remote does carry, it carries as it is: its routes,
// scopes, sessions, bounds, error codes and browser checks hold inside
// the tunnel as they do over HTTP.
package tunnel
