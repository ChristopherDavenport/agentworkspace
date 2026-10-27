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
// URL, an error or a log, and follows no redirect of the upgrade,
// whatever that client allows, so the credentials never leave the
// wss:// URL they were given for. An Enrollment that cannot judge an
// upgrade yet says ErrTryLater, which the Hub answers 503 and the host
// dials again on. A refused upgrade's 401 names the application's
// scheme in its WWW-Authenticate when the application gives one
// (WithChallenge), and carries none otherwise.
//
// What the server may do is the workspace end's to say: WithScopes is
// required, and every request that comes through the tunnel is from
// one principal, named after the server's host, that holds those scopes
// and no others. A workspace served with read and write alone is no
// Starter at the server, and its Exec is remote.ErrForbidden. A
// workspace that offers MCP servers (an agentworkspace.ServerProvider,
// such as an agentworkspace.Offering) is one at the server when the
// scopes hold servers (remote.ScopeServers); served with servers alone
// it is a ServerProvider and no Starter, so the server may start the
// offered servers and do nothing else (no file, no command), which is
// how a host offers servers that hold secrets (see
// agentworkspace.Offer). A
// server's output is read through its Offering's OutputFilter at the
// workspace end, before it enters the tunnel.
//
// # Proving the hub
//
// The workspace end sends its credential to whatever answers the
// server URL, and serves it the scopes it was given. Over wss:// the
// hub is proved by TLS. Over a ws:// loopback URL, as behind an ssh -R
// forward, it is not: any local user who binds the port first
// receives the credential, and is served a shell. The proof is the
// application's, as its authentication is, and the package has a hook
// at each end for it: the Hub adds the application's headers to the
// 101 (WithProofHeader), made for the request its enrollment named,
// and the workspace end checks them (WithHubProof) after the 101 and
// before it serves anything; a hub that fails is a refusal
// (ErrHubProof), and nothing has been served to it.
//
// The proof authenticates the hub at the handshake; it does not bind
// the connection to it. An impostor that can reach the real hub itself
// relays the upgrade to it, at once, and returns the hub's answer,
// which passes; it then sits between the two on a plain connection. So
// a proof holds against an impostor that cannot reach the hub: the ssh
// -R case, where the hub is reached only through the forward the
// impostor took the port of (with ExitOnForwardFailure, so ssh does not
// go on without it). Where the impostor can reach the hub, use wss://,
// whose TLS binds the connection.
//
// Over ws://, whatever the upgrade carries reaches an impostor before
// the proof is checked, so it carries nothing the hub would take from
// it later: no bearer token, no key, only a proof good for one upgrade
// at one hub. An HMAC keyed by a secret both ends hold, never sent,
// over a nonce the hub issues, is one such proof; the package's
// HubProof example has it in code.
//
//   - Before each upgrade the workspace end asks the hub for a nonce,
//     with a plain GET of the hub's URL that the program's handler
//     beside the Hub answers (the Hub takes upgrades alone). The hub
//     keeps nothing for it: the nonce is the time it was issued, on the
//     hub's monotonic clock from a random offset (so it does not say
//     when the hub started), random bytes, and the hub's MAC of both
//     under a secret it makes at its start, never the shared key.
//   - The upgrade carries the nonce, an instance id the workspace end
//     makes once for its run, and its MAC under the key of a label, the
//     workspace's name as the hub enrolls it, the nonce and the
//     instance id. The enrollment takes it only over a nonce the hub
//     issued (its own MAC), within a short lifetime by the hub's clock
//     (30 seconds, say; the other host's clock does not matter), and
//     once: it keeps the nonces it has taken until they expire, a
//     bounded number, and none it has not.
//   - The hub's 101 carries its MAC, under another label, of the same
//     name, nonce and instance id (WithProofHeader), which the
//     workspace end checks before it serves anything (WithHubProof).
//
// What that proves, with an impostor holding the port behind ssh -R: it
// learns no key; it cannot answer with the MAC it was sent, whose label
// is the workspace's; and the upgrade it captured is over a nonce it
// handed out, which the hub did not issue, so the hub refuses it when
// it is replayed through the forward once that is back. Nor does a
// proof for one hub pass at another that holds the same key: the name,
// and each hub's own secret, are in it.
//
// What it leaves. There is no channel binding: an impostor that can
// reach the hub while it holds the port relays the GET and the upgrade
// at once and passes, as above. And one that fetched nonces from the
// hub while the forward was up, then takes the port after a drop, can
// hand them out and replay each upgrade it collects within that
// nonce's lifetime. To keep that to as few upgrades as can be, the
// workspace end:
//
//   - stops, rather than dial again, at a hub that fails the proof,
//     at the first dial and after one that connected alike. Serve
//     returns ErrHubProof from a first dial, but tries again within the
//     lease after a connection that served, so the program ends Serve
//     from its WithHubProof hook, by cancelling Serve's context;
//   - stops, too, after a few upgrades in a row (three, say) that went
//     out over a nonce and got no answer a hub gives (a 101, 401, 403
//     or 503): a hub does not take an upgrade and drop it, an impostor
//     collecting them does;
//   - at the nonce's GET, dials again on no answer, or on one a hub
//     gives for a while (a 5xx, 429 or 408), and stops at one that
//     rules a hub out (a 200 without a nonce, a redirect, which it does
//     not follow, a 401, 403 or 404).
//
// After such a stop, waiting out the nonce's lifetime, or restarting the
// hub (a new secret, so no nonce it issued is known), before the
// forward comes back defeats whatever upgrade an impostor captured. The
// key was never sent, so it need not be rotated unless it may have
// leaked some other way.
//
// # Reconnecting
//
// Serve dials again when the connection drops, after a jittered wait
// that doubles from DefaultRedialMin to DefaultRedialMax (WithRedial)
// and starts over after a connection that lasted. Once it has
// connected, an upgrade refused as unauthenticated or forbidden is
// tried again too, for as long as the sessions' lease since the drop:
// a hub that restarts or a credential that rotates does not end the
// processes; past that they have lapsed, and Serve returns the
// refusal. A first upgrade refused so ends Serve at once. It serves every
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
// that is connected replaces the first, which is closed, once the new
// one has answered a request over HTTP/2 within HandshakeTimeout (a
// host that upgrades and never speaks HTTP/2 replaces nothing). Two
// hosts that both dial as one name take the name from each other at
// every redial, so an enrollment names each host apart. A Workspace
// keeps the info of the host it was dialled to (its Root, Env, bounds
// and whether it is a Starter or a ServerProvider); after another host
// takes the name, a program dials again.
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
// ConnWindow, all MaxStreams streams' windows, of all its bodies
// unread, so a body nobody reads stalls only itself. Serve gives remote
// bounds of its own (DefaultSessions, DefaultProcesses,
// DefaultTransfers) that keep the streams the server can hold open
// under MaxStreams with room to spare, so no output left unread and no
// number of open streams the handler allows keeps a renewal, a Wait, a
// Signal, a Close or a Start from getting through; MaxStreams' doc has
// the arithmetic, and Serve refuses remote bounds (WithHandlerOptions)
// that would break it. An upgrade, and the wait for HTTP/2 to start on it,
// take at most HandshakeTimeout. Serve's ctx ending closes the
// connection and the handler, which ends every process the server
// started; Hub.Close closes every connection.
//
// # As remote
//
// What remote does not carry, the tunnel does not. Everything remote
// does carry, it carries as it is: its routes, scopes, sessions,
// bounds, error codes and browser checks hold inside the tunnel as they
// do over HTTP, the offered servers among them.
package tunnel
