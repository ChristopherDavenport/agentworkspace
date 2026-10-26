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
// An HMAC over a nonce, keyed by a secret both ends hold, is one such
// proof. The key itself is never sent, since the upgrade reaches an
// impostor before the proof is checked: the workspace end sends a fresh
// nonce and its own MAC under the key, which the enrollment checks, and
// the hub answers with its MAC of the same nonce. The two MACs are of
// different labels, so an impostor cannot answer with the one it was
// sent; and the enrollment refuses a nonce it has seen, or one too old
// (a time in it), so the workspace's MAC that reached an impostor
// cannot be replayed to the hub.
//
//	// mac is the hex HMAC-SHA256, under key, of label and nonce.
//	mac := func(key []byte, label, nonce string) string {
//		m := hmac.New(sha256.New, key)
//		m.Write([]byte(label + "\x00" + nonce))
//		return hex.EncodeToString(m.Sum(nil))
//	}
//
//	// The workspace end: a nonce for each upgrade, and the hub's
//	// answer checked before anything is served.
//	err := tunnel.Serve(ctx, "ws://127.0.0.1:7000/tunnel", ws,
//		tunnel.WithScopes(remote.ScopeRead, remote.ScopeWrite, remote.ScopeExec),
//		tunnel.WithHeader(func(context.Context) (http.Header, error) {
//			nonce := strconv.FormatInt(time.Now().Unix(), 10) + "." + rand.Text()
//			return http.Header{
//				"X-Workspace": {id},
//				"X-Nonce":     {nonce},
//				"X-Proof":     {mac(key, "workspace", nonce)},
//			}, nil
//		}),
//		tunnel.WithHubProof(func(req *http.Request, resp *http.Response) error {
//			want := mac(key, "hub", req.Header.Get("X-Nonce"))
//			if !hmac.Equal([]byte(resp.Header.Get("X-Hub-Proof")), []byte(want)) {
//				return errors.New("the hub's proof does not match")
//			}
//			return nil
//		}))
//
//	// The hub: the enrollment checks the workspace's MAC (and refuses
//	// a stale or replayed nonce), and the proof answers with the hub's.
//	hub, err := tunnel.NewHub(
//		tunnel.WithEnrollment(enroll),
//		tunnel.WithProofHeader(func(r *http.Request, name string) (http.Header, error) {
//			return http.Header{"X-Hub-Proof": {mac(keyOf(name), "hub", r.Header.Get("X-Nonce"))}}, nil
//		}))
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
// the arithmetic. An upgrade, and the wait for HTTP/2 to start on it,
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
