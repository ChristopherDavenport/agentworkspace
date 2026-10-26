// Package remote serves an agentworkspace.Workspace over HTTP, and is
// one at the other end: NewHandler serves any workspace, and Dial
// returns a workspace whose files, writes, environment, Exec and, when
// the served workspace is an agentworkspace.Starter, Start are the
// served one's, and, when it is an agentworkspace.ServerProvider, so
// are the MCP servers it offers. It imports the standard library
// alone.
//
// It is the primitive cut: every file operation and process crosses
// the wire on its own, and the tools, and the policy in front of them,
// run on the client's side. A sandbox that should run the tools
// themselves, and answer for each call, is cut at the tool calls
// instead (dax's executor, over MCP); this package is then what such
// an executor may run over inside the sandbox, or what a container's
// workspace serves.
//
// # Wire
//
// Every route is under /v1 of the base URL. Bodies are JSON but for a
// file's content and a process's pipes, which are raw bytes streamed
// as they come. A name is a query's path, an fs.ValidPath name
// relative to the served Root; both halves refuse any other, one that
// leaves as agentworkspace.ErrOutside.
//
//	GET    /v1/info                              read or servers  root, the served descriptor, links, the bounds; Start with exec, Servers with servers, Env with exec
//	GET    /v1/fs/stat?path=N                    read
//	GET    /v1/fs/lstat?path=N                   read     unsupported when the served FS cannot read links
//	GET    /v1/fs/readlink?path=N                read     ditto
//	GET    /v1/fs/readdir?path=N                 read     at most the listing bound of entries
//	GET    /v1/fs/file?path=N                    read     a regular file's content, streamed
//	PUT    /v1/fs/file?path=N&perm=M             write    the body is the content, at most the write bound; perm's permission bits alone
//	DELETE /v1/fs/file?path=N                    write
//	POST   /v1/exec                              exec     NDJSON frames back
//	GET    /v1/servers                           servers  -> {servers: [{name, description}]}, in the served order; unsupported when it is no ServerProvider
//	POST   /v1/sessions                          either   -> {id, leaseMs}
//	POST   /v1/sessions/{s}/renew                either
//	DELETE /v1/sessions/{s}                      either   ends every process it started
//	POST   /v1/sessions/{s}/processes            exec     {args, dir, env, timeoutMs, stderr, key} -> {id}, the key
//	POST   /v1/sessions/{s}/servers/{n}/start    servers  {stderr, key} -> {id}, the key: the server called n, a process of the session
//	POST   /v1/sessions/{s}/processes/{p}/stdin  its own  the body, streamed to its input; its end is end of file
//	GET    /v1/sessions/{s}/processes/{p}/stdout its own  its output, streamed
//	GET    /v1/sessions/{s}/processes/{p}/stderr its own  its standard error, when the start asked for it
//	GET    /v1/sessions/{s}/processes/{p}/wait   its own  a long poll -> {status, ended?, error?}, the same each time
//	POST   /v1/sessions/{s}/processes/{p}/signal its own  {signal: "TERM"}, a name: HUP INT QUIT KILL USR1 USR2 PIPE ALRM TERM CONT STOP TSTP WINCH
//	POST   /v1/sessions/{s}/processes/{p}/cancel its own  ends it as its context's end does
//	DELETE /v1/sessions/{s}/processes/{p}        its own  ends it as Process.Close does -> {status, ...}
//
// A session's routes take exec or servers, either one ("either"). A
// process's routes take the scope it was started under ("its own"):
// exec for a command's, servers for a server's, so a principal holding
// one of them is refused a process the other started (ErrForbidden),
// though both are in its session. A session belongs to the principal
// that opened it, by Name, and not to a scope: its renewal and its
// close take either scope, and its close, or its lapse, ends every
// process in it, whichever scope started each. A Name should therefore
// carry one set of scopes; given several, any of its tokens renews and
// closes the sessions the Name opens. Info's Start and
// Servers each say what the asking principal may start here: Start
// that the served workspace is a Starter and the principal holds exec,
// Servers that it is a ServerProvider and the principal holds servers.
//
// A body is application/json, or application/octet-stream for a
// file's content and a process's input, and a route refuses any other
// type (415). Exec's reply is NDJSON (application/x-ndjson), one frame
// a line: {"data": base64} for Command.Stream's output as it arrives,
// at most 32 KiB of it a frame, when the request asked for a stream,
// then one {"output": {...}} or one {"error": {...}}, which is the
// last. A refusal before the command runs is an HTTP error. A start
// carries a key the client chose at random, which becomes the
// process's id: a start sent again is answered with the same process,
// one whose reply was lost is closed by its key, and a key whose
// process closed starts nothing again; a server's start is keyed so
// too, and a key names one start, of one command or one server. Every error is {"error": {"code", "message"}};
// the code is the sentinel's (agentworkspace.ErrOutside, fs.ErrNotExist,
// fs.ErrExist, fs.ErrPermission, fs.ErrInvalid, errors.ErrUnsupported,
// os.ErrProcessDone, fs.ErrClosed, agentworkspace.ErrNoServer, and this
// package's own), which the
// client maps back, so errors.Is holds on the client as it does on the
// served workspace. A mapped error's message is the system's or the
// sentinel's words, never the served error's own text, which may name
// a path on the server's machine; any other error's is its text, the
// served workspace's words, at most MaxErrorBytes.
//
// # Scopes
//
// This package ships no authentication: it is the capability layer,
// and who a request is from is the program's to decide. Every request
// is put to the Authenticator given with WithAuthenticator, the
// program's hook, whatever its route, which answers with the
// Principal it is from and the scopes it holds; without one, and
// without WithInsecureNoAuth, every request is refused. A principal's
// Name is what keeps its sessions from another's, and one with an
// empty Name is refused as unauthenticated. read covers the info route
// and the file system's reads; write, WriteFile and Remove; exec,
// Exec, a command's start, the sessions and the processes it started,
// and the environment, which the info route gives a principal without
// exec none of: such a client's Env is empty and it is no Starter. exec
// is never implied by read, write or servers.
//
// servers covers the MCP servers a served agentworkspace.ServerProvider
// offers: their list, a server's start, the sessions they run in and
// the routes of the processes it started so, and nothing else. It does
// not imply exec, nor exec it, and it needs no read: the info route
// answers it too, with nothing but the root's last element (Root, and
// the served descriptor's, are not the host's path to a principal
// without read), the descriptor, the bounds and that servers are
// offered. A principal holding servers
// alone dials, lists and starts the offered servers and talks to them
// over their pipes, and reads no file (its client's FS, WriteFile and
// Remove are ErrForbidden), runs no command (no Exec, no Start, no
// process exec started) and is told no environment; its client is an
// agentworkspace.ServerProvider and no Starter. That is how a host
// grants a client its servers and nothing else, and a server that holds
// secrets is offered safely only to a principal holding servers alone.
// exec exposes its environment: an agentworkspace.Offering embeds its
// Starter, so a client that may run commands as the servers' user reads
// a running server's environment from /proc (see agentworkspace.Offer).
// write defeats any filter: a client that may write the workspace can
// change the server's code, config or PATH wherever they lie in it, and
// have the server print its secret in an encoding the filter does not
// know. read reads whatever a server keeps in the workspace. A server is started by the served workspace's
// StartServer, on the serving side, so an Offering's OutputFilter reads
// its output there, before a byte of it is sent, and the secrets it
// names never cross the wire.
//
// The client sends no credentials of its own: the program's go
// in the http.Client it gives WithHTTPClient (a Transport that sets
// the Authorization header, a client certificate), and nothing of them
// is put in a URL, an error or a Descriptor. TLS, issuing and checking
// credentials and deciding by path are the program's: the handler is
// a plain http.Handler it mounts.
//
// This API has no browser clients, and a page the user opens must not
// reach it: every handler refuses a request carrying an Origin, or a
// Sec-Fetch-Site other than same-origin or none, and a body of another
// type than its route's, which a page cannot send without a preflight
// nothing answers. One made WithInsecureNoAuth, for a loopback
// listener the program alone dials, answers only a Host that is
// localhost or a loopback address (127.0.0.0/8, ::1, any port), so a
// name a page rebinds to 127.0.0.1 is refused too.
//
// # Lifetimes
//
// HTTP has no connection to end with the client, so the client opens a
// session at its first Start or StartServer, and renews it every third
// of its lease
// (DefaultLease); a Dial takes nothing on the server. A session that
// goes a lease unrenewed lapses, and Client.Close closes it; either
// way the server closes every process the session started. A client
// that goes a whole lease without a renewal that succeeded takes the
// session as lost too: its processes' Wait returns -1 and an error
// that is ErrNoSession, their pipes fail with it, and the next start
// opens another session. A started process, a server among them, lives
// no longer than its owner: Process.Close, the end of Start's context
// or StartServer's (the client asks the server to end it as its context's end would),
// Command.Timeout (kept by the server) and Client.Close each end it and
// everything it started. Exec's process lives no longer than its
// request. Handler.Close ends every session's processes and every Exec,
// and returns once they have ended. Session and process ids are random (crypto/rand's Text), and
// a process is reached only through the session that started it, by
// the principal that opened that: another's reads as no such session.
//
// # Bounds
//
// A JSON body is at most MaxRequestBytes; a WriteFile's data and an
// Exec's stdin at most the write bound (DefaultMaxWriteBytes,
// WithMaxWriteBytes); a listing at most DefaultMaxDirEntries entries
// (WithMaxDirEntries), of which the server reads one past the bound
// and no more; a handler holds at most DefaultMaxSessions sessions
// (WithMaxSessions), a principal at most
// DefaultMaxSessionsPerPrincipal of them
// (WithMaxSessionsPerPrincipal), and a session at most
// DefaultMaxProcesses processes (WithMaxProcesses), its commands and
// its servers together; a handler serves at
// most DefaultMaxTransfers Execs and reads and writes of a file's
// content at once (WithMaxTransfers); an Exec without a
// Stream gives back at most DefaultMaxExecOutputBytes of each stream
// (WithMaxExecOutputBytes), and past it the command has run and Exec
// is ErrLimit, its output not sent (the served workspace's own
// buffering of it is its own); a process's standard error is held
// StderrBytes deep for a client behind on it, and a Command.Stream that
// takes no more is left behind five seconds after the process ends; a
// client reads at most MaxReplyBytes of a JSON reply, and at most what
// the server's bounds allow of one of Exec's frames. Over a bound is
// ErrLimit. The info
// route tells the client the bounds the server keeps, and the client
// sends no more.
package remote
