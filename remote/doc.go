// Package remote serves an agentworkspace.Workspace over HTTP, and is
// one at the other end: NewHandler serves any workspace, and Dial
// returns a workspace whose files, writes, environment, Exec and, when
// the served workspace is an agentworkspace.Starter, Start are the
// served one's. It imports the standard library alone.
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
//	GET    /v1/info                              read   root, the served descriptor, Start, links, the bounds; Env with exec
//	GET    /v1/fs/stat?path=N                    read
//	GET    /v1/fs/lstat?path=N                   read   unsupported when the served FS cannot read links
//	GET    /v1/fs/readlink?path=N                read   ditto
//	GET    /v1/fs/readdir?path=N                 read   at most the listing bound of entries
//	GET    /v1/fs/file?path=N                    read   a regular file's content, streamed
//	PUT    /v1/fs/file?path=N&perm=M             write  the body is the content, at most the write bound
//	DELETE /v1/fs/file?path=N                    write
//	POST   /v1/exec                              exec   NDJSON frames back
//	POST   /v1/sessions                          exec   -> {id, leaseMs}
//	POST   /v1/sessions/{s}/renew                exec
//	DELETE /v1/sessions/{s}                      exec   ends every process it started
//	POST   /v1/sessions/{s}/processes            exec   {args, dir, env, timeoutMs, stderr} -> {id}
//	POST   /v1/sessions/{s}/processes/{p}/stdin  exec   the body, streamed to its input; its end is end of file
//	GET    /v1/sessions/{s}/processes/{p}/stdout exec   its output, streamed
//	GET    /v1/sessions/{s}/processes/{p}/stderr exec   its standard error, when the start asked for it
//	GET    /v1/sessions/{s}/processes/{p}/wait   exec   a long poll -> {status, ended?, error?}, the same each time
//	POST   /v1/sessions/{s}/processes/{p}/signal exec   {signal: "TERM"}, a name: HUP INT QUIT KILL USR1 USR2 PIPE ALRM TERM CONT STOP TSTP WINCH
//	POST   /v1/sessions/{s}/processes/{p}/cancel exec   ends it as its context's end does
//	DELETE /v1/sessions/{s}/processes/{p}        exec   ends it as Process.Close does -> {status, ...}
//
// Exec's reply is NDJSON (application/x-ndjson), one frame a line:
// {"data": base64} for Command.Stream's output as it arrives, when the
// request asked for a stream, then one {"output": {...}} or one
// {"error": {...}}, which is the last. A refusal before the command
// runs is an HTTP error. Every error is {"error": {"code", "message"}};
// the code is the sentinel's (agentworkspace.ErrOutside, fs.ErrNotExist,
// fs.ErrExist, fs.ErrPermission, fs.ErrInvalid, errors.ErrUnsupported,
// os.ErrProcessDone, fs.ErrClosed, and this package's own), which the
// client maps back, so errors.Is holds on the client as it does on the
// served workspace. A mapped error's message is the system's or the
// sentinel's words, never the served error's own text, which may name
// a path on the server's machine; any other error's is its text, the
// served workspace's words, at most MaxErrorBytes.
//
// # Scopes
//
// Every request is authenticated, whatever its route, by the
// Authenticator given with WithAuthenticator (BearerTokens over static
// tokens); without one, and without WithInsecureNoAuth, every request
// is refused. read covers the info route and the file system's reads;
// write, WriteFile and Remove; exec, Exec, the sessions and the
// processes, and the environment, which the info route gives a
// principal without exec none of: such a client's Env is empty and it
// is no Starter. exec is never implied by read or write. A client
// sends its token as a bearer token, and puts it in no URL, error or
// Descriptor. TLS, issuing tokens and deciding by path are the
// program's: the handler is a plain http.Handler it mounts.
//
// # Lifetimes
//
// HTTP has no connection to end with the client, so the client opens a
// session at Dial, when it may start processes, and renews it every
// third of its lease (DefaultLease). A session that goes a lease
// unrenewed lapses, and Client.Close closes it; either way the server
// closes every process the session started. A started process lives no
// longer than its owner: Process.Close, the end of Start's context
// (the client asks the server to end it as its context's end would),
// Command.Timeout (kept by the server) and Client.Close each end it and
// everything it started. Exec's process lives no longer than its
// request. Session and process ids are random (crypto/rand's Text), and
// a process is reached only through the session that started it, by
// the principal that opened that: another's reads as no such session.
//
// # Bounds
//
// A JSON body is at most MaxRequestBytes; a WriteFile's data and an
// Exec's stdin at most the write bound (DefaultMaxWriteBytes,
// WithMaxWriteBytes); a listing at most DefaultMaxDirEntries entries
// (WithMaxDirEntries); a handler holds at most DefaultMaxSessions
// sessions (WithMaxSessions) and a session at most DefaultMaxProcesses
// processes (WithMaxProcesses); a process's standard error is held
// StderrBytes deep for a client behind on it; a client reads at most
// MaxReplyBytes of a JSON reply. Over a bound is ErrLimit. The info
// route tells the client the bounds the server keeps, and the client
// sends no more.
package remote
