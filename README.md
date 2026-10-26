# agentworkspace

Where an agent's tools act: a file tree, processes, a working directory
and an environment, behind one interface, so the same read, write, edit
and bash tools run unchanged against a directory on this machine, a
container or a remote runtime. The module imports the standard library
alone, its package `remote` included, which serves a whole workspace
over HTTP; `mcpworkspace`, a module of its own beside it, carries a
workspace's processes and files over an MCP connection, and `tunnel`,
another, serves a workspace behind NAT through a connection its host
dials out.

```go
ws, err := agentworkspace.NewLocal(dir, env) // env: this process's, credentials removed
if err != nil {
	return err
}
defer ws.Close()

data, err := agentworkspace.Read(ws, "go.mod", 1<<20)
err = ws.WriteFile(ctx, "notes/todo.md", []byte("- ship it\n"), 0o644)
out, err := ws.Exec(ctx, agentworkspace.Command{
	Args:    []string{"go", "test", "./..."},
	Timeout: 2 * time.Minute,
})
```

Names are `fs.ValidPath` names relative to `Root`. A name that leaves
the workspace, absolute, climbing out with `..` or through a symbolic
link, is an error that `errors.Is` `ErrOutside`, for reads, writes,
removes and a command's `Dir` alike. Opening never blocks: a FIFO or a
device where a file should be opens, or fails, at once, and `WriteFile`
refuses one. An FS that can tell a link from its target implements
`fs.ReadLinkFS`, so a policy can follow a link inside the workspace
before it decides what a read of it really reads. `Env` is what the
constructor was given, a copy; nil is an empty environment, never the
host's. `Descriptor` (`Kind`, `Ref`, `Root`) is what a session records
about where its tools ran, as agentsession's env entry keeps it.

`Local` is a directory on this machine. Every file operation goes
through an `os.Root`, so a link that leads out is refused rather than
followed. Its processes are not confined: a shell reaches whatever the
user can, and the product's policy is what stands in front of it.
`Exec` runs each command in a process group of its own, so a timeout
or a cancel kills what it started too. `Local` runs on Linux and macOS.

## Long-lived processes

A workspace that can run a process for longer than one call is a
`Starter`. `Start` returns a `Process` with pipes to its standard input
and output, `Wait`, `Signal` and `Close`; its standard error goes to
`Command.Stream`. Its first use is an MCP stdio server running in the
workspace rather than beside the agent:

```go
p, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{
	Args:   []string{"my-mcp-server", "--stdio"},
	Stream: serverLog, // its standard error
})
if err != nil {
	return err // errors.ErrUnsupported when ws cannot start processes
}
defer p.Close()
transport := &mcp.IOTransport{Reader: p.Stdout(), Writer: p.Stdin()}
```

A process lives no longer than its owner. `Close` closes its input and
gives it a moment to leave, as the MCP stdio shutdown asks, then sends
SIGTERM, then SIGKILL; the context's end, `Command.Timeout` and the
workspace's `Close` end it too; each ends everything it started, and
when it exits by itself what it left running goes with it. `Starter` is
an interface of its own, beside `Workspace` as `fs.ReadLinkFS` is beside
`fs.FS`, so a workspace that only stores files, or a runtime that runs
one command at a time, is still a `Workspace`.

## Servers a workspace offers

A workspace may hold MCP servers of its own, such as one installed in a
container's image or one a host runs beside a sandbox: such a workspace
is a `ServerProvider`. `Servers` lists them (a `Server` is a name, as
`ValidServerName` holds it, and a description), and `StartServer` starts
one by name as a `Process` whose pipes carry the MCP stream, with
`Start`'s lifetime. A name it does not offer is `ErrNoServer`. Which
servers a session connects to stays the program's choice. `Offer` makes
one from a `Starter` and a command per server, for a workspace whose
servers are programs installed in it:

```go
ws, err := agentworkspace.Offer(local, []agentworkspace.OfferedServer{
	{Server: agentworkspace.Server{Name: "rag", Description: "the project's index"},
		Command: agentworkspace.Command{Args: []string{"rag-mcp", "--stdio"}}},
})
```

A server given a credential names it in `Secrets`, each a variable its
`Command.Env` sets once, and `WithOutputFilter` has the `Offering` read
that server's standard output and standard error through a filter told
the values, so a server that quotes its own token (an env tool, an
error message) does not hand it to the session. The root imports no
redactor; agentredact's standard-library-only `secrets` module is the
filter it is shaped for:

```go
ws, err := agentworkspace.Offer(local, []agentworkspace.OfferedServer{
	{Server: agentworkspace.Server{Name: "github"},
		Command: agentworkspace.Command{Args: []string{"github-mcp", "stdio"}, Env: []string{"GITHUB_TOKEN=" + tok}},
		Secrets: []string{"GITHUB_TOKEN"}},
}, agentworkspace.WithOutputFilter(func(_ string, values map[string]string, r io.Reader) io.Reader {
	return secrets.New(values).Reader(r, nil) // github.com/ChristopherDavenport/agentredact/secrets
}))
```

`Offer` fails closed: a server that names secrets is refused without a
filter, and so is a secret whose value is empty. The filter is told its
own copy of the values, once for each stream, and the two are read at
once; a filter that gives no reader, or panics, fails the start, and one
that holds standard error back is given a short grace when the server
ends. Standard error is never forwarded in part: when it cannot be
filtered whole and in order (the filter more than a megabyte behind,
so the lag can reach about twice that; the grace past; the filter
failing) it is cut for the rest of the server's life, after one line of
this package's own, `agentworkspace: <server>: standard error cut
(<reason>)`, and nothing of the server's follows. Standard output is
not cut. A secret holding a newline (a PEM key) is not found on its raw
spelling by a filter that works a line at a time, as agentredact's
`secrets` does.

It is a backstop: a value the server encodes otherwise (base64, split
across messages) passes through, and a very short value ("1", "true")
is replaced inside the server's JSON-RPC too, which breaks the
conversation rather than leak. It covers the offered servers' own
output and nothing else: an `Offering` embeds its `Starter`, so its
`Start` and `Exec` run anything unfiltered, as the same user, and a
client of a workspace that runs its commands (one served through
`mcpworkspace.NewProcesses` answers `process.start` as well as
`servers.start`, and `remote` with `exec` runs `Exec` and `Start`) can
read a running server's environment from `/proc/<pid>/environ`, and a
client that may write the workspace can change a server's code, config
or `PATH` there and have it print its secret in an encoding no filter
looks for. A secret-holding server is offered safely only from a
workspace the client can neither run commands in nor write, as dax's
box host does, each server a unit in its own container, or through
`remote` (or `tunnel`) to a principal holding the `servers` scope
alone: `exec` exposes the environment through `/proc`, `write` defeats
any filter.

## Over MCP: mcpworkspace

`github.com/ChristopherDavenport/agentworkspace/mcpworkspace` is for a
program whose tools act in a workspace at the other end of an MCP
connection it already has, such as a sandbox's: the server half answers
for a `Workspace`, and the client half is that workspace's `Starter` and
read-only file system on this side. It rides the program's connection
rather than dialing one, so a program that relays one stream (stdio
through a container's exec) carries the workspace on it too.

```go
// In the sandbox, on each session's server:
mcpworkspace.ServeFiles(srv, ws.FS())
procs := mcpworkspace.NewProcesses(ws) // ws is a Starter
err := procs.Register(srv)

// On this side, before the client connects:
err = mcpworkspace.RegisterClient(client)
// ... then, once it has, with a context that ends with the connection:
done, end := context.WithCancel(context.Background())
p, err := mcpworkspace.Start(ctx, mcpworkspace.Conn{Session: cs, Bounds: wire.DefaultBounds(), Done: done}, cmd)
fsys := mcpworkspace.NewFS(mcpworkspace.SessionSource(cs))

// And to hang up: end Done (or close every process) first, since the
// session's close waits for the requests in flight and a process's wait
// is one for as long as it runs. The server ends the processes when the
// session ends.
end()
cs.Close()
```

Two transports connect a program's MCP client to a stdio server in a
workspace, here or at the other end of the connection, each `Connect`
starting a new one, so a client that connects again after a server
ended gets a fresh server:

```go
// A server the program names a command for, started in the workspace:
t := mcpworkspace.CommandTransport(ws, agentworkspace.Command{Args: []string{"my-mcp-server"}})
// A server the workspace offers, by name; over a connection, the
// workspace there is mcpworkspace.Servers(conn):
t = mcpworkspace.ServerTransport(mcpworkspace.Servers(conn), "rag", serverLog)
```

A server the program reaches on its own, on this machine or at a URL,
needs neither: it is the SDK's command or streamable HTTP transport.
On the server half, `NewProcesses` answers `agentworkspace/servers.list`
and `servers.start` when its workspace is a `ServerProvider`; on the
client half, `ListServers` and `StartServer` call them, and a server
that does not answer them offers none.

The processes are custom JSON-RPC methods (`agentworkspace/process.*`),
never tools, so nothing offers them to a model; the files are one
read-only resource template, `agentworkspace:///{op}{?path}`. Package
`mcpworkspace/wire` names the protocol in the standard library alone,
for a program that only relays or answers the methods. What the
connection is, how it is authenticated and whether it is made again are
the program's.

## Over HTTP: remote

Package `remote` serves any workspace over HTTP and is one at the
other end: every file operation, write, `Exec` and started process
crosses the wire on its own, and the tools and their policy stay on the
client's side (the primitive cut; a sandbox that should run the tools
themselves is cut at the tool calls instead, as dax's executor is).

```go
// The server: a plain http.Handler; TLS, the listener and the
// authentication are the program's. authenticate is the program's own:
// it says who a request is from and what that principal may do.
h, err := remote.NewHandler(ws, remote.WithAuthenticator(func(r *http.Request) (remote.Principal, error) {
	name, err := authenticate(r) // the program's check of its credentials
	if err != nil {
		return remote.Principal{}, err
	}
	return remote.Principal{Name: name, Scopes: []remote.Scope{remote.ScopeRead, remote.ScopeWrite, remote.ScopeExec}}, nil
}), remote.WithChallenge("Bearer")) // a 401's WWW-Authenticate names the program's scheme
defer h.Close() // ends every session's processes
http.Handle("/", h)

// The client: a Workspace, a Starter when the server can start and
// the principal holds exec, and a ServerProvider when the server offers
// MCP servers and the principal holds servers. Its credentials are in
// the program's HTTP client (a Transport that sets the Authorization
// header, or a client certificate).
ws, err := remote.Dial(ctx, "https://host/", remote.WithHTTPClient(hc))
defer ws.Close()
```

The package ships no authentication: it asks the program's
`Authenticator` who each request is from and grants that principal's
scopes, and the client sends whatever the program's `http.Client` sends.
Every request is put to the authenticator, whatever its route; with no
authenticator every request is refused, unless the handler was made
`WithInsecureNoAuth`, and such a handler answers only a loopback Host. A
request that looks like a browser page's (an Origin, a cross-site
`Sec-Fetch-Site`, a body of another type than its route's) is refused in
every mode. `read` covers the files' reads, `write` writes and removes,
and `exec` runs anything: `Exec`, the processes and the environment,
which a client without it is not told. `exec` is never implied by `read`
or `write`. A served `ServerProvider`'s MCP servers take a scope of
their own, `servers`: the client lists them and starts one by name
(`Servers`, `StartServer`), each a process of its session, and runs no
command; `servers` and `exec` imply neither the other, `servers` needs
no `read` to dial (a client holding it alone reads no file: its
`FS`, `WriteFile` and `Remove` are `ErrForbidden`), and a process's
routes take the scope it was started under. A session is its
principal's, so its close ends every process in it, whichever scope
started it. The server starts it, so an
`Offering`'s `OutputFilter` reads its output before a byte is sent, and
its secrets never cross the wire. `Dial`'s client is a `Starter` exactly
when the server can start and the principal holds `exec`, and a
`ServerProvider` exactly when the server offers servers and the
principal holds `servers` (`*Client`, `*StarterClient`, `*ServerClient`,
`*StarterServerClient`). A client's processes live in a session it
opens at its first `Start` or `StartServer` and renews on a lease
(`DefaultLease`); when the lease lapses,
or the client closes, the server ends every process the session started,
and a process is reached only through its own session, by the principal
that opened it. `Exec` streams its output as NDJSON frames, and its
process ends with the request. Errors keep their sentinels across the
wire (`ErrOutside`, `fs.ErrNotExist` and the rest), and every bound
(body sizes, sessions, processes per session, a listing) is an exported
constant or option. The routes are in the package doc.

## Behind NAT: tunnel

`github.com/ChristopherDavenport/agentworkspace/tunnel` is for a
workspace the server cannot dial. Its host dials out over a WebSocket,
and serves the workspace to the server through it, on remote's wire
unchanged: the host is the HTTP/2 server inside the tunnel (h2c on the
WebSocket's bytes), the server the client.

```go
// The workspace's host: dial the hub and serve ws until ctx ends,
// dialling again after a drop. The scopes are this end's to grant; the
// credentials are the application's, in its HTTP client or a header.
err := tunnel.Serve(ctx, "wss://hub.example/tunnel", ws,
	tunnel.WithScopes(remote.ScopeRead, remote.ScopeWrite, remote.ScopeExec),
	tunnel.WithHeader(func(ctx context.Context) (http.Header, error) {
		return http.Header{"Authorization": {"Bearer " + credential()}}, nil
	}))

// The server: a Hub mounted where the hosts dial. The enrollment is the
// application's authentication: it names the workspace an upgrade is
// from, or refuses it, before the upgrade is answered.
hub, err := tunnel.NewHub(tunnel.WithEnrollment(func(r *http.Request) (string, error) {
	return enrolled(r) // "build-1", or an error
}), tunnel.WithChallenge("Bearer"))
defer hub.Close()
http.Handle("/tunnel", hub)

ws, err := hub.Workspace(ctx, "build-1") // waits for it to connect, bounded by ctx
defer ws.Close()
```

Over `wss://`, TLS proves the hub to the host. Over a `ws://` loopback
URL, as behind an `ssh -R` forward, whoever binds the port first gets
the upgrade, so the application proves the hub itself: the `Hub` adds
its headers to the 101 (`WithProofHeader`), and `Serve` checks them
(`WithHubProof`) before it serves anything. The package doc shows an
HMAC over a nonce as such a proof.

One `remote.Handler` serves every connection, so a session and its
processes outlive a drop shorter than the lease, and the server's
client renews through the next connection; what was in flight, a
process's open pipes included, fails and is not sent again. A drop past
the lease ends the processes, as over HTTP. A second connection for a
connected name replaces the first. HTTP/2 pings notice a dead tunnel at
both ends; a WebSocket message, the workspaces a hub holds and the
streams a tunnel carries are bounded. A workspace's offered MCP servers
cross as they do over HTTP: `WithScopes(remote.ScopeServers)` grants
the server the servers alone, no file and no command, its `Workspace`
a `ServerProvider` and no `Starter`. The package doc has the
details.

## Planned

- `Container`: a remote workspace whose server runs in a container.
- The built-in tools over a `Workspace` (`tools/`), when a second
  product wants them, and a persistent shell built on `Start`.

The design is dax's proposal (`docs/plans/proposals/agentworkspace-module.md`)
and the openhands-workspace study it came from.

## Development

```sh
make check    # gofmt, tidy, vet, deps, staticcheck, govulncheck, race tests, guard tests
```
