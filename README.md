# agentworkspace

Where an agent's tools act: a file tree, processes, a working directory
and an environment, behind one interface, so the same read, write, edit
and bash tools run unchanged against a directory on this machine, a
container or a remote runtime. The module imports the standard library
alone, its package `remote` included, which serves a whole workspace
over HTTP; `mcpworkspace`, a module of its own beside it, carries a
workspace's processes and files over an MCP connection.

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
`servers.start`) can read a running server's environment from
`/proc/<pid>/environ`. Offer secret-holding servers from a workspace
that runs nothing else for the client, as dax's box host does, each
server a unit in its own container.

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
// The server: a plain http.Handler; TLS and the listener are the program's.
h, err := remote.NewHandler(ws, remote.WithAuthenticator(remote.BearerTokens(map[string]remote.Principal{
	token: {Name: "agent", Scopes: []remote.Scope{remote.ScopeRead, remote.ScopeWrite, remote.ScopeExec}},
})))
defer h.Close() // ends every session's processes
http.Handle("/", h)

// The client: a Workspace, and a Starter when the server can start and
// the principal holds exec.
ws, err := remote.Dial(ctx, "https://host/", remote.WithBearerToken(token))
defer ws.Close()
```

Every request is authenticated, whatever its route; with no
authenticator every request is refused, unless the handler was made
`WithInsecureNoAuth`, and such a handler answers only a loopback Host.
A request that looks like a browser page's (an Origin, a cross-site
`Sec-Fetch-Site`, a body of another type than its route's) is refused
in every mode. `read` covers the files' reads, `write` writes and
removes, and `exec` runs anything: `Exec`, the processes and the
environment, which a client without it is not told. `exec` is never
implied by `read` or `write`. A client's processes live in a session it
opens at its first `Start` and renews on a lease (`DefaultLease`); when
the lease lapses, or the client closes, the server ends every process
the session started, and a process is reached only through its own
session, by the principal that opened it. `Exec` streams its output as
NDJSON frames, and its process ends with the request. Errors keep
their sentinels across the wire (`ErrOutside`, `fs.ErrNotExist` and the
rest), and every bound (body sizes, sessions, processes per session, a
listing) is an exported constant or option. The routes are in the
package doc.

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
