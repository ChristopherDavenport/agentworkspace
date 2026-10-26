# Changelog

All user-visible changes to this library. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/); before v1.0.0 minor
versions may break the API.

## Unreleased

- Added: package `workspacetest`, in the root module and on the
  standard library alone: the conformance tables every backend runs,
  exported so a backend in a nested module runs them from its own
  tests. `Run` takes `Backend`s (confinement, FIFOs, `Env`, `Exec`,
  names that are not valid paths), `RunStarter` takes `Starter`s (the
  pipes, the environment, the lifetimes, the refusals), and `Confined`
  lays out a workspace beside a secret. The root's backends and
  `remote` run them; the tables moved out of the root's tests
  unchanged.

- Added: nested module `tunnel`
  (`github.com/ChristopherDavenport/agentworkspace/tunnel`, on
  `github.com/coder/websocket` v1.8.15), released with the root: a
  workspace behind NAT or a firewall served to a server through a
  WebSocket its host dials out, carrying remote's wire unchanged over
  HTTP/2 without TLS (h2c) on the WebSocket's bytes. `Serve` is the
  workspace end, the HTTP/2 server: it dials `wss://` (`ws://` to a
  loopback host alone), serves one `remote.Handler` across every
  reconnect, so sessions and processes outlive a drop shorter than the
  lease, redials with jittered, capped backoff (`WithRedial`,
  `DefaultRedialMin`, `DefaultRedialMax`), and closes the handler, and
  every process, when its context ends; `WithScopes` (required) is what
  the server may do, `WithHTTPClient` and `WithHeader` carry the
  application's credentials, and `WithHandlerOptions` passes remote's.
  `Hub` is the server end, an `http.Handler`: `NewHub`, `WithEnrollment`
  (`Enrollment`, the application's authentication, asked before the
  101; without one every upgrade is refused, and any with an `Origin`),
  `WithMaxWorkspaces` (`DefaultMaxWorkspaces`), `Workspace`, which is
  `remote.Dial` over the connection and waits for a reconnect bounded
  by each request's context, `Connected`, `Close`, `ValidName` and
  `MaxNameBytes`; a second connection for a name replaces the first.
  Both ends take `WithMaxMessageBytes` (`DefaultMaxMessageBytes`),
  `WithPing` (HTTP/2 pings, `DefaultPingInterval`,
  `DefaultPingTimeout`) and `WithLogger`; `MaxStreams`, `StreamWindow`,
  `ConnWindow` and `HandshakeTimeout` bound the rest. `ErrNotConnected`
  and `ErrClosed` are its errors. Offered MCP servers are not carried.
- Removed: `remote.BearerTokens` and `remote.WithBearerToken`. The
  library ships no authentication: it is the capability layer, and the
  application supplies authentication through the hook
  (`WithAuthenticator`, which names a request's `Principal` and its
  scopes) and its own HTTP client (`WithHTTPClient`, which carries the
  credentials). `Scope`, `Principal`, `WithInsecureNoAuth`, failing
  closed without an authenticator and the browser checks are kept. A
  base URL with userinfo is still refused.

## v0.0.5 - 2026-10-25

- Added: package `remote`, in the root module and on the standard
  library alone: a whole `Workspace` served over HTTP (`NewHandler`,
  a `*Handler` the program mounts, with `Close`) and one at the other
  end (`Dial`, which returns a `Workspace`: a `*Client`, or a
  `*StarterClient` when the served workspace is a `Starter` and the
  principal holds `exec`; `Served` is the served workspace's own
  `Descriptor`, and `Descriptor` is `KindRemote` with the base URL as
  its `Ref`). Its `FS` is an `fs.StatFS` and `fs.ReadDirFS`, and an
  `fs.ReadLinkFS` when the served one is; `Open` never blocks, and a
  regular file's content is streamed. `Exec` streams `Command.Stream`
  as NDJSON frames under the request's context; a started process's
  pipes are streams, its wait a long poll, its signals sent by name.
  Authentication fails closed: `Scope` (`read`, `write`, `exec`, never
  implied), `Principal`, `Authenticator`, `BearerTokens` (constant
  time), `WithAuthenticator`, `WithInsecureNoAuth`, `WithBearerToken`,
  `WithHTTPClient`, `WithTimeout`, and `PrincipalFrom`. Processes live
  in a session the client opens and renews on a lease
  (`WithLease`); its lapse, `Client.Close` and `Handler.Close` end
  them. The bounds: `MaxRequestBytes`, `MaxErrorBytes`, `StderrBytes`,
  `MaxReplyBytes`, and `DefaultLease`, `DefaultMaxSessions`,
  `DefaultMaxProcesses`, `DefaultMaxWriteBytes` and
  `DefaultMaxDirEntries` with their options; over one is `ErrLimit`.
  `ErrOutside`, `fs.ErrNotExist`, `fs.ErrExist`, `fs.ErrPermission`,
  `fs.ErrInvalid`, `errors.ErrUnsupported`, `os.ErrProcessDone` and
  `fs.ErrClosed` cross the wire as codes, so `errors.Is` holds on the
  client; `ErrUnauthenticated`, `ErrForbidden`, `ErrNoSession` and
  `ErrNoProcess` are its own.
- Added: `remote` refuses, in every handler, a browser page's
  request: one carrying an `Origin`, or a `Sec-Fetch-Site` other than
  `same-origin` or `none`, and a body of another type than its route's
  (`application/json`, or `application/octet-stream` for a file's
  content and a process's input; 415). `WithInsecureNoAuth` answers
  only a loopback `Host` (localhost, 127.0.0.0/8, ::1), so DNS
  rebinding is refused. A principal with an empty `Name`, which keeps
  sessions apart, is unauthenticated. `Handler.Close` returns only once
  every `Exec` and every process being closed has ended. A session is
  opened at the first `Start`, not at `Dial`, and one principal holds
  at most `DefaultMaxSessionsPerPrincipal` (8,
  `WithMaxSessionsPerPrincipal`). A client that goes a whole lease
  without a renewal loses the session: its processes' `Wait` returns
  -1 and `ErrNoSession`, their pipes fail with it, and the next
  `Start` opens another. An `Exec` without a `Stream` gives back at
  most `DefaultMaxExecOutputBytes` (16 MiB, `WithMaxExecOutputBytes`)
  of each stream, told in `/v1/info`, and is `ErrLimit` past it, the
  command having run; data frames are at most 32 KiB and the client
  bounds every frame by the server's bounds. `WriteFile` passes the
  permission bits alone, never setuid, setgid or sticky. A start
  carries a key the client chose, which names the process: a start
  sent again is the same process, and one whose reply was lost is
  closed by its key. A blocked `Command.Stream` holds up neither
  `Wait` nor `Close`. A listing reads one entry past its bound and no
  more. A timeout a `time.Duration` cannot hold is refused. An
  unmapped errno is sent as its words alone, and another error with
  the served root and other paths' directories cut from it.
- Added (tests): the confinement, FIFO, `Exec`, name and `Start`
  tables run on every backend, `remote` over `Local` among them,
  which joins from package `agentworkspace_test` (`AddBackend`,
  `AddStarter`).

## v0.0.4 - 2026-10-25

- Added: an offered server's `Secrets`, the variables of its
  `Command.Env` whose values are secrets (each a variable's name its
  Env sets exactly once; `Offer` refuses anything else and copies the
  list), and `Offer`'s options (`OfferOption`), one of them
  `WithOutputFilter`: an `OutputFilter` that the `Offering` reads such
  a server's standard output and standard error through, told the
  server's name and the secrets' values, its standard error written by
  the time `Wait` or `Close` returns. Shaped for agentredact's
  `secrets` module (`secrets.New(values).Reader(r, nil)`), which the
  root does not import. `Offer` fails closed: a server naming secrets
  without a filter is refused, and so is an empty secret; a nil option
  is skipped. Each filter call gets its own copy of the values; a
  filter that gives no reader or panics fails the start (the server
  ended), and one that panics reading standard output fails that read
  and every later one. Standard error is never forwarded in part: the
  workspace's writes never wait on the filter, and when it cannot be
  filtered whole and in order (more than a megabyte behind, the lag
  reaching about twice that; still held a grace, Local's, after the
  server ends; the filter failing) it is cut for the rest of the
  server's life, after one notice line of this package's own. A secret
  holding a newline is not found raw by a line-based filter. A
  backstop: it covers the offered servers' own output,
  not what the embedded `Starter` runs, so a workspace that also runs a
  client's commands as the same user lets the client read a server's
  environment; offer secret-holding servers from one that runs nothing
  else for the client (#8).

## v0.0.3 - 2026-10-25

- Added: `ServerProvider`, a workspace that offers MCP servers of its
  own: `Servers` lists them (`Server`: a name, as `ValidServerName`
  holds it, and a description) and `StartServer` starts one by name as
  a `Process` with `Start`'s lifetime; a name not offered is
  `ErrNoServer`. `Offer` makes one from a `Starter` and a command per
  server (`OfferedServer`, `Offering`), which copies the commands and
  refuses a nil workspace. `Local` offers none.
- Added (mcpworkspace): `CommandTransport` and `ServerTransport`, MCP
  transports to a stdio server started in a workspace, from a command
  or by the name the workspace offers it under, each `Connect` starting
  a new one (from dax's `startTransport`). The server methods
  `agentworkspace/servers.list` and `servers.start`, which
  `Processes.Register` answers when its workspace is a
  `ServerProvider`, a started server being a process of the session's;
  on the client half `ListServers`, `StartServer` and `Servers` (the
  workspace at the other end as a `ServerProvider`), with
  `ServersParams`, `ServersResult` and `ServerStartParams`, which
  `RegisterClient` now registers too. A server that does not answer
  the methods offers none.
- Added (wire): `ServersPrefix`, `MethodServersList`,
  `MethodServersStart`, `ServerMethods`, `ServerInfo`, `ServerList`,
  `ServerStartArgs` and `CodeNoServer`, the JSON-RPC error code of a
  name not offered.

## v0.0.2 - 2026-10-25

- Added: `mcpworkspace`, a nested module
  (`github.com/ChristopherDavenport/agentworkspace/mcpworkspace`), a
  workspace's processes and files over an MCP connection, from dax's
  executor (`internal/executor`'s process methods and workspace files),
  so that a program other than dax can run its tools in a sandbox and
  start MCP servers there. The server half: `ServeFiles` (the files,
  read-only, under `agentworkspace:///{op}{?path}`) and `Processes`
  (`NewProcesses`, `Register`, `End`, `Close`: the process methods for
  a `Starter`, each MCP session's processes apart and ended with it).
  The client half: `RegisterClient`, `Start` over a `Conn` (the
  session, the server's bounds, the connection's end, which is
  required and ends this side's requests before the session closes,
  and how a lost one is reported), `ProcessID`, and `NewFS` over a `Source`
  (`SessionSource`, `WithTimeout`, `WithName`). The parameter types
  for calling a method directly (`StartParams` and the rest).
- Added: `mcpworkspace/wire`, the protocol in the standard library
  alone: the method names (`agentworkspace/process.start`, `write`,
  `read`, `closeStdin`, `signal`, `wait`, `close`), `FilesURITemplate`,
  the file ops (`Ops()`) and error kinds, the JSON shapes, `Bounds`,
  `DefaultBounds()` and the constants, the signal names (`SignalOf`,
  `SignalName`), `Methods()`, `FileURI` and `ParseFileURI`; the tables
  a server checks against are copies, so an importer cannot change
  them. Changed from dax's: the names are
  `agentworkspace/process.*` and `agentworkspace:///`, not
  `dax/process.*` and `dax-workspace:///`.
- Changed: the root and `mcpworkspace` are released together, at one
  version from one commit (`make release` tags `vX.Y.Z` and
  `mcpworkspace/vX.Y.Z`); `make replaces` and `make extracted` are
  new, and CI builds the nested module extracted.

## v0.0.1 - 2026-10-09

- Initial version, from dax's `workspace` package as it stood on dax's
  main, renamed to `agentworkspace`: the `Workspace` interface (`Root`,
  `FS` with `fs.ReadLinkFS`, `WriteFile`, `Remove`, `Env`, `Exec`,
  `Descriptor`, `Close`), `Command`, `Output`, `Descriptor`, the
  `Kind*` constants, `ErrOutside`, `Read`, and `Local` over an
  `os.Root`.
- Added: `Starter`, `Process` and `Start`, for a process that outlives
  one call, with pipes for its input and output: what an MCP stdio
  server needs to run in the workspace. `Starter` is optional; `Start`
  on a workspace that is not one is `errors.ErrUnsupported`. `Local`
  starts each process in a group of its own and ends the group on
  `Process.Close` (input closed first, then SIGTERM, then SIGKILL), on
  the context's end, on `Command.Timeout`, on the workspace's `Close`,
  and, for what the process left running, when it exits.
- Changed from dax's package: a `Command.Dir` is checked through the
  root, so a `Dir` that leads out through a link is `ErrOutside`, as a
  name that climbs out already was, and a `Dir` that is not a
  directory is an error before anything starts; every FS method and
  `WriteFile` and `Remove` refuse a name that is not `fs.ValidPath`:
  one that is absolute or climbs out is `ErrOutside` everywhere (dax's
  `Open` answered `fs.ErrInvalid` and its `Stat` `ErrOutside`), and
  any other (`a/../b`) is `fs.ErrInvalid` (dax's `Stat`, `WriteFile`
  and the rest accepted it); `Local.Close` ends the processes `Start`
  started.
