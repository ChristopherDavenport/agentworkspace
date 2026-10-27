# Changelog

All user-visible changes to this library. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/); before v1.0.0 minor
versions may break the API.

## Unreleased

- Added: `Descriptor.Host` and `Descriptor.Instance`, agentsession
  0.14's `workspace.host` and `workspace.instance` (#3). `Local` sets
  `Host` to the system's host name, or to `WithHost`'s, a new
  `LocalOption` of `NewLocal` for machines that share one, and leaves
  `Ref` empty. `remote` carries both in the served descriptor (`Served`),
  without the host to a principal lacking `read`. `Descriptor.Ref`'s
  doc no longer lists a host or an instance among what it holds, and
  `Workspace`'s says its methods are safe for concurrent use. RFC 0001
  §12 states which kind sets which, and `workspacetest.Run` checks the
  members and concurrent use.

- Fixed: on Linux, ending a process `Local` started (`Process.Close`,
  the context's end, `Command.Timeout`, the workspace's `Close`,
  `Process.Signal`, and an `Exec` its context or timeout ends) now
  reaches the descendants that left its process group, by `setsid`,
  `setpgid` or a shell's job control, which outlived it before (#32).
  Before each signal `Local` walks `/proc` by parent links, sends a
  descendant that leads a group of its own the signal by that group,
  and stops them all before it kills them. A descendant whose parent
  exited before any signal was sent is still not found. On macOS
  `Local` signals the group alone, as before.
- Added: `Reach`, `ReachOf` and `(*Local).Reach`, how far a starter's
  end of a process reaches (`ReachGroup`, `ReachDescendants`,
  `ReachAll`): `Local` is `ReachDescendants` on Linux and `ReachGroup`
  elsewhere. RFC 0001 §8 names the capability, and
  `workspacetest.Run` and `RunStarter` check a `setsid` child is gone
  after a timeout and a `Close`, skipping a backend whose reach is
  `ReachGroup`.

- Added: `Output.Interrupted`. When `Exec`'s context ends while the
  command runs, `Local.Exec` now returns the output collected so far,
  with `ExitCode` -1 and `Interrupted`, beside `ctx.Err()`, where it
  returned nil (#5). A `remote` client returns the same `Output`,
  without the output kept: the wire half of #5 (an exec key and a
  cancel route, so a hang-up keeps the output) is to come. RFC 0001 §6
  states the rule, and `workspacetest.Run` checks it.
- Changed: one ladder ends a process, however it is ended (#5):
  SIGINT, then SIGTERM two seconds later, then SIGKILL two seconds
  after that, to the process's group. `Process.Close` still closes the
  process's input and waits two seconds first. Before, `Local.Exec`
  sent SIGKILL at once on its context's end or its timeout, and a
  started process got SIGTERM then SIGKILL, so a process that ignores
  SIGINT and SIGTERM now takes two seconds longer to end. RFC 0001 §8
  states the ladder and why.

- Fixed (documentation): `Workspace.Env`'s doc comment said the
  environment had "the provider's key and other credentials already
  removed"; no backend removes anything (#37). It now says a workspace
  reports what it was given, nothing added or removed, and that keeping
  credentials out is the program's. RFC 0001 §4 states the rule, and
  `workspacetest.RunStarter` checks that `Env` is what the workspace
  was given.

- Changed: `Local` builds a process's environment itself, the
  workspace's with `Command.Env` over it name by name, each name once
  with the last value given it, rather than leave a repeated name to
  `os/exec` (#41). What a process sees is unchanged; the rule is now
  the contract's (RFC 0001 §4), and `workspacetest.Run` checks it.

- Fixed: `Local.Exec` and `Local.Start` find a bare program name on the
  `PATH` of the environment the process starts with (the workspace's,
  then the command's), as execvp(3) does, and no longer on the `PATH`
  of the program holding the workspace (#4). A relative `PATH` entry,
  the empty one included, is skipped, and with no `PATH` a bare name is
  not found, so a workspace built with an environment that has no
  `PATH` now runs only programs named with a slash. RFC 0001 §5 states
  the rule, and `workspacetest.Run` and `RunStarter` check it.

- Added (documentation): RFC 0001, the workspace contract
  (`docs/rfcs/0001-workspace-contract.md`), draft 0.1. It states what
  the module already promises (names and confinement, files, the
  environment, commands, Exec, processes and how they end, offered
  servers, the descriptor) with MUST/SHOULD wording, the Go,
  mcpworkspace, remote and tunnel bindings as tables, and
  `workspacetest` as the conformance suite. Rules the open issues will
  add are marked "Open: #N", and the places where the code and its doc
  comments disagree are listed as open questions. No API changes.

## v0.0.10 - 2026-10-26

- Fixed (documentation): the `tunnel` package doc's "Proving the hub"
  recipe had the workspace end make its own nonce, and said a MAC that
  reached an impostor could not be replayed to the hub later. Behind
  `ssh -R` it can: the impostor takes the port, collects an upgrade,
  and replays it through the forward once that is back, over a nonce
  the hub has never seen (#25). The recipe now has the hub issue the
  nonce, before each upgrade, in answer to a plain GET the program's
  handler beside the `Hub` serves: statelessly (its age on the hub's
  monotonic clock from a random offset, random bytes, and its MAC
  under a secret the hub makes at start, never the shared key), taken
  over a nonce the hub issued, within a short lifetime, once (a
  bounded cache of taken nonces). Both MACs cover a per-direction
  label, the workspace name the hub enrolls, the nonce and an instance
  id, so a proof for one hub does not pass at another with the same
  key. It says what is left: no channel binding, and an impostor that
  fetched real nonces before taking the port gets a replay within
  their lifetime per upgrade it collects; so the workspace end stops,
  rather than dial again, after `ErrHubProof` in every phase and after
  a few upgrades in a row a hub would have answered and did not, and
  on a nonce answer that rules a hub out. The recipe is now a
  compiled, offline example (`Example_hubProof`), and `WithHubProof`'s
  doc says to end `Serve` from the hook (cancel its context), since
  `Serve` tries again within the lease after a connection that served.
  No API changes.

## v0.0.9 - 2026-10-26

- Fixed: a `remote` process closed right after its start no longer
  loses its standard error (#24). Its `Command.Stream` gets everything
  the process wrote, including what it writes on its way out, however
  soon `Close` follows `Start`. Both ends close the race:
  - the client's `Process.Close` waits for the stream of standard
    error to open (the server's answer to `GET .../stderr`, whose
    headers it sends as soon as it has claimed the stream, as every
    release since v0.0.5 already did) before it sends the close. The
    wait is capped at a grace (two seconds) or the client's timeout,
    whichever is shorter, so headers a proxy holds back, or a dead
    connection, cost each close two seconds, not a timeout. `Start`
    waits for nothing more. Against a v0.0.8 server this half alone
    is the fix;
  - the server serves a closed process's standard error to a stream
    that comes after the close, until a grace after the process's
    exit; once a stream has taken it, another is `no_process`. Until
    it is streamed, or that grace passes, the process counts against
    `WithMaxProcesses`, as a closing one does. A v0.0.8 client, which
    sends the close without waiting, is covered by this half only
    while its stderr request arrives within that grace after the
    exit.
- Fixed: a process's stream or wait that found it just before its
  close took it from the session's table is refused (`no_process`)
  once its streams have ended, where it was served without being
  counted against the stream arithmetic.

## v0.0.8 - 2026-10-26

- Added: `remote.Client.Scopes`, on the `remote.Workspace` interface
  `Dial` returns: the scopes the server said, at `Dial`, the principal
  holds (the info route's `scopes`), a copy. A program can tell whether
  a grant may write before it asks, where it had to probe with a
  `Remove`. They are never empty: `Dial` now refuses a description
  that names no scopes, which no `Handler` sends. `remote.Workspace`
  gains the method, so a type of a program's own that implemented it
  must add it.
- Added: `remote.WithChallenge(value)`, the `WWW-Authenticate` of every
  401 a `Handler` writes (the `Authenticator`'s refusals, a principal
  with no name, no authenticator at all, a served workspace's
  `ErrUnauthenticated`), sent as given; and `tunnel.WithChallenge(value)`,
  the same for the 401s a `Hub` writes for an upgrade it refuses. The
  scheme is the program's, as its authentication is.
- Changed (breaking): a `remote.Handler`'s 401 no longer carries
  `WWW-Authenticate: Bearer` by default; without `WithChallenge` it
  carries no challenge. A program that relied on the `Bearer` default
  passes `remote.WithChallenge("Bearer")`.
- Added: hooks for the application to prove the hub to the workspace
  end before anything is served, where TLS does not (a `ws://`
  loopback URL behind an `ssh -R` forward, whose port any local user
  may bind first). `tunnel.WithHubProof(f)` at the workspace end calls
  `f(req, resp)` with the upgrade as sent and the hub's 101, after the
  101 and before the connection serves a request; an error ends the
  connection and is a refusal (`tunnel.ErrHubProof` wraps it), returned
  from a first dial and tried again within the lease after one that
  connected. `tunnel.WithProofHeader(f)` on the `Hub` adds the headers
  `f(r, name)` returns to the 101, given the request the enrollment
  named workspace `name` for; an error, or a header of the handshake's
  own, refuses the upgrade (500). The package doc shows an HMAC over a
  nonce as the proof (the package ships none), and what it does not
  prove: it authenticates the hub at the handshake and does not bind
  the connection, so it holds against an impostor that cannot reach the
  hub (the `ssh -R` case); where one can, use `wss://`. Over `ws://`
  the upgrade must carry no reusable credential.
- Added: `remote.Handler.Bounds`, the bounds a handler was made with
  (`remote.Bounds`: sessions, sessions per principal, processes,
  transfers, write, Exec output and listing bounds), beside `Lease`.
- Added: `tunnel.ReservedStreams` (`MaxStreams / 4`), the streams a
  tunnel keeps for the requests answered at once.
- Changed (breaking): `tunnel.Serve` enforces the stream arithmetic
  its guarantee rests on (no unread output stops a renewal, a wait or
  a start): it refuses remote bounds, given through
  `WithHandlerOptions`, that would let the server hold more than
  `MaxStreams − ReservedStreams` streams open (the sessions its one
  principal may have × their processes × 4, plus the transfers), where before it served them and gave the
  guarantee up. One bound is raised by lowering another. `MaxStreams`
  and the windows are not raised to fit instead: the hub's windows are
  its own, and assume the workspace end allows `MaxStreams` streams.
- Changed: `remote` keeps what a process and a session hold within its
  bounds whatever the client sends. A process being closed counts
  against `WithMaxProcesses` until its streams have all ended, and a
  session being closed or lapsed against `WithMaxSessions` and
  `WithMaxSessionsPerPrincipal` until it has ended, its closes in
  flight with it. A closed process's streams are cut, read or not: its
  input's as the close begins, its output's once it has exited, and
  its standard error's, sent to its end first, a grace after that. One
  wait is served for a process at a time: another while one waits is
  refused (`invalid`), and a client asks again, as `remote.Client`
  does. A program that wraps the handler's `ResponseWriter` gives the
  wrapper an `Unwrap`, as `http.ResponseController` asks.

## v0.0.7 - 2026-10-26

- Added: `remote` and `tunnel` carry a workspace's offered MCP servers
  (an `agentworkspace.ServerProvider`, such as an `Offering`). A new
  scope, `remote.ScopeServers` (`servers`), lists them
  (`GET /v1/servers`, in the served order) and starts one by name
  (`POST /v1/sessions/{s}/servers/{name}/start`, with a client-chosen
  key, an optional standard error stream, and lost-reply cleanup by the
  key, as a command's start has); the server is a process of the
  session, served by the process routes, counted by `WithMaxProcesses`,
  and ended by its lease, `Client.Close` and `Handler.Close`. The served
  workspace's `StartServer` starts it, so an `Offering`'s `OutputFilter`
  runs on the serving side and its secrets never cross the wire. `Dial`
  returns a `*remote.ServerClient` (an `agentworkspace.ServerProvider`)
  when the served workspace offers servers and the principal holds
  `servers`, and a `*remote.StarterServerClient` when it may start
  commands too; a type assertion to `Starter` or `ServerProvider` tells
  the truth in every combination. A name not offered is `agentworkspace.ErrNoServer`
  on the client (the wire code `no_server`). `servers` and `exec` imply
  neither the other, and `servers` needs no `read`: the info route
  answers it, so a principal holding `servers` alone dials, and its
  client lists and starts the servers, reads no file (its `FS`,
  `WriteFile` and `Remove` are `ErrForbidden`), runs no command, and is
  told the root's last element alone, not the host's path. A server
  that holds secrets is offered safely only to a principal holding
  `servers` alone: `exec` exposes its environment through `/proc`, and
  `write` can change its code, config or `PATH` in the workspace to
  print the secret past any filter. `tunnel.WithScopes` takes `remote.ScopeServers`; its stream
  arithmetic is unchanged, a server being one of its session's
  processes. `workspacetest.RunServerProvider` (`ServerProvider`) is
  the conformance table every `ServerProvider` runs: the list, the
  pipes, the workspace a server runs in, `ErrNoServer`, the lifetimes,
  standard error, and secrets kept out of the output; an `Offering` over
  `Local`, over `mcpworkspace`'s connection, over `remote` and over
  `tunnel` run it. A test in `mcpworkspace` holds one MCP server to one
  interface (the same whole `initialize` result, `tools/list` and
  `tools/call`), offered and as a command, here, over `mcpworkspace` and
  over `remote`.
- Added: `agentworkspace.ErrOutputFilter`, which every `OutputFilter`
  failure wraps (no reader, a panic in the call or a read). `remote`
  and `mcpworkspace` send its words alone for a server's start that
  failed so, never the panic's value, which may hold a secret.
- Changed: `remote`'s session routes take `exec` or `servers`, either
  one, where they took `exec`, and a process's routes take the scope it
  was started under: a command's start is `exec`'s, a server's
  `servers'`, and a principal holding the one is refused
  (`ErrForbidden`) a process the other started. A session is still its
  principal's: its close, or its lapse, ends every process in it,
  whichever scope started each. The info route answers `read` or
  `servers`, and its `start` and `servers` flags each say what the asking
  principal may start: `start` is now false for a principal without
  `exec`. A principal's Name owns its sessions and should carry one set
  of scopes: given several, any of them renews and closes the Name's
  sessions.
- Changed: `WithInsecureNoAuth`'s principal now holds `servers` too,
  and a principal holding `exec` and `servers` over a served
  `ServerProvider` (an `Offering`, say) gets a
  `*remote.StarterServerClient` from `Dial`, where v0.0.6 gave a
  `*remote.StarterClient`. Assert the interfaces
  (`agentworkspace.Starter`, `agentworkspace.ServerProvider`), not the
  concrete types.
- Changed: `mcpworkspace.StartServer` refuses a name
  `agentworkspace.ValidServerName` refuses with an error that is
  `agentworkspace.ErrNoServer`, as `Offering` and `remote` do, and
  `servers.start` answers such a name with `wire.CodeNoServer`.

## v0.0.6 - 2026-10-25

- Added: package `workspacetest`, in the root module and on the
  standard library alone: the conformance tables every backend runs,
  exported so a backend in a nested module runs them from its own
  tests. `Run` takes `Backend`s (confinement, FIFOs, `Env`, `Exec`,
  names that are not valid paths), `RunStarter` takes `Starter`s (the
  pipes, the environment, the lifetimes, the refusals), and `Confined`
  lays out a workspace beside a secret. The root's backends and
  `remote` run them; the tables moved out of the root's tests
  unchanged.

- Added: `remote.WithMaxTransfers` and `remote.DefaultMaxTransfers`
  (256): a handler serves at most that many Execs and reads and writes
  of a file's content at once, one past it `ErrLimit`; a session's own
  routes are not counted. `Handler.Lease` says the lease a handler
  keeps.
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
  It follows no redirect of the upgrade, whatever the application's
  client allows, and once connected tries a refused upgrade again
  within the lease. `DefaultSessions`, `DefaultProcesses` and
  `DefaultTransfers` are its bounds for remote's, which with
  `StreamWindow` and `ConnWindow` (all streams' windows) keep unread
  output and open streams from starving a renewal.
  `Hub` is the server end, an `http.Handler`: `NewHub`, `WithEnrollment`
  (`Enrollment`, the application's authentication, asked before the
  101; without one every upgrade is refused, and any with an `Origin`;
  `ErrTryLater` is a 503 the host dials again on),
  `WithMaxWorkspaces` (`DefaultMaxWorkspaces`), `Workspace`, which is
  `remote.Dial` over the connection and waits for a reconnect bounded
  by each request's context, `Connected`, `Close`, `ValidName` and
  `MaxNameBytes`; a second connection for a name replaces the first
  once it has answered over HTTP/2.
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

- Fixed: `remote.Handler.Close` returns only once a session that was
  ending as it was called (one that lapsed, or that its client closed)
  has ended with its processes; it used to miss those, which had left
  the table first.

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
