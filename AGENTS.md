# agentworkspace

The seam that lets an agent's built-in tools act on a local directory,
a container or a remote runtime: the `Workspace` interface (a file
system, writes, an environment, one-shot `Exec`, a descriptor the
session records), `Starter` for long-lived processes with pipes, and
`Local`, a directory on this machine over an `os.Root`. The design is
dax's proposal, `docs/plans/proposals/agentworkspace-module.md` in
`../dax`, and the study it came from, `../examples/openhands-workspace`
(`design.md`, `findings.md`); read the proposal before changing the
interface.

## Module

- Module path: `github.com/ChristopherDavenport/agentworkspace`.
- Go 1.26 is the floor. The root module holds three packages:
  `agentworkspace`, at the root, `remote`, and `workspacetest`, the
  conformance tables every backend runs (`Run`, `RunStarter`,
  `RunServerProvider`, `Confined`), exported so a nested module's
  backend runs them too.
- The root module imports the standard library alone. `make deps` and
  a test (`TestImportBoundary`, which names the root's packages)
  enforce it. Anything with a dependency is a nested module, and none
  of them may pull one into the root.
- `remote` (`github.com/ChristopherDavenport/agentworkspace/remote`):
  the primitive cut, a whole `Workspace` over HTTP. `NewHandler`
  serves any workspace as an `http.Handler` the program mounts (TLS is
  the program's); `Dial` returns a `*Client`, a `*StarterClient` when
  the served workspace is a `Starter` and the principal holds `exec`, a
  `*ServerClient` when it is a `ServerProvider` and the principal holds
  `servers`, or a `*StarterServerClient` when both hold, so both type
  assertions tell the truth. It ships no
  authentication: every request is put to the program's
  `Authenticator`, which names its `Principal` and scopes (`read`,
  `write`, `exec`, `servers`; `exec` and `servers` imply neither the
  other, `servers` needs no `read` to dial, and a process's routes take
  the scope it was started under; a session is its principal's, and
  its close ends every process in it),
  and the client's credentials are in the
  `http.Client` the program gives `WithHTTPClient` (no authenticator
  refuses everything unless `WithInsecureNoAuth`, which answers a
  loopback Host alone; every handler refuses a browser page's request:
  an Origin, a cross-site `Sec-Fetch-Site`, an untyped body). Started
  processes, and started servers, live in a session the client opens
  at its first start and renews on a lease; a lapse, a lease unrenewed
  on the client's side, or `Close` ends them.
  Its bounds are exported constants and options, its wire is in its
  package doc, and it is not the tool-call cut dax's executor uses.
- `mcpworkspace` (nested module
  `github.com/ChristopherDavenport/agentworkspace/mcpworkspace`, on the
  MCP Go SDK): a workspace's processes and files over an MCP connection
  the program already has. The server half (`ServeFiles`,
  `NewProcesses`) answers for a `Workspace`; the client half
  (`RegisterClient`, `Start` over a `Conn`, `NewFS` over a `Source`) is
  its `Starter` and read-only `fs.FS`. Package `mcpworkspace/wire`, the
  method names, resource template, JSON shapes and bounds, imports the
  standard library alone (`make deps` and its `TestImportBoundary`), so a relay can name them
  without the SDK. The processes are custom methods, never tools. It
  came from dax's executor (`internal/executor`'s `process.go` and
  `files.go`); dax keeps the connection, its redial and its capability.
  `CommandTransport` and `ServerTransport` are MCP transports to a
  stdio server started in a workspace, and the server methods
  (`servers.list`, `servers.start`) carry a `ServerProvider` over the
  connection.
- `tunnel` (nested module
  `github.com/ChristopherDavenport/agentworkspace/tunnel`, on
  `github.com/coder/websocket`): remote's wire, unchanged, over a
  WebSocket the workspace's host dials out, for a workspace behind NAT
  or a firewall. The roles flip inside it: the workspace end (`Serve`)
  is the WebSocket client and the HTTP/2 server (h2c with prior
  knowledge, `remote.NewHandler`), the server end (`Hub`, an
  `http.Handler`) the WebSocket server and the HTTP/2 client
  (`Hub.Workspace` is `remote.Dial` over an `http.ClientConn` from
  `Transport.NewClientConn`; a `Transport`'s pool dials more than once
  and cannot be given one connection). It ships no authentication: the
  hub asks the application's `Enrollment` for an upgrade's workspace
  before the 101, and the workspace end's credentials are in the
  application's HTTP client or header (`WithHTTPClient`,
  `WithHeader`); proving the hub to the workspace end, where TLS does
  not, is the application's too, through a hook at each end
  (`WithProofHeader` on the 101, `WithHubProof` before anything is
  served). The scopes are the workspace end's (`WithScopes`,
  required). One `remote.Handler` serves every reconnect, so sessions
  outlive a drop shorter than the lease; HTTP/2 pings notice a dead
  tunnel at both ends. Its connection window is all its streams'
  windows, and its defaults for remote's bounds keep the streams the
  server can hold open under `MaxStreams` (the arithmetic is on that
  constant, and a started server is one of its session's processes),
  so no unread output starves a renewal; keep both true when changing
  either. Offered MCP servers cross as remote carries them
  (`WithScopes` with `remote.ScopeServers`).
- Planned, beside them: a container starter.
- Releases are lockstep, as agenttool's: the root and every nested
  module at one version from one commit, a nested module requiring the
  root at exactly that version, with `replace ... => ..` in its go.mod
  so the release commit can name a version the proxy does not serve
  yet (`make replaces` checks it; `make extracted` builds without it).
- `Local` is for Unix (Linux and macOS): process groups and
  non-blocking opens. So is `remote`, whose signals are Unix's, sent by
  name.

## Rules the interface keeps

Every backend, present and planned, holds to these; the tests check
them, and a new backend runs the same tables: `workspacetest.Run` for
a `Workspace`, `workspacetest.RunStarter` for a `Starter` and
`workspacetest.RunServerProvider` for a `ServerProvider`, called from
the backend's own tests, as the root's `confine_test.go` and
`start_test.go`, remote's `conformance_test.go` and each nested
module's do. A table belongs in `workspacetest` when every backend
keeps its rule; one about Local alone stays in the root's tests.

- A name that leaves the workspace is `ErrOutside`, however it leaves:
  absolute, `..`, or a link out, read, written, removed or used as a
  command's `Dir`.
- Nothing blocks on a FIFO or a device.
- `Env` is what the constructor was given, a copy; nil is empty, never
  the host's environment.
- A started process lives no longer than its owner: `Close`, the
  context's end, `Command.Timeout` and the workspace's `Close` end it
  and everything it started.
- `Start` is optional (`Starter`), so a workspace that only stores
  files is still a `Workspace`.
- So is offering MCP servers (`ServerProvider`): a server it starts
  keeps `Start`'s lifetime, its name is `ValidServerName`'s, and which
  servers a session uses is the program's to decide, never the
  workspace's. A server's secrets are named on its offer and never
  told to a client; redacting its output is an `OutputFilter`'s, and
  the root imports no redactor (agentredact's `secrets` module is the
  one it is shaped for). `Offer` fails closed (secrets without a
  filter, an empty secret), and a filter that gives no reader, panics
  in a call or a read, or holds standard error back does not crash or
  hold up the host (a goroutine of the filter's own is the filter's). The filter covers the server's own output only: an `Offering`
  embeds its `Starter`, so a workspace that also runs a client's
  commands as the same user exposes a server's environment, and one
  that lets a client write can have a server's code, config or `PATH`
  changed to print its secret in an encoding no filter knows; secret
  holders are offered only to a client holding `servers` alone (remote,
  tunnel) or from a workspace that runs and writes nothing for it. A backend that carries a `ServerProvider` across a wire
  (mcpworkspace's server methods, remote's `servers` routes) starts the
  server on the serving side, so the filter runs there and no secret
  crosses, and lets a host grant the servers apart from commands and
  files, as remote's `servers` scope does alone (mcpworkspace's
  `NewProcesses` answers both, so there it is what the host serves). A
  name `ValidServerName` refuses is `ErrNoServer` on every backend.
- An MCP server's interface is the same wherever it runs: at the
  control, at a URL, or in the workspace (a command started there or a
  server it offers), and over whichever wire reaches it, so a program
  has one MCP transport and one set of tools whatever the placement.
  No wire adds to, drops or rewrites what the server says but for an
  `OutputFilter`'s redaction. mcpworkspace's
  `TestOneServerOneInterfaceOverEveryWire` pins it: one stdio server,
  offered and as a command, here, over mcpworkspace and over remote,
  gives one `initialize`, `tools/list` and `tools/call`.

## Siblings

Peer repositories, each independently versioned:

- `../dax`: the first product. Its `workspace` package is this
  module's draft; dax moves here by changing its import.
- `../agentsession`: the session format. `Descriptor` is what its env
  entry records (`EnvEntry.SetWorkspace(kind, ref)`); this module does
  not import it.
- `../agenttool`, `../agentturn`: the tool contract and the loop. Tools
  written over a `Workspace` live in a product for now; they move here
  (`tools/`) once a second product wants them.

## Conventions

Mirror `../dax` and `../agentsmd`: a `Makefile` with `build`, `deps`,
`test`, `vet`, `fmt`, `tidy`, `tidy-check`, `lint`, `vuln`, `check`,
`guard-test`, `release-guard` and `release` targets, the same CI
workflow shape (minimum and stable Go; lint on Go 1.26.x with
`check-latest`, since staticcheck cannot read go1.27.2's export data
yet), a `CHANGELOG.md` in Keep a Changelog form, and annotated `v*`
tags whose message becomes the release notes (`mcpworkspace/vX.Y.Z`
and `tunnel/vX.Y.Z` for the nested modules, written by the same `make
release`). `make check` must pass before any commit; in this workspace
run it as `GOWORK=off GOFLAGS=-mod=readonly make check`, and run `make
extracted` before pushing a change to a nested module.

Tests are table-driven and offline. They make real files, links,
FIFOs and processes under `t.TempDir()`; a process test bounds its
wait, so a process that was not ended fails rather than hangs.
