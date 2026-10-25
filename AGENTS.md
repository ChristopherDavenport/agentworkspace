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
- Go 1.26 is the floor. One package, `agentworkspace`, at the root.
- The root module imports the standard library alone. `make deps` and
  a test enforce it. Anything with a dependency is a nested module, and
  none of them may pull one into the root.
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
- Planned, beside it: the HTTP `remote` client and handler (the
  primitive cut, a whole `Workspace` over HTTP) and a container
  starter.
- Releases are lockstep, as agenttool's: the root and every nested
  module at one version from one commit, a nested module requiring the
  root at exactly that version, with `replace ... => ..` in its go.mod
  so the release commit can name a version the proxy does not serve
  yet (`make replaces` checks it; `make extracted` builds without it).
- `Local` is for Unix (Linux and macOS): process groups and
  non-blocking opens.

## Rules the interface keeps

Every backend, present and planned, holds to these; the tests check
them, and a new backend runs the same tables (`backends` in
`confine_test.go`).

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
  commands as the same user exposes a server's environment; secret
  holders are offered from a workspace that runs nothing else for the
  client.

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
for the nested module, written by the same `make release`). `make
check` must pass before any commit; in this workspace run it as
`GOWORK=off GOFLAGS=-mod=readonly make check`, and run `make extracted`
before pushing a change to `mcpworkspace`.

Tests are table-driven and offline. They make real files, links,
FIFOs and processes under `t.TempDir()`; a process test bounds its
wait, so a process that was not ended fails rather than hangs.
