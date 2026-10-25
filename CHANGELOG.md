# Changelog

All user-visible changes to this library. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/); before v1.0.0 minor
versions may break the API.

## Unreleased

- Added: `ServerProvider`, a workspace that offers MCP servers of its
  own: `Servers` lists them (`Server`: a name, as `ValidServerName`
  holds it, and a description) and `StartServer` starts one by name as
  a `Process` with `Start`'s lifetime; a name not offered is
  `ErrNoServer`. `Offer` makes one from a `Starter` and a command per
  server (`OfferedServer`, `Offering`). `Local` offers none.
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
