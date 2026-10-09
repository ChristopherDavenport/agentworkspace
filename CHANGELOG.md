# Changelog

All user-visible changes to this library. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/); before v1.0.0 minor
versions may break the API.

## Unreleased

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
