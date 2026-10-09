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
  a test enforce it. The remote client and handler, and a container
  starter, are planned as subpackages or nested modules with their own
  dependencies; none of them may pull one into the root.
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
tags whose message becomes the release notes. `make check` must pass
before any commit; in this workspace run it as
`GOWORK=off GOFLAGS=-mod=readonly make check`.

Tests are table-driven and offline. They make real files, links,
FIFOs and processes under `t.TempDir()`; a process test bounds its
wait, so a process that was not ended fails rather than hangs.
