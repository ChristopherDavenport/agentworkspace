# agentworkspace

Where an agent's tools act: a file tree, processes, a working directory
and an environment, behind one interface, so the same read, write, edit
and bash tools run unchanged against a directory on this machine, a
container or a remote runtime. The module imports the standard library
alone.

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

## Planned

- `remote`: `Handler(ws)`, an `http.Handler` serving any workspace, and
  `Dial(url)`, a workspace over it, with `Start`'s pipes on the wire
  and scoped credentials (read, write, exec).
- `Container`: a remote workspace whose server runs in a container.
- The built-in tools over a `Workspace` (`tools/`), when a second
  product wants them, and a persistent shell built on `Start`.

The design is dax's proposal (`docs/plans/proposals/agentworkspace-module.md`)
and the openhands-workspace study it came from.

## Development

```sh
make check    # gofmt, tidy, vet, deps, staticcheck, govulncheck, race tests, guard tests
```
