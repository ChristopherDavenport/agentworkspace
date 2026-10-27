# RFC 0001: Workspace Contract

Status: draft 0.2
Author: Christopher Davenport
Discussion: to be opened against this repository. The Go module at its
root is the reference binding; `Local` is the reference backend, and
`workspacetest` the reference conformance suite.

## Summary

A workspace is where an agent's tools act: a file tree under one root,
an environment, and processes that run there. It may be a directory on
the agent's machine, a container, or a runtime on another machine; a
tool written against the contract runs unchanged on any of them. The
contract has one required part, the **workspace** (names and
confinement, files, an environment, one-shot execution, a descriptor
the session records), and two optional ones: the **starter**, which
runs a process for longer than one call with pipes to it, and the
**server provider**, which offers MCP servers of its own by name.

This document states what the Go module already promises in its doc
comments and package docs, with the module as the reference binding
and its two wires, `remote` (HTTP) and `mcpworkspace` (MCP), as
bindings given against it. Draft 0.1 added no rule that the code or
its doc comments did not already state; draft 0.2 settles the issues
listed under [changes since draft 0.1](#changes-since-draft-01). Where an open issue will add or change a rule, the section
says so in an **Open** note naming the issue, and the rule is written
when that issue lands. Where the code and its doc comments disagree,
the disagreement is listed under [open questions](#19-open-questions)
rather than settled here.

## Motivation

The module's rules lived in three places: doc comments in
`workspace.go`, the "Rules the interface keeps" list in `AGENTS.md`,
and the package docs of `remote` and `mcpworkspace/wire`, which are the
two wire specifications. A second implementation, in Go or another
language, had to read all three and infer which sentences were
contract and which were the Go binding's choices. The sibling
libraries each have an RFC for that reason (agenttool's tool contract,
agentsession's session format, agentpolicy's policy rules); this one
did not.

The open issues make the gap concrete. How a bare program name is found
(#4), what an interrupted command returns (#5), what "everything it
started" covers (#32) and what a descriptor records (#3) are each a
rule another implementation must follow to interoperate, and without a
specification each would be settled in a Go doc comment.

## Goals

- **One contract, several placements.** A tool sees the same names,
  refusals, environment and process lifetimes whether the workspace is
  local, in a container or remote.
- **Confinement by name.** Every way a name can leave the root is
  refused the same way, for every operation.
- **Processes that do not outlive their owner.** Every way a process is
  ended ends what it started.
- **A record the session can compare.** The descriptor maps onto
  agentsession's `env` entry, member for member.
- **Wires that add nothing.** A binding over a wire carries the
  contract and refuses what it cannot carry; it does not invent
  behaviour.

## Non-goals

- **Isolation.** This contract confines names, not processes. A
  process a workspace runs reaches whatever its user can; what stands
  in front of it is the program's policy (see
  [isolation statement](#13-isolation-statement)).
- **Authentication.** The library is the capability layer; who a
  request is from is the program's to decide.
- **Tools.** The read, write, edit and shell tools a product builds
  over a workspace are agenttool tools, specified there.
- **Transport of the program's own MCP connection.** mcpworkspace rides
  a connection the program already has; dialling, redialling and
  authenticating it are the program's.

## 1. Terminology

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be
interpreted as in RFC 2119.

- **Workspace**: a file tree under one root, an environment, one-shot
  execution and a descriptor, as [sections 2 to 6](#2-names-and-confinement)
  and [section 12](#12-descriptor) define.
- **Root**: the absolute path of the workspace's working directory as
  the workspace's own processes see it: the checkout on the agent's
  machine, `/workspace` in a container. It is what the model is told
  and what the session records as its `cwd`.
- **Name**: a path relative to the root, as [section 2](#2-names-and-confinement)
  defines.
- **Backend**: an implementation of the workspace: `Local`, a
  container, a client of a wire.
- **Binding**: a mapping of this contract onto a language or a wire.
- **Wire**: a binding that crosses a connection: remote (HTTP),
  mcpworkspace (MCP custom methods and resources), and tunnel (remote's
  wire over a WebSocket).
- **Serving side** and **client side**: the two ends of a wire. The
  serving side holds the workspace; the client side is a backend whose
  operations cross to it.
- **Starter**: a workspace that can also start a long-lived process
  ([section 7](#7-processes)).
- **Process**: a process a starter started, with pipes to it.
- **Owner**: what a started process lives no longer than: its handle's
  close, the context it was started under, its timeout, and the
  workspace's close.
- **Server provider**: a workspace, or a client of one, that offers MCP
  servers by name ([section 11](#11-servers-offered)).
- **Offering**: a server provider whose servers are commands in a
  starter, with the variables of each that hold secrets named.
- **Program**: the application that builds the workspace and decides
  what it is used for: an agent harness, a host serving a workspace.
- **Principal**: who a request on a wire is from, as the program's
  authenticator names it, with the scopes it holds.

## 2. Names and confinement

A **name** is valid when it is UTF-8, slash-separated, relative, and
has no empty, `.` or `..` element and no trailing slash; `.` alone is
the root. This is Go's `fs.ValidPath`. Turning a path the model wrote
into a name is the tool's, not the workspace's.

Every entry point that takes a name (open, stat, lstat, readlink,
readdir, read, write, remove) and a command's directory MUST refuse an
invalid name before it reaches the file system:

- a name that is absolute, or that cleans to `..` or to a path under
  it, **leaves** the workspace and is refused as *outside*;
- any other invalid name (`a/../b`, `./b`, `b/`, the empty name) is
  refused as *invalid*.

A valid name that leads out of the root through a symbolic link, at
any element, MUST be refused as *outside* for reads, writes, removes
and a command's directory alike, and nothing outside the root is read
or changed. A link that stays inside works. Where a link leads is read
through the workspace or not at all; it MUST NOT be followed on the
agent's machine.

A workspace whose file system can tell a link from what it leads to
SHOULD offer lstat and readlink. One that cannot leaves a check that
must follow links unable to, and so asking; a binding says which it is
(remote's info route carries `links`).

No operation blocks on a FIFO or a device. Opening one MUST return, or
fail, at once, and what was opened MUST say what it is, so a caller
checks for a regular file. A read, a listing or a write of one is
refused.

### Error kinds

A refusal has one of these kinds, which every binding maps to and from
its own vocabulary so that a caller can test for the kind on either
side of a wire:

| Kind | Meaning | Go | remote code |
| --- | --- | --- | --- |
| outside | the name leaves the workspace | `ErrOutside` | `outside` |
| not-exist | nothing has the name | `fs.ErrNotExist` | `not_exist` |
| exist | something already has the name | `fs.ErrExist` | `exist` |
| permission | the operation is not permitted | `fs.ErrPermission` | `permission` |
| invalid | the name or argument is malformed | `fs.ErrInvalid` | `invalid` |
| unsupported | this workspace cannot do the operation | `errors.ErrUnsupported` | `unsupported` |
| process-done | the process has exited | `os.ErrProcessDone` | `process_done` |
| closed | the workspace or the handle is closed | `fs.ErrClosed` | `closed` |
| no-server | no server is offered by that name | `ErrNoServer` | `no_server` |

*outside* is matched before any other kind, since a link's refusal may
also read as another.

Over remote, a mapped refusal's message is the system's or the kind's
words, never the served error's own text, which may name a path on the
serving side's machine. mcpworkspace's file kinds are a subset of this
table, and its messages are the served error's text; both differences
are listed under [open questions](#19-open-questions).

## 3. Files

The file operations are:

- **stat** a name, following links that stay inside; **lstat** and
  **readlink**, where offered, without following.
- **readdir**: a directory's entries, as lstat describes each, opened
  as a directory and without blocking, so a FIFO or a device in its
  place is refused at once.
- **open** and **read**: a regular file's content. A bounded read
  (Go's `Read(w, name, max)`) refuses a FIFO, a device, a directory and
  a file over its bound.
- **write**: create or replace a regular file with the given content
  and permission bits, creating the directories above it. A FIFO or a
  device at the name MUST be refused without blocking on it.
- **remove**: a file or an empty directory.

Every operation takes names as [section 2](#2-names-and-confinement)
says. A binding MAY carry a subset (mcpworkspace serves the reads
alone); one it does not carry is not offered, or refused as
*unsupported*.

## 4. Environment

A workspace has one **environment**: the list of `NAME=value` entries
every process in it starts with. It is what the workspace was
constructed with, and nothing else:

- an empty environment is empty, never the environment of the process
  that built the workspace;
- the environment a caller is given MUST be a copy: changing it changes
  nothing a later process starts with.

A workspace MUST report the environment it was given, and MUST start
its processes with it, adding nothing and removing nothing: not the
model provider's key, not a token, not a name that looks like a secret.
Keeping the program's own credentials out of a workspace is the
program's duty, done before it constructs the workspace, and a reader
of the environment MUST NOT take it as screened. A backend that offers
to remove entries does so only by an option the program names, and
reports what is left.

**A process's environment.** A process starts with the workspace's
environment with the command's entries ([section 5](#5-commands)) over
it, name by name. An entry's name is what comes before its first `=`.
An entry of the command replaces the workspace's entry of the same
name, and a later entry of either replaces an earlier one of the same
name. Each name MUST appear once in what the process receives, with
the last value given it; where in the list it appears is not part of
the contract. A backend MUST apply this rule itself rather than rely on
what its process API does with a repeated name, which differs between
systems. A binding over a wire passes the command's entries as given,
and the serving side's workspace applies the rule.

A binding over a wire tells the environment only to a principal that
may run commands (remote's `exec` scope); to any other it is empty.

## 5. Commands

A **command** is one process to run:

| Member | Meaning |
| --- | --- |
| args | argv; MUST NOT be empty, and an empty one is refused before anything starts |
| dir | a name of a directory, relative to the root; empty is the root. A dir that leaves the workspace, by name or through a link, is refused as *outside*; one that is not a directory is refused |
| env | entries over the workspace's environment, name by name ([section 4](#4-environment)) |
| stdin | the bytes of standard input for Exec; none when absent. Start refuses it: a started process's input is its pipe |
| timeout | ends the process, and everything it started, after this long; zero is no limit but the owner's |
| stream | where the output the caller is not otherwise given goes as it arrives: for Exec, standard output and standard error interleaved; for Start, standard error. Absent discards it (Start) or keeps it in the result (Exec) |

The process runs at the root, or at dir, as the workspace's own
processes see it, with the workspace's environment and then the
command's.

**Argv resolution.** The program a command runs is found as POSIX
`execvp` finds it, over the process's own environment:

- When `args[0]` contains a `/`, it names the program, relative to dir
  when it is not absolute.
- When it contains none, it MUST be looked up in the `PATH` of the
  environment the process starts with (the workspace's, with the
  command's over it, [section 4](#4-environment)), as the workspace's
  own processes see the file system, and never in the `PATH` of the
  program that holds the workspace. Each entry is tried in order, and
  the first executable file of that name runs.
- Only absolute entries are searched. A relative entry, the empty one
  included, MUST be skipped, not resolved against dir: otherwise a file
  written into the workspace could stand in for a program (Go's
  `exec.ErrDot` refuses the same).
- With no `PATH`, a bare name is not found. A backend MUST NOT supply a
  default, since a container's default differs from the host's.
- A name not found is an error, and nothing runs. The process gets
  `args[0]` as given, whatever the lookup found.

`workspacetest` holds it: `Run` runs a bare name found only on the
command's `PATH`, and `RunStarter`, over a workspace with no `PATH`,
finds nothing for a bare name, an empty `PATH` or a relative entry, and
runs a name with a slash relative to dir.

## 6. Exec

Exec runs one command to completion in a fresh process and returns how
it ended:

| Member | Meaning |
| --- | --- |
| stdout, stderr | what the process wrote, each in full, when the command had no stream; empty when it had one |
| exit status | the process's status; −1 when a signal ended it, or its timeout or the caller's context did |
| timed out | the command's timeout ended it |
| interrupted | the end of the caller's context ended it; the result comes beside the context's error |

- An exit status other than zero is a result, not an error.
- When the command's timeout ends it, Exec MUST end the command and
  everything it started ([section 8](#8-ending-a-process)), and MUST
  return a result with exit status −1 and *timed out* set, not an
  error.
- When the caller's context ends while the command runs, Exec MUST end
  the command and everything it started in the same way, and MUST
  return a result beside the context's error: the output collected
  until the command ended, exit status −1 and *interrupted* set. With a
  stream, the output went to the stream as it came, and the result
  keeps none, as when the command runs to its end. A caller that
  interrupts a command still learns what it printed, which a build or a
  test run has often said by then.
- When the caller's context has ended before the command runs, Exec
  returns the context's error, runs nothing, and gives no result.
- A command that cannot start (no args, a refused dir, a program not
  found) is an error, and nothing runs.

A binding over a wire MUST deliver an interrupted command's output to
the client, so its cancellation is a message, not a dropped connection.
**Pending (#5, the wire half):** remote does not yet. `/v1/exec` sends
its `{"output"}` frame only at the end, and the client hangs up when its
context ends, so over remote an interrupted Exec returns −1 and
*interrupted* with no output kept, beyond what a stream was given. The
change gives exec a key and a cancel route, as processes already have
`/cancel`, and the client reads on after it cancels. Until it lands,
`workspacetest.Run` checks the kept output on every backend but a
remote one.

Open: #33 will give the mcpworkspace client an Exec composed over
Start, held to this section.

## 7. Processes

Starting is optional. A workspace that only stores files, or a runtime
whose API runs one command at a time, is still a workspace; a caller
asks whether it is a starter, and a start on one that is not is refused
as *unsupported*.

**Start** starts a command and returns at once. It MUST refuse a
command with stdin, and MUST refuse a start whose context has already
ended, with the context's error, before anything runs. A started
process has:

- **standard input**, a pipe; closing it is end of file for the
  process;
- **standard output**, a pipe, which reaches end of file once the
  process and everything it started have closed it;
- **standard error**, to the command's stream, or nowhere;
- **wait**, which waits for the exit and returns the status (−1 if a
  signal ended it) and an error: the owner's (the context's) when the
  context or the timeout ended it, otherwise none unless waiting itself
  failed. An exit status other than zero is not an error. Wait MAY be
  called any number of times and MUST return the same each time;
- **signal**, which sends a signal to the process and everything it
  started, and is *process-done* once it has exited;
- **close** ([section 8](#8-ending-a-process)).

A process's operations MUST be safe to use at once: one reader of its
output, one writer of its input and one waiter. A caller that is done
with a process closes it, even after it exited.

Signals are named on a wire by their POSIX names without the `SIG`
prefix. remote carries `HUP INT QUIT KILL USR1 USR2 PIPE ALRM TERM CONT
STOP TSTP WINCH`; mcpworkspace carries `INT TERM KILL`. A binding MUST
refuse a signal it does not carry rather than send another.

Open: #29 will add a terminal mode: a process started with a
pseudo-terminal of a given size as its controlling terminal, a resize,
and a refusal, never a fallback to pipes, where a backend cannot give
one.

## 8. Ending a process

A started process lives no longer than its owner. Each of these MUST
end it and everything it started:

- its **close**;
- the end of the **context** it was started under;
- the command's **timeout**;
- the workspace's **close**.

When the process exits by itself, what it started MUST go with it.

**The ladder.** Every way a process is ended (its close, the end of its
context, its timeout, the workspace's close, and an Exec's context or
timeout, [section 6](#6-exec)) MUST send the same signals, in this
order, to the process and everything it started, and stop once the
process has exited:

1. INT;
2. a grace later, TERM;
3. a grace after that, KILL.

When the process exits partway, what it started that is still running
is killed at once. The grace is the backend's; the reference binding's
is two seconds at each step (`internal/lifetime.Grace`), and
`workspacetest` checks against the same value.

**Close** first closes the process's input and gives it a grace to
exit, as the MCP stdio transport's shutdown asks, and then climbs the
ladder. It returns once the process has exited, and MAY be called more
than once. The end of the context and the timeout are interrupts, not a
shutdown, and begin at the ladder's first step.

The ladder is the same for all of them so that a program behaves one
way however it is stopped. INT comes first because it is the interrupt
a terminal's user sends to a foreground job, and the end of a call's
context is the user's interrupt (agenttool RFC 0001, stopping a call);
a program that buffers its output or prints a summary writes it on INT,
which is what an interrupted Exec returns. TERM follows because a job a
non-interactive shell started in the background ignores INT (POSIX has
it ignore INT and QUIT) but not TERM, and because the MCP stdio shutdown
names TERM, so a server or a background job gets its chance to clean up
before KILL. A process that ignores both is killed two graces after the
ladder begins.

Open: #32 will state that "everything it started" includes the
descendants that left its process group or session, and how a backend
that cannot find an orphaned descendant says so.

## 9. Ports

The contract has no port operation at draft 0.1. A process in the
workspace may listen, and nothing in the contract reaches it from the
agent's side.

Open: #31 will define `connect(port)`, one byte stream to a TCP port on
the workspace's loopback as its own processes reach it, the wire
carriers for it, and the scope it takes.

## 10. Roots

A workspace has exactly one root, and every name and every command's
dir is relative to it. The name *named roots* is reserved.

Open: #34 will settle whether a workspace may have further named roots
(as ACP's `additionalDirectories` asks), how a name selects one, what a
process sees of each, and how the session records them; the session
side is agentsession RFC 0001's to decide first.

## 11. Servers offered

Offering servers is optional. A **server provider** offers MCP servers
of its own, each a stdio server it starts by name: one installed in a
container's image, one a host runs beside a sandbox. A client of a
workspace elsewhere may be a server provider without being a
workspace.

**Names.** A server's name is one to 32 lowercase ASCII letters,
digits and inner hyphens, beginning and ending with a letter or a
digit (`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`). It has no underscore,
so a program can prefix a server's tools with its name and a separator
of underscores (`mcp__<name>__<tool>`) without two servers' tools
running together.

**List.** The provider lists the servers it offers, each a name and an
optional description for a person choosing among them, in a stable
order.

**Start.** Starting a server by name returns at once, as Start does,
with the same lifetime ([section 8](#8-ending-a-process)): its close,
the end of its context and the provider's close each end it and
everything it started. Its standard input and output carry the MCP
stream; its standard error goes to the caller's writer, or nowhere. A
name the provider does not offer, a name that is not valid among them,
MUST be refused as *no-server* on every binding.

**Choice.** Which of the servers a session uses is the program's to
decide. A workspace offering a server does not make the program
connect to it.

**One interface.** An MCP server's interface is the same wherever it
runs (at the control, at a URL, or in the workspace, as a command or
offered) and over whichever wire reaches it. No binding adds to, drops
or rewrites what the server says, but for the redaction below.

**Secrets.** An offering names, for each server, the variables of its
environment whose values are secrets. Their names and values MUST NOT
be told to a client: a listing carries name and description alone. An
offering that names secrets MUST read the server's standard output and
standard error through a redacting filter, and MUST refuse to offer
the server when it has none, or when a named secret is not set exactly
once, or is set to the empty string. The filter runs where the server
is started, on the serving side of any wire, so no secret crosses one.
A filter that fails fails closed: the start fails, standard output's
read fails, or standard error is cut after a notice; output is never
passed on unread by the filter.

The filter covers the server's own output and nothing else. A client
that may run commands as the server's user can read a running server's
environment; a client that may write the workspace can change the
server's code, configuration or `PATH` and have it print a secret in an
encoding no filter knows. A server that holds secrets SHOULD therefore
be offered only to a client that may neither run commands in the
workspace nor write it: remote's `servers` scope alone, or a workspace
that runs and writes nothing for that client.

Open: #23 will add a `secrets` mark to a listed server, informative
only: a client MUST NOT read its absence as a statement that a server
holds none.

## 12. Descriptor

A **descriptor** is what the session records about a workspace, in
agentsession RFC 0001's `env` entry:

| Descriptor | agentsession `env` | Meaning |
| --- | --- | --- |
| kind | `workspace.kind` | `local`, `container` or `remote`; closed |
| ref | `workspace.ref` | what the harness resolves to the workspace: a container's image digest, a remote's endpoint; empty for `local` |
| root | `cwd` | the root |

The descriptor is a fact about placement, recorded so that a session
resumed against another file system records a substitution.

- `Local` records `kind: local` and its root, and no `ref`.
- A remote client records `kind: remote`, the base URL with no
  userinfo, query or fragment as `ref`, and the served root. It keeps
  the served workspace's own descriptor beside it (Go's `Served`), and
  never puts a credential in either.
- A tunnel's client records `kind: remote` and
  `http://<name>.tunnel.invalid` as `ref`, a host that resolves
  nowhere.

Since agentsession 0.14, `workspace` also has `host`, the machine the
file system is on, and `instance`, the file system among those its
`kind`, `ref` and `host` describe, both opaque strings; a writer SHOULD
write `host` for every `local` workspace and `instance` whenever a
restart gives a new file system. No descriptor carries either at draft
0.1. Open: #3 will add both members, say which kind sets which, and
what a `host` value must distinguish.

Open: #30, a container backend, records the image digest as `ref` and
the running instance as `instance`, and ends its instance at close.

## 13. Isolation statement

Nothing a workspace reports about itself (its descriptor, a server
listing, a wire's info route) is a statement of isolation. Only the
host that enforces isolation can make one. A program MUST NOT relax
its policy on a workspace's own word. Confinement in this contract is
confinement of names ([section 2](#2-names-and-confinement)); a process
a workspace runs is not confined by it, and `Local`'s processes reach
whatever the user can.

The consumers of an isolation statement are agenttool RFC 0001's
*confined* property and agentpolicy RFC 0001 §Confinement. Open: #9
will define what a host says about an offered server's isolation
(files, network, secrets, runtime); it waits for a second product, and
until then no listing carries one.

## 14. Bindings

### Go

The root module is the reference binding.

| Contract | Go |
| --- | --- |
| workspace | `Workspace` interface |
| root | `Workspace.Root()` |
| names | `fs.ValidPath` names; `ErrOutside` |
| files: stat, lstat, readlink, readdir, open | `Workspace.FS()` → `fs.FS`; `fs.StatFS`, `fs.ReadDirFS`, `fs.ReadLinkFS` where offered |
| bounded read | `Read(w, name, max)` |
| write, remove | `Workspace.WriteFile(ctx, name, data, perm)`, `Workspace.Remove(ctx, name)` |
| environment | `Workspace.Env()`, a copy |
| command | `Command{Args, Dir, Env, Stdin, Timeout, Stream}` |
| exec | `Workspace.Exec(ctx, cmd)` → `*Output{Stdout, Stderr, ExitCode, TimedOut, Interrupted}`, beside `ctx.Err()` when interrupted |
| descriptor | `Workspace.Descriptor()` → `Descriptor{Kind, Ref, Root}`; `KindLocal`, `KindContainer`, `KindRemote` |
| close | `Workspace.Close()` |
| starter | `Starter` interface; `Start(ctx, ws, cmd)` refuses a non-starter with `errors.ErrUnsupported` |
| process | `Process{Stdin, Stdout, Wait, Signal, Close}` |
| owner's context | the `ctx` given to `Start` or `StartServer` |
| server provider | `ServerProvider{Servers, StartServer}`, `Server{Name, Description}`, `ValidServerName`, `ErrNoServer` |
| offering | `Offer(ws, servers, opts...)` → `*Offering`; `OfferedServer{Server, Command, Secrets}`; `WithOutputFilter(OutputFilter)`; `ErrOutputFilter` |
| reference backend | `NewLocal(dir, env)` → `*Local`, over an `os.Root`; Unix (Linux and macOS) |

### mcpworkspace

mcpworkspace carries a workspace's processes, its offered servers and
a read-only view of its files over an MCP connection the program
already has. Its protocol is package `mcpworkspace/wire`, which imports
the standard library alone.

| Contract | mcpworkspace |
| --- | --- |
| start | `agentworkspace/process.start {args, dir?, env?, timeoutMs?, stderr?} → {id}` |
| standard input | `process.write {id, data}`, at most `MaxWriteBytes` (1 MiB) each; `process.closeStdin {id}` |
| standard output, standard error | `process.read {id, fd: 1\|2, max?} → {data?, eof?}`, a long poll, at most `MaxReadBytes` (64 KiB) |
| signal | `process.signal {id, signal: INT\|TERM\|KILL} → {done?}` |
| wait | `process.wait {id} → {status, ended?, error?}` |
| close | `process.close {id} → {status, ended?, error?, stderr?}` |
| owner | the MCP session: each session has its own process table, at most `MaxProcesses` (32) open; its end ends them |
| list servers | `agentworkspace/servers.list {} → {servers: [{name, description?}]}` |
| start a server | `agentworkspace/servers.start {name, stderr?} → {id}`, a process of the session; not offered is JSON-RPC error `-32010` |
| files | resource template `agentworkspace:///{op}{?path}`, op `stat`, `lstat`, `readdir`, `readlink` or `read`; at most `MaxFileBytes` (1 MiB) a read and `MaxDirEntries` (10000) a listing |
| error kinds (files) | `notexist`, `outside`, `invalid`, `permission`, `other` |

The process and server methods are custom JSON-RPC methods, never
tools: a tool would be offered to the model of any harness connected
to the server, and starting a process is arbitrary execution. A server
offering no servers need not answer the server methods; a client takes
*method not found* as none offered.

It does not carry write, remove, Exec, the environment, the root or
the descriptor. Its client side is a starter's Start and a read-only
file system, not a workspace.

### remote

remote carries a whole workspace over HTTP. Every route is under `/v1`
of a base URL; its package doc is the route table, which this section
does not repeat. In summary:

| Contract | remote | Scope |
| --- | --- | --- |
| root, descriptor, bounds, environment | `GET /v1/info` | `read` or `servers`; environment only with `exec` |
| files | `GET /v1/fs/{stat,lstat,readlink,readdir,file}?path=N` | `read` |
| write, remove | `PUT`, `DELETE /v1/fs/file?path=N` | `write` |
| exec | `POST /v1/exec`, NDJSON frames back: `{"data"}` for the stream, then one `{"output"}` or `{"error"}` | `exec` |
| list servers | `GET /v1/servers` | `servers` |
| owner | a session, `POST /v1/sessions`, renewed on a lease; its lapse or close ends every process in it | `exec` or `servers` |
| start | `POST /v1/sessions/{s}/processes`, keyed by the client | `exec` |
| start a server | `POST /v1/sessions/{s}/servers/{n}/start`, keyed | `servers` |
| pipes, wait, signal, cancel, close | `/v1/sessions/{s}/processes/{p}/…` | the scope it was started under |
| error kinds | every kind of [section 2](#error-kinds), and the wire's own (`unauthenticated`, `forbidden`, `limit`, `no_session`, `no_process`, `media_type`) | |

`exec` and `servers` imply neither the other; `servers` needs no
`read`. A principal holding `servers` alone is a server provider and no
starter, reads no file, runs no command and is told no environment. A
principal without `read` is told the root's last element alone, not a
path on the serving side's machine.

The server says its bounds in the info route, and the client sends no
more. Exec without a stream gives back at most the output bound of each
stream; past it the command has run and Exec is refused as `limit`.

### tunnel

tunnel is remote's wire, unchanged, over a WebSocket the workspace's
host dials out, for a workspace behind NAT or a firewall. It is not a
separate contract: what remote does not carry, the tunnel does not,
and its routes, scopes, sessions, bounds, error kinds and browser
checks hold inside it as over HTTP. Its own rules (enrollment, the
hub's proof, reconnecting within the lease, liveness and stream
bounds) are in its package doc.

## 15. Security considerations

- **No authentication in the library.** remote and tunnel put every
  request to the program's authenticator, which names a principal and
  its scopes; without one, every request is refused. A binding MUST
  NOT carry a credential in a URL, an error or a descriptor.
- **Scopes are capabilities.** `exec` is never implied by another
  scope. Running commands exposes a running server's environment, and
  writing defeats any output filter, so secret holders are offered
  under `servers` alone.
- **No browser clients.** A remote handler refuses a request carrying
  an `Origin`, a `Sec-Fetch-Site` other than `same-origin` or `none`,
  or a body of another media type than its route's. One made without
  authentication for a loopback listener answers a loopback `Host`
  alone, so a rebound name is refused.
- **Paths do not leak over remote.** A mapped error crossing remote
  carries no path on the serving side's machine, and the root is told
  in full only to a principal that may read. mcpworkspace does not
  promise the first (see [open questions](#19-open-questions)).
- **Process ids** are random, and a process is reached only through
  the session that started it, by the principal that opened that
  session.
- **Names confine; processes do not.** See
  [isolation statement](#13-isolation-statement).

## 16. Conformance

**A conforming workspace** refuses every name as
[section 2](#2-names-and-confinement) says, reads and changes nothing
outside its root, blocks on no FIFO or device, gives a copy of the
environment it was constructed with, runs Exec as
[section 6](#6-exec) says, and describes itself as
[section 12](#12-descriptor) says.

**A conforming starter** is a conforming workspace that starts and
ends processes as [sections 7](#7-processes) and
[8](#8-ending-a-process) say.

**A conforming server provider** lists, starts and refuses servers as
[section 11](#11-servers-offered) says, and keeps an offering's secrets
out of what reaches the client.

**A conforming binding** maps each row of its table, refuses what it
does not carry as *unsupported* or by not offering it, and adds
nothing the contract cannot express.

The reference suite is Go package `workspacetest`:

| Table | Holds |
| --- | --- |
| `Run` | a workspace: every way out refused, what stays in works, nothing blocks on a FIFO, the environment a copy, Exec (argv resolution, the environment, a timeout, an interrupt), invalid names |
| `RunStarter` | a starter: the pipes, the directory and environment, the lifetimes (context, timeout, close, exit, the workspace's close), signals, refusals, concurrency |
| `RunServerProvider` | a server provider: the list, the pipes, the directory, *no-server*, the lifetimes, standard error, secrets kept out of the output |
| `Confined` | the layout `Run` uses: a workspace beside a directory holding a secret, with links that lead out and one that stays in |

A backend runs the tables from its own tests, as `Local`, remote, the
tunnel and mcpworkspace do. The tables make real files, links, FIFOs
and processes, expect a POSIX shell and the common utilities (`sh`,
`cat`, `sleep`, `pwd`) on the workspace's `PATH`, and bound every wait,
so a process that was not ended fails rather than hangs.

**A server in another language** is checked through a Go client of
its wire. A server that speaks remote's `/v1` is dialled with
`remote.Dial` and run under `Run`, `RunStarter` and
`RunServerProvider`, which is how the remote and tunnel bindings are
themselves checked. A server that speaks mcpworkspace's methods is
reached with the mcpworkspace client: `RunServerProvider` runs over it
today, while its process and file methods are checked by mcpworkspace's
own tests rather than by the shared tables, since its client is not a
workspace (Open: #33). A fixture corpus of wire exchanges, checkable
without Go, is an open question.

Open: #30 will hold a container backend to `Run` and `RunStarter`, to
[section 12](#12-descriptor)'s members, and to ending its instance at
close.

## 17. Versioning

This document is versioned with the Go module. A draft number changes
when a rule is added or changed; each draft records its changes in a
"Changes since" section. The module is pre-1.0, so a minor version may
change its API; a change that makes a conforming backend
non-conforming is listed in the changelog as breaking. A wire's version
is its path or prefix (`/v1`, `agentworkspace/process.`); a change a
conforming client of the old wire cannot read takes a new one.

## 18. Prior art

- **OpenHands workspace.** A workspace as a small value with a file
  system, one-shot execution and a descriptor, local or remote; the
  study in `../examples/openhands-workspace` this interface came from.
- **The Agent Client Protocol** gives the client file system and
  terminal methods an agent calls, and a session's additional
  directories; the terminal and roots notes above follow it.
- **E2B and Daytona** sandboxes expose files, commands and processes
  over an API to a runtime elsewhere, the placement remote binds.
- **The MCP stdio transport** shuts a server down by closing its input,
  waiting, then terminating and killing it, which Close follows.
- **agentsession RFC 0001** is the record a descriptor lands in, and
  defines the substitution rule that compares it.

## 19. Open questions

The issues that will add or change a rule here:

- #3: the descriptor's `host` and `instance` members, and which kind
  sets which ([section 12](#12-descriptor)).
- #5, the wire half: remote carrying an interrupted Exec's output, by an
  exec key and a cancel route ([section 6](#6-exec)).
- #9: an offered server's isolation, beyond the trust rule
  ([section 13](#13-isolation-statement)); waits for a second product.
- #23: the `secrets` mark on a listed server
  ([section 11](#11-servers-offered)).
- #29: terminal mode, with size and resize ([section 7](#7-processes)).
- #30: a container backend: conformance, descriptor, its instance
  ended at close.
- #31: `connect(port)` ([section 9](#9-ports)).
- #32: descendants that left the process group or session
  ([section 8](#8-ending-a-process)).
- #33: Exec over mcpworkspace's process methods
  ([sections 6](#6-exec) and [14](#mcpworkspace)).
- #34: named roots ([section 10](#10-roots)).

Where the code and its doc comments disagree today. This draft does
not pick a side; each is settled by the issue named, or by a later
draft:

- **"Everything it started".** The doc comments of `Command.Timeout`,
  `Starter.Start`, `Process.Signal` and `Process.Close`, and the README
  for Exec, promise that an end reaches everything the process started;
  `Local` signals the process group, so a descendant that called
  `setsid` or `setpgid` outlives it, for Exec and Start alike (#32).
- **`Descriptor.Ref`.** Its doc comment lists "a host, an instance ID"
  among what `Ref` may hold; agentsession 0.14 gives the host and the
  instance members of their own and says `ref` is absent for `local`
  (#3).
- **`Workspace.Root`.** Its doc comment says it is the absolute path of
  the working directory; a remote client dialled by a principal without
  `read` is told the root's last element alone, so its `Root()`, and
  its `Served().Root`, are not absolute.
- **Error kinds over mcpworkspace.** remote maps every kind of
  [section 2](#error-kinds) both ways; mcpworkspace's file errors have
  five kinds, so *exist*, *unsupported* and *closed* reach its client as
  `other`, and a test for the kind does not hold there as it does over
  remote.
- **Error messages over mcpworkspace.** remote sends a mapped error's
  kind words alone, so a served error's text, which may name a path on
  the serving side's machine, never crosses; mcpworkspace's file
  errors carry the served error's text (at most `MaxErrorBytes`), as
  `FileError`'s doc says ("the server's words"). Whether the contract
  requires remote's rule of every wire is open.
- **Signals.** remote carries thirteen signal names, mcpworkspace
  three, and `Local` any the system has. The contract names no minimum
  set a starter must carry.

Questions with no issue yet:

- **A wire fixture corpus.** A non-Go server is checked only through a
  Go client. A corpus of request and reply exchanges per wire, as
  agenttool keeps for schemas, would let another language check one
  without Go.

## Changes since draft 0.1

- [Section 5](#5-commands): argv resolution (#4). A bare `args[0]` is
  looked up in the `PATH` of the process's own environment, absolute
  entries alone; with no `PATH` it is not found.
- [Section 4](#4-environment): a process's environment (#41). The
  command's entries are over the workspace's name by name, and each
  name reaches the process once, with the last value given it.
- [Section 4](#4-environment): a workspace reports the environment it
  was given and removes nothing; keeping secrets out is the program's
  (#37). `Workspace.Env`'s doc comment no longer says the provider's key
  is already removed.
- [Section 6](#6-exec): an interrupted Exec (#5) ends what the command
  started and returns the output so far, with exit status −1 and
  *interrupted*, beside the context's error. A wire MUST carry it;
  remote's change is pending.
- [Section 8](#8-ending-a-process): one ladder for every way a process
  is ended (#5): INT, then TERM, then KILL, a grace apart, with Close
  closing input first. The reference binding sent TERM then KILL for a
  started process and KILL at once for Exec.
