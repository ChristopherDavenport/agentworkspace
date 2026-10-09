// Package agentworkspace is where an agent's tools act: a file tree,
// processes, a working directory and an environment. A session runs
// over one Workspace, whichever machine it is on, and every tool that
// touches files or runs a command goes through it, so a tool written
// for one runs unchanged on another. Local is the working directory on
// this machine; a container or a remote runtime is another
// implementation of the same interface.
//
// The interface is the one the openhands-workspace study settled on: a
// small value with a file system, one-shot execution and a descriptor
// the session records. A workspace that can run a process for longer
// than one call, with pipes to it, is also a Starter.
//
// The package is pre-1.0: its API may change in a minor version.
package agentworkspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
)

// ErrOutside is what a name that leaves the workspace gets, whether it
// names another directory outright, climbs out with "..", or goes out
// through a symbolic link.
var ErrOutside = errors.New("path is outside the workspace")

// Kinds a Descriptor names, as agentsession's env entry records them.
const (
	KindLocal     = "local"
	KindContainer = "container"
	KindRemote    = "remote"
)

// Workspace is where a session's tools act. Names given to it are
// fs.ValidPath names relative to Root; turning a path the model wrote
// into one is the tool's.
type Workspace interface {
	// Root is the absolute path of the working directory as the
	// workspace's own processes see it: the checkout on this machine,
	// /workspace in a container. It is what the model is told and what
	// the session records as its cwd.
	Root() string
	// FS reads the workspace's files. Opening a name never blocks: a
	// FIFO or a device opens, or fails, at once, and a tool checks what
	// it opened is a regular file. A name that leaves the workspace is
	// an error that errors.Is ErrOutside. An FS that can tell a link
	// from what it leads to implements fs.ReadLinkFS; one that cannot
	// leaves a check that must follow links unable to, and so asking.
	FS() fs.FS
	// WriteFile creates or replaces a regular file, creating the
	// directories above it. It refuses a FIFO or a device where the file
	// is, and does not block on one.
	WriteFile(ctx context.Context, name string, data []byte, perm fs.FileMode) error
	// Remove deletes a file or an empty directory.
	Remove(ctx context.Context, name string) error
	// Env is the environment every process in the workspace starts
	// with, the provider's key and other credentials already removed.
	Env() []string
	// Exec runs one command to completion in a fresh process.
	Exec(ctx context.Context, cmd Command) (*Output, error)
	// Descriptor is what the session records about where the tools ran.
	Descriptor() Descriptor
	// Close releases the workspace. It is used until Close and not after.
	Close() error
}

// Command is one process to run.
type Command struct {
	// Args is argv; Args[0] is resolved in the workspace.
	Args []string
	// Dir is relative to Root; "" is Root. A Dir that leaves the
	// workspace, by name or through a link, is ErrOutside.
	Dir string
	// Env is added to Workspace.Env.
	Env []string
	// Stdin is the process's standard input; nil is none. Start refuses
	// it: a started process's input is Process.Stdin.
	Stdin []byte
	// Timeout ends the process, and everything it started, after this
	// long; zero is no limit but the context's.
	Timeout time.Duration
	// Stream, when set, receives the output the caller is not otherwise
	// given, as it arrives. For Exec that is standard output and
	// standard error, interleaved, and Output holds neither. For Start
	// it is standard error, standard output being Process.Stdout; nil
	// discards it.
	Stream io.Writer
}

// Output is how a process ended.
type Output struct {
	Stdout, Stderr []byte
	// ExitCode is the process's status; -1 when it was killed.
	ExitCode int
	// TimedOut reports that Command.Timeout ended it.
	TimedOut bool
}

// Descriptor is what the session records about a workspace, in its env
// entry: agentsession's workspace kind and ref, and the root, which is
// the entry's cwd.
type Descriptor struct {
	// Kind is KindLocal, KindContainer or KindRemote.
	Kind string
	// Ref is what the harness resolves to it: an image digest, a host,
	// an instance ID; empty for the local machine.
	Ref string
	// Root is Workspace.Root.
	Root string
}

// Starter is a Workspace that can run a process for longer than one
// call, with pipes to it: an MCP server over stdio, a persistent shell.
// It is an interface of its own, as fs.ReadLinkFS is beside fs.FS, so
// that a workspace that only stores files, or a runtime whose API runs
// one command at a time, is still a Workspace; a caller asks for it
// with a type assertion, or calls Start.
type Starter interface {
	Workspace
	// Start starts cmd and returns at once. The process lives no longer
	// than its owner: Process.Close, the end of ctx, cmd.Timeout and
	// the workspace's Close each end it and everything it started, and
	// when it exits by itself, what it started goes with it. A caller
	// that is done with it calls Close, even after it exited.
	Start(ctx context.Context, cmd Command) (Process, error)
}

// Process is a process started in a workspace. Its methods are safe
// for concurrent use: one goroutine may read Stdout while another
// writes Stdin and a third waits.
type Process interface {
	// Stdin is the process's standard input. Closing it is end of file
	// for the process.
	Stdin() io.WriteCloser
	// Stdout is the process's standard output. It reaches end of file
	// once the process and everything it started have closed it.
	Stdout() io.ReadCloser
	// Wait waits for the process to exit and returns its status, -1 if
	// a signal ended it. err is the context's error when the context or
	// Command.Timeout ended it, and otherwise nil unless waiting itself
	// failed; an exit status other than zero is not an error. Wait may
	// be called any number of times and returns the same each time.
	Wait() (status int, err error)
	// Signal sends sig to the process and to everything it started. It
	// is os.ErrProcessDone once the process has exited.
	Signal(sig os.Signal) error
	// Close ends the process, if it is still running, and everything it
	// started, and releases the pipes. It first closes Stdin and gives
	// the process a moment to exit, as the MCP stdio transport's
	// shutdown asks, then terminates it, then kills it. It returns once
	// the process has exited, and may be called more than once.
	Close() error
}

// Start starts cmd in ws if ws is a Starter, and otherwise returns an
// error that errors.Is errors.ErrUnsupported.
func Start(ctx context.Context, ws Workspace, cmd Command) (Process, error) {
	s, ok := ws.(Starter)
	if !ok {
		return nil, fmt.Errorf("agentworkspace: a %s workspace cannot start a process: %w", ws.Descriptor().Kind, errors.ErrUnsupported)
	}
	return s.Start(ctx, cmd)
}

// Read is fs.ReadFile on w's files, at most max bytes of a regular
// file: a FIFO, a device or a directory is an error, and so is a file
// over max.
func Read(w Workspace, name string, max int64) ([]byte, error) {
	f, err := w.FS().Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", name)
	}
	if fi.Size() > max {
		return nil, fmt.Errorf("%s is %d bytes, over the %d-byte limit", name, fi.Size(), max)
	}
	return io.ReadAll(io.LimitReader(f, max+1))
}
