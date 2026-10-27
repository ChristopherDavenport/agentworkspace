package agentworkspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Local is a directory on this machine. Every file operation goes
// through an os.Root, which the kernel-facing layer of the standard
// library keeps from following a name out of the directory, so a link
// inside it that points outside is refused rather than followed. A
// process it runs is not confined: a shell reaches whatever the user
// does, and the policy is what stands in front of it.
//
// Local runs on Unix: its processes run in process groups of their
// own, and its files are opened without blocking. Ending a process
// ends its group and, on Linux, the descendants that left it (Reach).
type Local struct {
	root *os.Root
	dir  string // as given, absolute and cleaned
	real string // dir with its links resolved
	env  []string

	mu     sync.Mutex
	closed bool
	procs  map[*localProcess]struct{} // started and not yet closed
}

var (
	_ Workspace = (*Local)(nil)
	_ Starter   = (*Local)(nil)
)

// NewLocal opens dir as a workspace whose processes start with env and
// nothing else: nil is an empty environment, not this process's. A
// product passes this process's environment without its credentials.
func NewLocal(dir string, env []string) (*Local, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		real = abs
	}
	return &Local{root: root, dir: abs, real: real, env: env, procs: map[*localProcess]struct{}{}}, nil
}

// Root is the directory, absolute and cleaned.
func (l *Local) Root() string { return l.dir }

// Real is the directory with its links resolved, in which an absolute
// path the model wrote may be spelled.
func (l *Local) Real() string { return l.real }

// Env is the environment its processes start with, a copy: a caller
// that changes it changes nothing a later process starts with.
func (l *Local) Env() []string { return slices.Clone(l.env) }

// Reach is ReachDescendants on Linux, where Local walks /proc for the
// descendants that left a process's group before it signals, and
// ReachGroup elsewhere, macOS among them, where it signals the group
// alone. On either, a descendant whose parent exited before the end
// began is not found.
func (l *Local) Reach() Reach { return reach }

// Descriptor names the local machine.
func (l *Local) Descriptor() Descriptor { return Descriptor{Kind: KindLocal, Root: l.dir} }

// Close ends every process Start started that is not closed yet, as
// Process.Close does, and releases the directory handle.
func (l *Local) Close() error {
	l.mu.Lock()
	l.closed = true
	procs := make([]*localProcess, 0, len(l.procs))
	for p := range l.procs {
		procs = append(procs, p)
	}
	l.mu.Unlock()
	var wg sync.WaitGroup
	for _, p := range procs {
		wg.Go(func() { p.Close() })
	}
	wg.Wait()
	return l.root.Close()
}

// FS reads through the root, opening without blocking.
func (l *Local) FS() fs.FS { return localFS{l.root} }

// WriteFile creates or replaces a regular file under the root.
func (l *Local) WriteFile(_ context.Context, name string, data []byte, perm fs.FileMode) error {
	if err := valid("write", name); err != nil {
		return err
	}
	if dir := path.Dir(name); dir != "." {
		if err := l.root.MkdirAll(dir, 0o755); err != nil {
			// A link in the way reads as "file exists"; the Stat says
			// where it leads.
			if _, serr := l.root.Stat(dir); serr != nil {
				err = serr
			}
			return outside(name, err)
		}
	}
	// Opened without blocking and checked before it is cut short: a
	// FIFO with no reader would otherwise hang the write, and a device
	// is nothing a tool should write.
	f, err := l.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|syscall.O_NONBLOCK, perm)
	if err != nil {
		return outside(name, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", name)
	}
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Close()
}

// Remove deletes a file or an empty directory under the root.
func (l *Local) Remove(_ context.Context, name string) error {
	if err := valid("remove", name); err != nil {
		return err
	}
	return outside(name, l.root.Remove(name))
}

// Exec runs a command in its own process group. When ctx ends, or
// Command.Timeout, before the command does, Exec ends it and what it
// started with Local's ladder (SIGINT, then SIGTERM, then SIGKILL, a
// grace apart; see Process) and returns the output so far, with
// ExitCode -1 and Interrupted or TimedOut. It waits at most two seconds
// past the command's exit for output its children still hold open.
func (l *Local) Exec(ctx context.Context, c Command) (*Output, error) {
	dir, env, prog, err := l.prepare(c)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	run := ctx // the caller's, bounded by the timeout
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		run, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	cmd := exec.Command(prog, c.Args[1:]...)
	cmd.Args[0] = c.Args[0]
	cmd.Dir, cmd.Env = dir, env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = waitDelay
	if c.Stdin != nil {
		cmd.Stdin = bytes.NewReader(c.Stdin)
	}
	var stdout, stderr bytes.Buffer
	if c.Stream != nil {
		cmd.Stdout, cmd.Stderr = c.Stream, c.Stream
	} else {
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	g := &group{pgid: cmd.Process.Pid}
	// The watch ends the command when run ends first. mu orders its
	// start against Wait's return, so the outcome says which came
	// first.
	var (
		mu     sync.Mutex
		waited bool
		cause  error // the caller's error, or the timeout's when it is nil
		ending bool
	)
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-run.Done():
		}
		mu.Lock()
		if waited {
			mu.Unlock()
			return
		}
		ending, cause = true, ctx.Err()
		mu.Unlock()
		g.end(func(d time.Duration) bool { return closedWithin(done, d) })
	}()
	err = cmd.Wait()
	mu.Lock()
	waited = true
	mu.Unlock()
	close(done)
	// Ended, what is left of the group goes; a command that exited by
	// itself keeps what it left running.
	g.reap(ending)
	out := &Output{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	switch {
	case ending && cause != nil:
		out.ExitCode, out.Interrupted = -1, true
		return out, cause
	case ending:
		out.ExitCode, out.TimedOut = -1, true
		return out, nil
	case err == nil:
		return out, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		out.ExitCode = ee.ExitCode()
		return out, nil
	}
	return nil, err
}

// waitDelay is how long a process's end waits for output that its
// children, killed or not, still hold open.
const waitDelay = 2 * time.Second

// prepare checks a command and returns the directory it runs in, its
// whole environment and the program it runs.
func (l *Local) prepare(c Command) (dir string, env []string, prog string, err error) {
	if len(c.Args) == 0 {
		return "", nil, "", errors.New("agentworkspace: no command")
	}
	dir = l.dir
	if c.Dir != "" {
		if !filepath.IsLocal(c.Dir) {
			return "", nil, "", fmt.Errorf("%w: %s", ErrOutside, c.Dir)
		}
		// Through the root, so a link that leads out is refused as a
		// name that does.
		fi, err := l.root.Stat(c.Dir)
		if err != nil {
			return "", nil, "", outside(c.Dir, err)
		}
		if !fi.IsDir() {
			return "", nil, "", fmt.Errorf("%s is not a directory", c.Dir)
		}
		dir = filepath.Join(l.dir, c.Dir)
	}
	env = processEnv(l.env, c.Env)
	if prog, err = lookPath(c.Args[0], env); err != nil {
		return "", nil, "", err
	}
	return dir, env, prog, nil
}

// processEnv is the environment a process starts with: the
// workspace's, with the command's over it name by name. Each name is
// there once, where it first appears, with the last value given it, so
// what the process sees is this rule's and not os/exec's. It is never
// nil, which exec takes as this process's whole environment, the
// credentials the workspace's was built without included.
func processEnv(ws, cmd []string) []string {
	env := make([]string, 0, len(ws)+len(cmd))
	at := make(map[string]int, len(ws)+len(cmd))
	for _, kv := range slices.Concat(ws, cmd) {
		name, _, _ := strings.Cut(kv, "=")
		if i, ok := at[name]; ok {
			env[i] = kv
			continue
		}
		at[name] = len(env)
		env = append(env, kv)
	}
	return env
}

// lookPath finds the program a command runs as execvp(3) does, but on
// the PATH of env, the environment the process starts with, rather
// than this process's, which is what exec.LookPath reads. A name with
// a slash is used as it is, relative to the command's Dir. A bare name
// is looked for in each absolute entry of PATH in turn; a relative
// entry, the empty one included, is skipped, so a file written into
// the workspace cannot stand in for a program (exec.ErrDot's hazard).
// With no PATH a bare name is not found: there is no default, since a
// container's would differ from this machine's.
func lookPath(name string, env []string) (string, error) {
	if strings.Contains(name, "/") {
		return name, nil
	}
	var pathList string
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			pathList = v
		}
	}
	if name != "" {
		for _, d := range filepath.SplitList(pathList) {
			if !filepath.IsAbs(d) {
				continue
			}
			if p := filepath.Join(d, name); executable(p) {
				return p, nil
			}
		}
	}
	return "", &exec.Error{Name: name, Err: exec.ErrNotFound}
}

// executable reports whether p is a file this process may execute.
func executable(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir() && syscall.Access(p, 1 /* X_OK */) == nil
}

// localFS is the root's files, opened without blocking.
type localFS struct{ root *os.Root }

var (
	_ fs.StatFS     = localFS{}
	_ fs.ReadDirFS  = localFS{}
	_ fs.ReadLinkFS = localFS{}
)

func (f localFS) Open(name string) (fs.File, error) {
	if err := valid("open", name); err != nil {
		return nil, err
	}
	file, err := f.root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, outside(name, err)
	}
	return file, nil
}

func (f localFS) Stat(name string) (fs.FileInfo, error) {
	if err := valid("stat", name); err != nil {
		return nil, err
	}
	fi, err := f.root.Stat(name)
	return fi, outside(name, err)
}

func (f localFS) Lstat(name string) (fs.FileInfo, error) {
	if err := valid("lstat", name); err != nil {
		return nil, err
	}
	fi, err := f.root.Lstat(name)
	return fi, outside(name, err)
}

func (f localFS) ReadLink(name string) (string, error) {
	if err := valid("readlink", name); err != nil {
		return "", err
	}
	s, err := f.root.Readlink(name)
	return s, outside(name, err)
}

// ReadDir lists a directory, opened as one and without blocking, so a
// FIFO or a device in its place is an error at once.
func (f localFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if err := valid("readdir", name); err != nil {
		return nil, err
	}
	file, err := f.root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, outside(name, err)
	}
	defer file.Close()
	return file.ReadDir(-1)
}

// valid refuses a name that is not fs.ValidPath: one that is absolute
// or climbs out with ".." leaves, and is ErrOutside as a link out is;
// any other ("a/../b", "a/") is fs.ErrInvalid.
func valid(op, name string) error {
	if fs.ValidPath(name) {
		return nil
	}
	if c := path.Clean(name); path.IsAbs(c) || c == ".." || strings.HasPrefix(c, "../") {
		return fmt.Errorf("%w: %s", ErrOutside, name)
	}
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
}

// outside names the path in an error the root raised for a name that
// leaves, so every way out reads the same and errors.Is ErrOutside.
func outside(name string, err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "path escapes from parent") {
		return fmt.Errorf("%w: %s", ErrOutside, name)
	}
	return err
}
