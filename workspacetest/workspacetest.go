// Package workspacetest is the conformance tables every
// agentworkspace backend runs: Run, the rules every Workspace keeps
// (confinement, nothing blocking on a FIFO, Env a copy, Exec, names
// that are not valid paths),
// RunStarter, the rules every Starter keeps (the pipes, the
// environment, the lifetimes, the refusals), and RunServerProvider,
// the rules every ServerProvider keeps (the list, the pipes,
// ErrNoServer, the lifetimes, standard error, secrets kept out of the
// output). The root's own backends,
// remote and every nested module's backend run the same tables from
// their own tests, so a backend in a module of its own is held to
// what Local is. It imports the standard library and the root alone.
//
// The tables make real files, links, FIFOs and processes under
// t.TempDir(), and every wait is bounded, so a process that was not
// ended fails the test rather than hangs it.
package workspacetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// Backend is a workspace the tables run on. Open opens one over dir,
// with PATH alone in its environment, and closes it when the test
// ends; Links says its FS can read links (an fs.ReadLinkFS); Wire says
// it is the client side of a wire, whose refusals name no path of the
// serving side's machine (RFC 0001 §15).
type Backend struct {
	Name  string
	Open  func(t *testing.T, dir string) agentworkspace.Workspace
	Links bool
	Wire  bool
	// CloseServed, when set, closes the workspace ws, one Open gave,
	// runs its commands in: for the client side of a wire whose own
	// Close leaves the served workspace open, as remote's and the
	// tunnel's do, the served one. Nil is ws.Close.
	CloseServed func(t *testing.T, ws agentworkspace.Workspace)
}

// Run runs every table a Workspace keeps (confinement, a FIFO, Env,
// Exec, names that are not valid paths, the descriptor, concurrent use) on every backend, each a subtest
// named after the table and then the backend.
func Run(t *testing.T, backends ...Backend) {
	for _, table := range []struct {
		name string
		run  func(*testing.T, Backend)
	}{
		{"EveryWayOutIsRefused", everyWayOutIsRefused},
		{"InsideWorks", insideWorks},
		{"NothingBlocksOnAFIFO", nothingBlocksOnAFIFO},
		{"EnvIsACopy", envIsACopy},
		{"Exec", execRuns},
		{"ExecLeftoversEndAtClose", execLeftoversEndAtClose},
		{"NamesThatAreNotValidPaths", namesThatAreNotValidPaths},
		{"Concurrently", concurrently},
		{"RefusalsNameNoServedPath", refusalsNameNoServedPath},
		{"Connect", connects},
	} {
		t.Run(table.name, func(t *testing.T) {
			for _, b := range backends {
				t.Run(b.Name, func(t *testing.T) { table.run(t, b) })
			}
		})
	}
}

// Confined lays out a workspace beside a directory that holds a
// secret (outside/secret.txt, "hunter2"), with links in the workspace
// that lead out (linkdir, linkfile, sub/up) and one that stays in
// (alias.txt, to inside.txt, "ok"). dir is the workspace's directory.
func Confined(t *testing.T) (dir, outside string) {
	t.Helper()
	base := t.TempDir()
	dir, outside = filepath.Join(base, "work"), filepath.Join(base, "outside")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []string{dir, outside, filepath.Join(dir, "sub")} {
		must(os.MkdirAll(d, 0o755))
	}
	must(os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("hunter2"), 0o644))
	must(os.WriteFile(filepath.Join(dir, "inside.txt"), []byte("ok"), 0o644))
	must(os.Symlink(outside, filepath.Join(dir, "linkdir")))                               // a directory, out
	must(os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(dir, "linkfile"))) // a file, out
	must(os.Symlink("../../outside", filepath.Join(dir, "sub", "up")))                     // relative, out
	must(os.Symlink("inside.txt", filepath.Join(dir, "alias.txt")))                        // stays in
	return dir, outside
}

// within fails the test if f takes longer than d, as a process that
// was not ended would.
func within(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s took longer than %v", what, d)
	}
}

// Every way out of the root is ErrOutside, for reads, writes, removes
// and a command's directory, and nothing outside is read or changed.
func everyWayOutIsRefused(t *testing.T, b Backend) {
	dir, outside := Confined(t)
	ws := b.Open(t, dir)
	ctx := context.Background()
	secret := filepath.Join(outside, "secret.txt")
	fsys := ws.FS()
	read := func(name string) error {
		b, err := fs.ReadFile(fsys, name)
		if strings.Contains(string(b), "hunter2") {
			t.Errorf("read %s leaked the secret", name)
		}
		return err
	}
	cases := map[string]error{}
	for _, name := range []string{
		secret,                         // absolute
		"../outside/secret.txt",        // climbs
		"sub/../../outside/secret.txt", // climbs after a name
		"linkdir/secret.txt",           // through a link to a directory
		"linkfile",                     // a link to a file
		"sub/up/secret.txt",            // through a relative link
	} {
		cases["read "+name] = read(name)
		cases["stat "+name] = func() error { _, err := fs.Stat(fsys, name); return err }()
		cases["Read "+name] = func() error { _, err := agentworkspace.Read(ws, name, 1<<20); return err }()
	}
	for _, name := range []string{outside, "..", "linkdir", "sub/up"} {
		cases["readdir "+name] = func() error { _, err := fs.ReadDir(fsys, name); return err }()
		cases["exec in "+name] = func() error {
			_, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"pwd"}, Dir: name})
			return err
		}()
	}
	for _, name := range []string{
		filepath.Join(outside, "new.txt"),
		"../outside/new.txt",
		"linkdir/new.txt",
		"linkdir/a/b.txt", // directories made through a link
		"linkfile",        // over a link to a file
		"sub/up/new.txt",
	} {
		cases["write "+name] = ws.WriteFile(ctx, name, []byte("x"), 0o644)
	}
	for _, name := range []string{secret, "../outside/secret.txt", "linkdir/secret.txt", "sub/up/secret.txt"} {
		cases["remove "+name] = ws.Remove(ctx, name)
	}
	for what, err := range cases {
		if !errors.Is(err, agentworkspace.ErrOutside) {
			t.Errorf("%s: err = %v, want ErrOutside", what, err)
		}
	}
	entries, _ := os.ReadDir(outside)
	if b, _ := os.ReadFile(secret); len(entries) != 1 || string(b) != "hunter2" {
		t.Errorf("outside changed: %v, secret %q", entries, b)
	}
}

// What stays inside works, a link that stays in included, and a
// command's directory is the workspace's own name for it.
func insideWorks(t *testing.T, b Backend) {
	dir, outside := Confined(t)
	ws := b.Open(t, dir)
	ctx := context.Background()
	for _, name := range []string{"inside.txt", "alias.txt"} {
		if got, err := fs.ReadFile(ws.FS(), name); err != nil || string(got) != "ok" {
			t.Errorf("read %s = %q, %v", name, got, err)
		}
	}
	if err := ws.WriteFile(ctx, "new/dir/f.txt", []byte("made"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "new/dir/f.txt")); string(got) != "made" {
		t.Errorf("write landed %q", got)
	}
	if err := ws.Remove(ctx, "new/dir/f.txt"); err != nil {
		t.Errorf("remove: %v", err)
	}
	out, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"pwd"}, Dir: "sub"})
	if want := filepath.Join(ws.Root(), "sub") + "\n"; err != nil || string(out.Stdout) != want {
		t.Errorf("pwd in sub = %q, %v; want %q", out.Stdout, err, want)
	}
	if d := ws.Descriptor(); d.Root != ws.Root() {
		t.Errorf("Descriptor.Root = %q, Root = %q", d.Root, ws.Root())
	}
	describes(t, ws.Descriptor())
	// Where links lead is read through the workspace, or not at all:
	// never followed on the host.
	links, ok := ws.FS().(fs.ReadLinkFS)
	if !b.Links {
		if ok {
			t.Error("an FS that cannot read links offers ReadLinkFS")
		}
		return
	}
	if !ok {
		t.Fatal("FS is not a ReadLinkFS")
	}
	if fi, err := links.Lstat("alias.txt"); err != nil || fi.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("Lstat alias.txt = %v, %v", fi, err)
	}
	if target, err := links.ReadLink("linkdir"); err != nil || target != outside {
		t.Errorf("ReadLink linkdir = %q, %v", target, err)
	}
}

// describes checks d has the members its kind must: a local workspace
// names its host and has no ref, a container its image and its
// instance, a remote its endpoint.
func describes(t *testing.T, d agentworkspace.Descriptor) {
	t.Helper()
	// A workspace whose files may be read tells its root, absolute.
	if !path.IsAbs(d.Root) {
		t.Errorf("Descriptor.Root = %q, want an absolute path", d.Root)
	}
	switch d.Kind {
	case agentworkspace.KindLocal:
		if d.Ref != "" || d.Host == "" {
			t.Errorf("a local Descriptor = %+v, want a Host and no Ref", d)
		}
	case agentworkspace.KindContainer:
		if d.Ref == "" || d.Instance == "" {
			t.Errorf("a container Descriptor = %+v, want a Ref and an Instance", d)
		}
	case agentworkspace.KindRemote:
		if d.Ref == "" {
			t.Errorf("a remote Descriptor = %+v, want a Ref", d)
		}
	default:
		t.Errorf("Descriptor.Kind = %q", d.Kind)
	}
}

// The workspace's methods are safe for concurrent use, and its
// Descriptor and Root answer the same every time.
func concurrently(t *testing.T, b Backend) {
	ws := b.Open(t, t.TempDir())
	ctx := context.Background()
	d, root := ws.Descriptor(), ws.Root()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			name := fmt.Sprintf("f%d.txt", i)
			if err := ws.WriteFile(ctx, name, []byte(name), 0o644); err != nil {
				t.Error(err)
				return
			}
			if b, err := fs.ReadFile(ws.FS(), name); err != nil || string(b) != name {
				t.Errorf("read %s = %q, %v", name, b, err)
			}
			if _, err := fs.Stat(ws.FS(), name); err != nil {
				t.Error(err)
			}
			if got := ws.Descriptor(); got != d || ws.Root() != root {
				t.Errorf("Descriptor = %+v, then %+v", d, got)
			}
			ws.Env()
			if err := ws.Remove(ctx, name); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

// Nothing blocks on a FIFO: it opens, or fails, at once, what was
// opened says what it is, and it is not read as a file, listed or
// written.
func nothingBlocksOnAFIFO(t *testing.T, b Backend) {
	dir := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(dir, "pipe"), 0o644); err != nil {
		t.Skipf("no FIFO here: %v", err)
	}
	ws := b.Open(t, dir)
	ctx := context.Background()
	within(t, 10*time.Second, "the operations on a FIFO", func() {
		if f, err := ws.FS().Open("pipe"); err == nil {
			if fi, err := f.Stat(); err != nil || fi.Mode()&fs.ModeNamedPipe == 0 {
				t.Errorf("Stat of the opened FIFO = %v, %v", fi, err)
			}
			f.Read(make([]byte, 1)) // returns, whatever it says
			f.Close()
		}
		if fi, err := fs.Stat(ws.FS(), "pipe"); err != nil || fi.Mode()&fs.ModeNamedPipe == 0 {
			t.Errorf("Stat of the FIFO = %v, %v", fi, err)
		}
		if _, err := agentworkspace.Read(ws, "pipe", 1<<20); err == nil {
			t.Error("Read read a FIFO")
		}
		if _, err := fs.ReadDir(ws.FS(), "pipe"); err == nil {
			t.Error("a FIFO was listed")
		}
		if err := ws.WriteFile(ctx, "pipe", []byte("x"), 0o644); err == nil {
			t.Error("a FIFO was written")
		}
	})
}

// Env is a copy: a caller that changes it changes nothing the
// workspace gives next.
func envIsACopy(t *testing.T, b Backend) {
	ws := b.Open(t, t.TempDir())
	e := ws.Env()
	if len(e) == 0 {
		t.Fatal("no environment")
	}
	want := e[0]
	e[0] = "CHANGED=1"
	if got := ws.Env(); got[0] != want {
		t.Errorf("Env after a caller changed its copy = %q", got)
	}
}

// Execer runs one command to completion, as Workspace.Exec does: what
// the Exec table needs of a backend, so that a client of a wire that
// carries Exec but not a whole workspace (mcpworkspace's) is held to
// it too. An Execer with a Reach method says its reach with it, as a
// workspace does.
type Execer interface {
	Exec(ctx context.Context, cmd agentworkspace.Command) (*agentworkspace.Output, error)
}

// ExecBackend is an Execer the Exec table runs on. Open opens one over
// a workspace on dir whose environment is env, and closes it when the
// test ends; Wire is Backend's.
type ExecBackend struct {
	Name string
	Open func(t *testing.T, dir string, env []string) Execer
	Wire bool
	// CloseServed, when set, closes the workspace ws, one Open gave,
	// runs its commands in; nil is ws's Close, when it has one, and
	// otherwise the table that needs it is skipped.
	CloseServed func(t *testing.T, ws Execer)
}

// RunExec runs the Exec table Run runs (status, streams, Dir, Env,
// argv resolution, Stdin, a timeout, an interrupt, a descendant that
// left the group) on every backend, each a subtest named after the
// backend. The workspace's root is its Root, when the Execer has one,
// and otherwise what pwd prints in it.
func RunExec(t *testing.T, backends ...ExecBackend) {
	for _, b := range backends {
		t.Run(b.Name, func(t *testing.T) {
			dir := t.TempDir()
			env := []string{"PATH=" + os.Getenv("PATH")}
			ws := b.Open(t, dir, env)
			execTable(t, dir, ws, env)
			if b.Wire {
				t.Run("refusals name no served path", func(t *testing.T) {
					noServedPath(t, dir, execRefusals(t, dir, ws))
				})
			}
			t.Run("leftovers end at close", func(t *testing.T) {
				dir := t.TempDir()
				ws := b.Open(t, dir, env)
				closeWS := func() {
					if c, ok := ws.(io.Closer); ok {
						c.Close()
					}
				}
				switch _, closer := ws.(io.Closer); {
				case b.CloseServed != nil:
					closeWS = func() { b.CloseServed(t, ws) }
				case !closer:
					t.Skip("no way to close the workspace")
				}
				leftoversEndAtClose(t, dir, ws, closeWS)
			})
		})
	}
}

// What an Exec's command left running when it exited ends at the
// workspace's close at the latest, by the ladder (RFC 0001 §6, §8).
func execLeftoversEndAtClose(t *testing.T, b Backend) {
	dir := t.TempDir()
	ws := b.Open(t, dir)
	closeWS := func() { ws.Close() }
	if b.CloseServed != nil {
		closeWS = func() { b.CloseServed(t, ws) }
	}
	leftoversEndAtClose(t, dir, ws, closeWS)
}

// leftoversEndAtClose runs, through an Exec in a workspace on dir, a
// command that leaves beat.sh running in its group, its output its
// own, then closes the workspace with closeWS: the beat is gone within
// the ladder's graces. A backend may end it sooner, as mcpworkspace's
// composed Exec does at its return.
func leftoversEndAtClose(t *testing.T, dir string, ws Execer, closeWS func()) {
	os.Remove(filepath.Join(dir, "beat"))
	if err := os.WriteFile(filepath.Join(dir, "beat.sh"), []byte(beatScript), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := ws.Exec(context.Background(), agentworkspace.Command{Args: []string{"sh", "-c", "sh beat.sh </dev/null >/dev/null 2>&1 & echo started"}})
	if err != nil || string(out.Stdout) != "started\n" {
		t.Fatalf("Exec = %+v, %v", out, err)
	}
	if !beating(t, dir) {
		t.Log("what the Exec left was ended at its return")
	}
	within(t, 15*time.Second, "closing the workspace", closeWS)
	if beating(t, dir) {
		t.Error("what the Exec left running outlived the workspace's close")
	}
}

// Exec runs to completion: status, streams, Dir, Env, Stdin, a
// timeout that kills what the command started, a stream, and a cancel.
func execRuns(t *testing.T, b Backend) {
	dir := t.TempDir()
	ws := b.Open(t, dir)
	execTable(t, dir, ws, ws.Env())
}

// rootOf is ws's Root, or, for an Execer with none, the directory its
// commands run in.
func rootOf(t *testing.T, ws Execer) string {
	t.Helper()
	if r, ok := ws.(interface{ Root() string }); ok {
		return r.Root()
	}
	out, err := ws.Exec(context.Background(), agentworkspace.Command{Args: []string{"pwd"}})
	if err != nil || out.ExitCode != 0 {
		t.Fatalf("pwd at the root: %+v, %v", out, err)
	}
	return strings.TrimSuffix(string(out.Stdout), "\n")
}

// reachOf is how far ws's end of a process reaches, as it says with a
// Reach method, or "".
func reachOf(ws Execer) agentworkspace.Reach {
	if r, ok := ws.(interface{ Reach() agentworkspace.Reach }); ok {
		return r.Reach()
	}
	return ""
}

// execTable is the Exec table on ws, a workspace on dir whose
// environment is env.
func execTable(t *testing.T, dir string, ws Execer, env []string) {
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	hello(t, dir)
	root := rootOf(t, ws)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		cmd  agentworkspace.Command
		want agentworkspace.Output
	}{
		{"exit status and both streams", agentworkspace.Command{Args: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}}, agentworkspace.Output{Stdout: []byte("out\n"), Stderr: []byte("err\n"), ExitCode: 3}},
		{"Dir below the root", agentworkspace.Command{Args: []string{"pwd"}, Dir: "sub"}, agentworkspace.Output{Stdout: []byte(filepath.Join(root, "sub") + "\n")}},
		{"Env added", agentworkspace.Command{Args: []string{"sh", "-c", "echo $X"}, Env: []string{"X=y"}}, agentworkspace.Output{Stdout: []byte("y\n")}},
		{"Stdin", agentworkspace.Command{Args: []string{"cat"}, Stdin: []byte("in")}, agentworkspace.Output{Stdout: []byte("in")}},
		{"a bare name on the command's PATH", agentworkspace.Command{Args: []string{"hello"}, Env: []string{"PATH=" + path.Join(root, "bin")}}, agentworkspace.Output{Stdout: []byte("hello\n")}},
		{"Timeout kills what it started", agentworkspace.Command{Args: []string{"sh", "-c", "sleep 30 & sleep 30"}, Timeout: 200 * time.Millisecond}, agentworkspace.Output{ExitCode: -1, TimedOut: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			got, err := ws.Exec(ctx, tc.cmd)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Stdout, tc.want.Stdout) || !bytes.Equal(got.Stderr, tc.want.Stderr) || got.ExitCode != tc.want.ExitCode || got.TimedOut != tc.want.TimedOut {
				t.Errorf("Exec = %+v (%q, %q), want %+v", got, got.Stdout, got.Stderr, tc.want)
			}
			if time.Since(start) > 10*time.Second {
				t.Errorf("took %v", time.Since(start))
			}
		})
	}
	// The command's environment is over the workspace's name by name,
	// and each name reaches the process once.
	cmd := agentworkspace.Command{Args: []string{"sh", "-c", `env | grep -c '^X='; echo $X; env | grep -c '^PATH='`}, Env: []string{"X=1", "X=2"}}
	for _, kv := range env {
		if strings.HasPrefix(kv, "PATH=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if out, err := ws.Exec(ctx, cmd); err != nil || string(out.Stdout) != "1\n2\n1\n" {
		t.Errorf("a name set twice by the command, and one set by both: %q, %v; want once each, the last value", out.Stdout, err)
	}
	// Both to the stream, in their order; the pause keeps it over a wire
	// that carries the two apart.
	var stream bytes.Buffer
	out, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"sh", "-c", "echo a; sleep 0.2; echo b >&2"}, Stream: &stream})
	if err != nil || stream.String() != "a\nb\n" || len(out.Stdout)+len(out.Stderr) != 0 {
		t.Errorf("a stream got %q and the output kept %q %q (%v)", stream.String(), out.Stdout, out.Stderr, err)
	}
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"pwd"}, Dir: "../"}); !errors.Is(err, agentworkspace.ErrOutside) {
		t.Errorf("a Dir out of the root: err = %v", err)
	}
	programNotFound(t, func(c agentworkspace.Command) error {
		_, err := ws.Exec(ctx, c)
		return err
	})
	execCutShort(t, ws)
	// A Dir that is there but is not a directory is invalid, before
	// anything runs (RFC 0001 §5).
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"pwd"}, Dir: "file.txt"}); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("a Dir that is a file: err = %v, want fs.ErrInvalid", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ws.Exec(cctx, agentworkspace.Command{Args: []string{"sleep", "5"}}); err == nil {
		t.Error("a cancelled context ran the command to the end")
	}
	execInterrupted(t, ws)
	execEndsWhatLeftItsGroup(t, dir, ws)
}

// A command that exits while what it left running holds its output
// open gives the output so far and its own exit status beside an error
// of its own kind, which is exec.ErrWaitDelay too (RFC 0001 §6). A
// backend may instead end what a command left as the command exits, as
// mcpworkspace's composed Exec does a background job in its group: it
// then returns at once, with no error. One that left the group (setsid)
// is held over on every backend whose end reaches no orphan.
func execCutShort(t *testing.T, ws Execer) {
	ctx := context.Background()
	check := func(t *testing.T, script string, mayEnd bool) {
		var out *agentworkspace.Output
		var err error
		start := time.Now()
		within(t, 15*time.Second, "the Exec", func() {
			out, err = ws.Exec(ctx, agentworkspace.Command{Args: []string{"sh", "-c", script}})
		})
		ended := mayEnd && err == nil && time.Since(start) < grace
		if !ended && (!errors.Is(err, agentworkspace.ErrCutShort) || !errors.Is(err, exec.ErrWaitDelay)) {
			t.Errorf("err = %v after %v, want ErrCutShort and exec.ErrWaitDelay", err, time.Since(start))
		}
		if out == nil || string(out.Stdout) != "hi\n" || out.ExitCode != 0 || out.Interrupted || out.TimedOut {
			t.Errorf("Output = %+v, want what it wrote, hi, and status 0", out)
		}
	}
	t.Run("output held by what the command left", func(t *testing.T) {
		check(t, "sleep 10 & echo hi", true)
	})
	t.Run("output held by what left the group", func(t *testing.T) {
		if _, err := exec.LookPath("setsid"); err != nil {
			t.Skip("no setsid here")
		}
		// The pause lets setsid leave the group before the shell exits,
		// which would otherwise end it with the group.
		check(t, "setsid sleep 10 & sleep 0.5; echo hi", false)
	})
}

// programNotFound checks that run, an Exec or a Start, refuses a
// program that is not there, by a bare name on no PATH entry or by a
// path, as fs.ErrNotExist, naming the program as given (RFC 0001 §5).
func programNotFound(t *testing.T, run func(agentworkspace.Command) error) {
	t.Helper()
	for what, name := range map[string]string{
		"a bare name on no PATH entry":  "agentworkspace-missing-program",
		"a missing program by its path": "./agentworkspace-missing-program",
	} {
		err := run(agentworkspace.Command{Args: []string{name}})
		if !errors.Is(err, fs.ErrNotExist) || !strings.Contains(fmt.Sprint(err), name) {
			t.Errorf("%s: err = %v, want one that is fs.ErrNotExist and names %s", what, err, name)
		}
	}
}

// A timeout ends what the command started that left its group, as
// startEndsWhatLeftItsGroup's Close does.
func execEndsWhatLeftItsGroup(t *testing.T, dir string, ws Execer) {
	t.Run("a timeout ends what left the group", func(t *testing.T) {
		leavesItsGroup(t, ws, dir)
		var out *agentworkspace.Output
		var err error
		within(t, 15*time.Second, "the Exec", func() {
			out, err = ws.Exec(context.Background(), agentworkspace.Command{Args: []string{"sh", "-c", "setsid sh beat.sh & sleep 30"}, Timeout: time.Second})
		})
		if err != nil || !out.TimedOut {
			t.Fatalf("Exec = %+v, %v; want it timed out", out, err)
		}
		if beating(t, dir) {
			t.Error("the child that left the group outlived the timeout")
		}
	})
}

// beatScript beats into the file beat until it is killed, or for 30
// seconds, ignoring SIGINT and SIGTERM.
const beatScript = `trap '' INT TERM HUP
i=0
while [ $i -lt 300 ]; do
	i=$((i+1))
	echo $i > beat.tmp && mv beat.tmp beat
	sleep 0.1
done
`

// leavesItsGroup writes beat.sh into dir, skipping the test where a
// child cannot leave its group (no setsid on this PATH) or where ws
// says its end of a process does not reach one that has.
func leavesItsGroup(t *testing.T, ws Execer, dir string) {
	t.Helper()
	if reachOf(ws) == agentworkspace.ReachGroup {
		t.Skip("this workspace's end of a process reaches its group alone")
	}
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("no setsid here")
	}
	os.Remove(filepath.Join(dir, "beat"))
	if err := os.WriteFile(filepath.Join(dir, "beat.sh"), []byte(beatScript), 0o644); err != nil {
		t.Fatal(err)
	}
}

// beating reports whether beat.sh, run in dir, is still beating: its
// file changes within a second.
func beating(t *testing.T, dir string) bool {
	t.Helper()
	name := filepath.Join(dir, "beat")
	first, _ := os.ReadFile(name)
	for range 10 {
		time.Sleep(100 * time.Millisecond)
		if b, _ := os.ReadFile(name); len(b) > 0 && !bytes.Equal(b, first) {
			return true
		}
	}
	return false
}

// An Exec whose context ends while it runs ends the command, and what
// it started, and returns what it wrote until then, with ExitCode -1
// and Interrupted, beside the context's error: given to a Stream as it
// came, or kept in the Output.
func execInterrupted(t *testing.T, ws Execer) {
	// The pause keeps the two in order over a wire that carries them
	// apart, whose stream is given each as it arrives.
	cmd := agentworkspace.Command{Args: []string{"sh", "-c", "echo started; sleep 0.2; echo also >&2; sleep 30 & sleep 30"}}
	check := func(what string, out *agentworkspace.Output, err error) {
		t.Helper()
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s: err = %v, want context.Canceled", what, err)
		}
		if out == nil || out.ExitCode != -1 || !out.Interrupted || out.TimedOut {
			t.Errorf("%s: Output = %+v, want ExitCode -1 and Interrupted", what, out)
		}
	}

	t.Run("kept", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		time.AfterFunc(time.Second, cancel)
		var out *agentworkspace.Output
		var err error
		within(t, 10*time.Second, "the interrupted Exec", func() { out, err = ws.Exec(ctx, cmd) })
		check("kept", out, err)
		if out != nil && (string(out.Stdout) != "started\n" || string(out.Stderr) != "also\n") {
			t.Errorf("kept: the output so far = %q, %q", out.Stdout, out.Stderr)
		}
	})

	t.Run("streamed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream := &cancelAt{want: "started\nalso\n", cancel: cancel}
		c := cmd
		c.Stream = stream
		var out *agentworkspace.Output
		var err error
		within(t, 10*time.Second, "the interrupted Exec", func() { out, err = ws.Exec(ctx, c) })
		check("streamed", out, err)
		if got := stream.String(); got != "started\nalso\n" {
			t.Errorf("streamed: the stream got %q", got)
		}
		if out != nil && len(out.Stdout)+len(out.Stderr) != 0 {
			t.Errorf("streamed: the Output kept %q, %q", out.Stdout, out.Stderr)
		}
	})
}

// cancelAt is a Stream that calls cancel once it has been given want.
type cancelAt struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	want   string
	cancel context.CancelFunc
}

func (c *cancelAt) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.Write(p)
	if c.buf.String() == c.want {
		c.cancel()
	}
	return len(p), nil
}

func (c *cancelAt) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// A client of a wire is refused with the kind and words that name no
// path of the serving side's machine but a name in the workspace,
// relative to its root: neither the workspace's directory nor the one
// above it crosses in a refusal's text (RFC 0001 §15).
func refusalsNameNoServedPath(t *testing.T, b Backend) {
	if !b.Wire {
		t.Skip("not the client side of a wire")
	}
	dir, _ := Confined(t)
	ws := b.Open(t, dir)
	ctx := context.Background()
	refusals := execRefusals(t, dir, ws)
	_, refusals["open a missing name"] = ws.FS().Open("missing/x")
	_, refusals["list a file"] = fs.ReadDir(ws.FS(), "inside.txt")
	_, refusals["read through a link out"] = fs.ReadFile(ws.FS(), "linkfile")
	refusals["write under a file"] = ws.WriteFile(ctx, "inside.txt/x", []byte("x"), 0o644)
	refusals["remove a missing name"] = ws.Remove(ctx, "missing")
	refusals["remove a directory that is not empty"] = ws.Remove(ctx, "sub")
	noServedPath(t, dir, refusals)
}

// execRefusals are the refusals of commands that cannot run in a
// workspace on dir: a missing Dir, a Dir that is a file, a program
// that is not there by its path or on the PATH.
func execRefusals(t *testing.T, dir string, ws Execer) map[string]error {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	refusals := map[string]error{}
	for what, cmd := range map[string]agentworkspace.Command{
		"a missing Dir":           {Args: []string{"pwd"}, Dir: "missing"},
		"a Dir that is a file":    {Args: []string{"pwd"}, Dir: "file.txt"},
		"a missing program":       {Args: []string{"./missing"}},
		"a program on no PATH":    {Args: []string{"missing-program"}, Env: []string{"PATH=" + dir}},
		"a program that is a dir": {Args: []string{"./sub"}},
	} {
		os.MkdirAll(filepath.Join(dir, "sub"), 0o755)
		_, refusals[what] = ws.Exec(ctx, cmd)
	}
	return refusals
}

// noServedPath fails the test for a refusal that is missing, or whose
// text names dir or the directory above it.
func noServedPath(t *testing.T, dir string, refusals map[string]error) {
	t.Helper()
	paths := []string{filepath.Dir(dir)}
	if real, err := filepath.EvalSymlinks(filepath.Dir(dir)); err == nil {
		paths = append(paths, real)
	}
	for what, err := range refusals {
		if err == nil {
			t.Errorf("%s: no refusal", what)
			continue
		}
		for _, p := range paths {
			if strings.Contains(err.Error(), p) {
				t.Errorf("%s: %q names %s, a path of the serving side's", what, err, p)
			}
		}
	}
}

// hello writes bin/hello under dir, a program that prints "hello" and
// is on no PATH but one that names that directory.
func hello(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "hello"), []byte("#!/bin/sh\necho hello\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

// A name that is not fs.ValidPath is refused by every entry point: one
// that leaves is ErrOutside, any other fs.ErrInvalid, and neither
// reaches the file system.
func namesThatAreNotValidPaths(t *testing.T, b Backend) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "b"), []byte("x"), 0o644)
	ws := b.Open(t, dir)
	ctx := context.Background()
	ops := map[string]func(name string) error{
		"open":    func(n string) error { _, err := ws.FS().Open(n); return err },
		"stat":    func(n string) error { _, err := fs.Stat(ws.FS(), n); return err },
		"readdir": func(n string) error { _, err := fs.ReadDir(ws.FS(), n); return err },
		"write":   func(n string) error { return ws.WriteFile(ctx, n, []byte("y"), 0o644) },
		"remove":  func(n string) error { return ws.Remove(ctx, n) },
	}
	if b.Links {
		ops["lstat"] = func(n string) error { _, err := ws.FS().(fs.ReadLinkFS).Lstat(n); return err }
		ops["readlink"] = func(n string) error { _, err := ws.FS().(fs.ReadLinkFS).ReadLink(n); return err }
	}
	for name, want := range map[string]error{
		"/etc/passwd": agentworkspace.ErrOutside,
		"..":          agentworkspace.ErrOutside,
		"../b":        agentworkspace.ErrOutside,
		"a/../../b":   agentworkspace.ErrOutside,
		"a/../b":      fs.ErrInvalid,
		"./b":         fs.ErrInvalid,
		"b/":          fs.ErrInvalid,
		"":            fs.ErrInvalid,
	} {
		for op, f := range ops {
			if err := f(name); !errors.Is(err, want) {
				t.Errorf("%s %q: err = %v, want %v", op, name, err, want)
			}
		}
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "b")); string(b) != "x" {
		t.Errorf("b was changed to %q", b)
	}
	// A command's directory must be one.
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"pwd"}, Dir: "b"}); err == nil {
		t.Error("Exec ran in a file")
	}
}

// A Connector's stream reaches a TCP port on its loopback: what is
// written arrives, CloseWrite is end of file there, and what the port
// sends back is read; Forward listens on the loopback alone and
// carries each connection it accepts the same way; a port out of range
// is fs.ErrInvalid, and one nothing listens on is refused. The case
// listens on this machine's loopback, so it holds for a backend whose
// loopback is the test's; one whose loopback is not (a container's)
// runs a case of its own. A workspace that is no Connector is skipped.
func connects(t *testing.T, b Backend) {
	ws := b.Open(t, t.TempDir())
	if _, ok := ws.(agentworkspace.Connector); !ok {
		t.Skip("not an agentworkspace.Connector")
	}
	ctx := context.Background()
	port := Upper(t)
	c, err := agentworkspace.Connect(ctx, ws, port)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	within(t, 10*time.Second, "the stream", func() {
		if _, err := io.WriteString(c, "hello"); err != nil {
			t.Error(err)
		}
		if err := c.CloseWrite(); err != nil {
			t.Error(err)
		}
		if got, err := io.ReadAll(c); string(got) != "HELLO" || err != nil {
			t.Errorf("the port answered %q, %v", got, err)
		}
	})
	ln, err := agentworkspace.Forward(ctx, ws, port)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if a, ok := ln.Addr().(*net.TCPAddr); !ok || !a.IP.IsLoopback() {
		t.Errorf("Forward listens on %v, want the loopback alone", ln.Addr())
	}
	within(t, 10*time.Second, "the forwarded stream", func() {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer nc.Close()
		io.WriteString(nc, "abc")
		nc.(*net.TCPConn).CloseWrite()
		if got, err := io.ReadAll(nc); string(got) != "ABC" || err != nil {
			t.Errorf("the forwarded port answered %q, %v", got, err)
		}
	})
	if _, err := agentworkspace.Connect(ctx, ws, 0); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("port 0: %v, want fs.ErrInvalid", err)
	}
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unused := free.Addr().(*net.TCPAddr).Port
	free.Close()
	if c, err := agentworkspace.Connect(ctx, ws, unused); err == nil {
		c.Close()
		t.Errorf("a port nothing listens on was connected")
	}
}

// Upper listens on this machine's loopback until the test ends, and
// answers each connection with what it sent, upper-cased, once it has
// sent all of it; it returns the port.
func Upper(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				b, _ := io.ReadAll(c)
				c.Write(bytes.ToUpper(b))
			}()
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}
