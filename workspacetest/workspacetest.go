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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// Backend is a workspace the tables run on. Open opens one over dir,
// with PATH alone in its environment, and closes it when the test
// ends; Links says its FS can read links (an fs.ReadLinkFS).
type Backend struct {
	Name  string
	Open  func(t *testing.T, dir string) agentworkspace.Workspace
	Links bool
}

// Run runs every table a Workspace keeps (confinement, a FIFO, Env,
// Exec, names that are not valid paths) on every backend, each a subtest
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
		{"NamesThatAreNotValidPaths", namesThatAreNotValidPaths},
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

// Exec runs to completion: status, streams, Dir, Env, Stdin, a
// timeout that kills what the command started, a stream, and a cancel.
func execRuns(t *testing.T, b Backend) {
	dir := t.TempDir()
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	ws := b.Open(t, dir)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		cmd  agentworkspace.Command
		want agentworkspace.Output
	}{
		{"exit status and both streams", agentworkspace.Command{Args: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}}, agentworkspace.Output{Stdout: []byte("out\n"), Stderr: []byte("err\n"), ExitCode: 3}},
		{"Dir below the root", agentworkspace.Command{Args: []string{"pwd"}, Dir: "sub"}, agentworkspace.Output{Stdout: []byte(filepath.Join(ws.Root(), "sub") + "\n")}},
		{"Env added", agentworkspace.Command{Args: []string{"sh", "-c", "echo $X"}, Env: []string{"X=y"}}, agentworkspace.Output{Stdout: []byte("y\n")}},
		{"Stdin", agentworkspace.Command{Args: []string{"cat"}, Stdin: []byte("in")}, agentworkspace.Output{Stdout: []byte("in")}},
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
	var stream bytes.Buffer
	out, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"sh", "-c", "echo a; echo b >&2"}, Stream: &stream})
	if err != nil || stream.String() != "a\nb\n" || len(out.Stdout)+len(out.Stderr) != 0 {
		t.Errorf("a stream got %q and the output kept %q %q (%v)", stream.String(), out.Stdout, out.Stderr, err)
	}
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"pwd"}, Dir: "../"}); !errors.Is(err, agentworkspace.ErrOutside) {
		t.Errorf("a Dir out of the root: err = %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ws.Exec(cctx, agentworkspace.Command{Args: []string{"sleep", "5"}}); err == nil {
		t.Error("a cancelled context ran the command to the end")
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
