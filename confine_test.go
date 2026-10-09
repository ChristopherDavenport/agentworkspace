package agentworkspace

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// mapped stands in for a container: its processes and its files see
// the working directory as /workspace, whatever directory on this
// machine holds it. Paths in a command's arguments are mapped in, and
// paths in its output mapped back, as a container's mount does. It is
// not a Starter.
type mapped struct {
	backing *Local
	dir     string // the directory on this machine
}

const mappedRoot = "/workspace"

func (m *mapped) Root() string  { return mappedRoot }
func (m *mapped) FS() fs.FS     { return m.backing.FS() }
func (m *mapped) Env() []string { return m.backing.Env() }
func (m *mapped) Close() error  { return m.backing.Close() }
func (m *mapped) WriteFile(ctx context.Context, name string, data []byte, perm fs.FileMode) error {
	return m.backing.WriteFile(ctx, name, data, perm)
}
func (m *mapped) Remove(ctx context.Context, name string) error { return m.backing.Remove(ctx, name) }
func (m *mapped) Descriptor() Descriptor {
	return Descriptor{Kind: KindContainer, Ref: "sha256:test", Root: mappedRoot}
}

func (m *mapped) Exec(ctx context.Context, c Command) (*Output, error) {
	in := func(s string) string { return strings.ReplaceAll(s, mappedRoot, m.dir) }
	out := func(b []byte) []byte { return bytes.ReplaceAll(b, []byte(m.dir), []byte(mappedRoot)) }
	args := make([]string, len(c.Args))
	for i, a := range c.Args {
		args[i] = in(a)
	}
	c.Args = args
	stream := c.Stream
	var buf bytes.Buffer
	if stream != nil {
		c.Stream = &buf
	}
	o, err := m.backing.Exec(ctx, c)
	if err != nil {
		return nil, err
	}
	if stream != nil {
		stream.Write(out(buf.Bytes()))
	}
	o.Stdout, o.Stderr = out(o.Stdout), out(o.Stderr)
	return o, nil
}

// noLinks is a workspace whose FS cannot tell a link from what it leads
// to, as an FS over a plain file API may not.
type noLinks struct{ *Local }

func (n noLinks) FS() fs.FS { return struct{ fs.FS }{n.Local.FS()} }

// backends are the workspaces every confinement test runs on, each over
// dir.
var backends = []struct {
	name string
	open func(t *testing.T, dir string) Workspace
}{
	{"local", func(t *testing.T, dir string) Workspace { return mustLocal(t, dir) }},
	{"container stand-in", func(t *testing.T, dir string) Workspace {
		l := mustLocal(t, dir)
		return &mapped{backing: l, dir: l.Root()}
	}},
	{"no links", func(t *testing.T, dir string) Workspace { return noLinks{mustLocal(t, dir)} }},
}

func mustLocal(t *testing.T, dir string) *Local {
	t.Helper()
	l, err := NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// confined lays out a workspace beside a directory that holds a secret,
// with links in the workspace that lead out and one that stays in.
func confined(t *testing.T) (dir, outside string) {
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

// Every way out of the root is ErrOutside, on every backend, for reads,
// writes, removes and a command's directory, and nothing outside is
// read or changed.
func TestEveryWayOutIsRefused(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			dir, outside := confined(t)
			ws := b.open(t, dir)
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
				cases["Read "+name] = func() error { _, err := Read(ws, name, 1<<20); return err }()
			}
			for _, name := range []string{outside, "..", "linkdir", "sub/up"} {
				cases["readdir "+name] = func() error { _, err := fs.ReadDir(fsys, name); return err }()
				cases["exec in "+name] = func() error { _, err := ws.Exec(ctx, Command{Args: []string{"pwd"}, Dir: name}); return err }()
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
				if !errors.Is(err, ErrOutside) {
					t.Errorf("%s: err = %v, want ErrOutside", what, err)
				}
			}
			entries, _ := os.ReadDir(outside)
			if b, _ := os.ReadFile(secret); len(entries) != 1 || string(b) != "hunter2" {
				t.Errorf("outside changed: %v, secret %q", entries, b)
			}
		})
	}
}

// What stays inside works on every backend, a link that stays in
// included, and a command's directory is the workspace's own name for
// it.
func TestInsideWorks(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			dir, outside := confined(t)
			ws := b.open(t, dir)
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
			out, err := ws.Exec(ctx, Command{Args: []string{"pwd"}, Dir: "sub"})
			if want := filepath.Join(ws.Root(), "sub") + "\n"; err != nil || string(out.Stdout) != want {
				t.Errorf("pwd in sub = %q, %v; want %q", out.Stdout, err, want)
			}
			if d := ws.Descriptor(); d.Root != ws.Root() {
				t.Errorf("Descriptor.Root = %q, Root = %q", d.Root, ws.Root())
			}
			// Where links lead is read through the workspace, or not at
			// all: never followed on the host.
			links, ok := ws.FS().(fs.ReadLinkFS)
			if b.name == "no links" {
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
		})
	}
}

// A link swapped between a directory inside and one outside while it is
// read and written through never leads out: the root resolves each name
// as it opens it, not before.
func TestALinkSwappedMidOperationStaysIn(t *testing.T) {
	dir, outside := confined(t)
	if err := os.Mkdir(filepath.Join(dir, "in"), 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "in", "secret.txt"), []byte("inside"), 0o644)
	flip := filepath.Join(dir, "flip")
	os.Symlink("in", flip)
	ws := mustLocal(t, dir)
	ctx := context.Background()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tmp := filepath.Join(dir, "flip.tmp")
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			target := "in"
			if i%2 == 0 {
				target = outside
			}
			os.Remove(tmp)
			if os.Symlink(target, tmp) == nil {
				os.Rename(tmp, flip)
			}
		}
	})
	for range 500 {
		if b, err := fs.ReadFile(ws.FS(), "flip/secret.txt"); err == nil && string(b) != "inside" {
			t.Errorf("read through the swapped link got %q", b)
			break
		} else if err != nil && !errors.Is(err, ErrOutside) {
			t.Errorf("read through the swapped link: %v", err)
			break
		}
		ws.WriteFile(ctx, "flip/w.txt", []byte("x"), 0o644)
	}
	close(stop)
	wg.Wait()
	if _, err := os.Stat(filepath.Join(outside, "w.txt")); err == nil {
		t.Error("a write through the swapped link landed outside")
	}
}
