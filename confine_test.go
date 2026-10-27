package agentworkspace_test

// The root's backends in the conformance tables, workspacetest's, and
// the confinement tests of Local's own. They are in the external test
// package because workspacetest imports this one.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
)

// mapped stands in for a container: its processes and its files see
// the working directory as /workspace, whatever directory on this
// machine holds it. Paths in a command's arguments and environment
// are mapped in, and paths in its output mapped back, as a container's
// mount does. It is not a Starter.
type mapped struct {
	backing *agentworkspace.Local
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
func (m *mapped) Descriptor() agentworkspace.Descriptor {
	return agentworkspace.Descriptor{Kind: agentworkspace.KindContainer, Ref: "sha256:test", Root: mappedRoot}
}

func (m *mapped) Exec(ctx context.Context, c agentworkspace.Command) (*agentworkspace.Output, error) {
	in := func(s string) string { return strings.ReplaceAll(s, mappedRoot, m.dir) }
	out := func(b []byte) []byte { return bytes.ReplaceAll(b, []byte(m.dir), []byte(mappedRoot)) }
	args := make([]string, len(c.Args))
	for i, a := range c.Args {
		args[i] = in(a)
	}
	c.Args = args
	env := make([]string, len(c.Env))
	for i, e := range c.Env {
		env[i] = in(e)
	}
	c.Env = env
	if c.Stream != nil {
		// A pointer, so Local sees one writer for both streams and
		// keeps their order.
		c.Stream = &mappedWriter{c.Stream, out}
	}
	o, err := m.backing.Exec(ctx, c)
	if o == nil {
		return nil, err
	}
	o.Stdout, o.Stderr = out(o.Stdout), out(o.Stderr)
	return o, err
}

// mappedWriter maps paths back in each write as it passes, so a stream
// gets the output as it comes. A path split across two writes is not
// mapped, which the tables' short outputs never do.
type mappedWriter struct {
	w   io.Writer
	out func([]byte) []byte
}

func (m *mappedWriter) Write(p []byte) (int, error) {
	if _, err := m.w.Write(m.out(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// noLinks is a workspace whose FS cannot tell a link from what it leads
// to, as an FS over a plain file API may not.
type noLinks struct{ *agentworkspace.Local }

func (n noLinks) FS() fs.FS { return struct{ fs.FS }{n.Local.FS()} }

func mustLocal(t *testing.T, dir string) *agentworkspace.Local {
	t.Helper()
	l, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// The root's backends run the confinement tables: Local, a container's
// stand-in and an FS that cannot read links. remote runs them from its
// own tests, and every nested module's backend from its.
func TestConformance(t *testing.T) {
	workspacetest.Run(t,
		workspacetest.Backend{Name: "local", Links: true, Open: func(t *testing.T, dir string) agentworkspace.Workspace {
			return mustLocal(t, dir)
		}},
		workspacetest.Backend{Name: "container stand-in", Links: true, Open: func(t *testing.T, dir string) agentworkspace.Workspace {
			l := mustLocal(t, dir)
			return &mapped{backing: l, dir: l.Root()}
		}},
		workspacetest.Backend{Name: "no links", Open: func(t *testing.T, dir string) agentworkspace.Workspace {
			return noLinks{mustLocal(t, dir)}
		}},
	)
}

// A link swapped between a directory inside and one outside while it is
// read and written through never leads out: the root resolves each name
// as it opens it, not before.
func TestALinkSwappedMidOperationStaysIn(t *testing.T) {
	dir, outside := workspacetest.Confined(t)
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
		} else if err != nil && !errors.Is(err, agentworkspace.ErrOutside) {
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
