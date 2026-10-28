package agentworkspace

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func local(t *testing.T, env []string) (*Local, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := NewLocal(dir, env)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, dir
}

// Env is a copy: a caller that changes it changes nothing a later
// process starts with.
func TestEnvIsACopy(t *testing.T) {
	l, _ := local(t, []string{"A=1"})
	e := l.Env()
	e[0] = "A=changed"
	_ = append(e, "B=2")
	if got := l.Env(); !slices.Equal(got, []string{"A=1"}) {
		t.Errorf("Env after a caller changed its copy = %q", got)
	}
	out, err := l.Exec(context.Background(), Command{Args: []string{"/bin/sh", "-c", "echo $A$B"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(out.Stdout)); got != "1" {
		t.Errorf("a process started with A$B = %q, want 1", got)
	}
}

// A process's environment is the workspace's with the command's over
// it, name by name: each name once, where it first appears, with the
// last value given it.
func TestProcessEnv(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ws, cmd []string
		want    []string
	}{
		{"neither", nil, nil, []string{}},
		{"the command's added", []string{"A=1"}, []string{"B=2"}, []string{"A=1", "B=2"}},
		{"the command's over the workspace's", []string{"A=1", "PATH=/bin", "C=3"}, []string{"PATH=/usr/bin"}, []string{"A=1", "PATH=/usr/bin", "C=3"}},
		{"the last of the command's", nil, []string{"A=1", "A=2"}, []string{"A=2"}},
		{"the workspace's own repeated", []string{"A=1", "A=2"}, nil, []string{"A=2"}},
		{"an empty value is a value", []string{"A=1"}, []string{"A="}, []string{"A="}},
		{"a value with an =", []string{"A=1"}, []string{"A=b=c"}, []string{"A=b=c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := processEnv(tc.ws, tc.cmd)
			if got == nil || !slices.Equal(got, tc.want) {
				t.Errorf("processEnv(%q, %q) = %#v, want %q", tc.ws, tc.cmd, got, tc.want)
			}
		})
	}
}

// A local Descriptor names the machine, as the system does or as
// WithHost says, and has no Ref.
func TestLocalDescriptorNamesTheHost(t *testing.T) {
	dir := t.TempDir()
	hostname, err := os.Hostname()
	if err != nil {
		t.Skip(err)
	}
	for _, tc := range []struct {
		name string
		opts []LocalOption
		want string
	}{
		{"the system's", nil, hostname},
		{"WithHost", []LocalOption{WithHost("build-7")}, "build-7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, err := NewLocal(dir, nil, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			if d := l.Descriptor(); d != (Descriptor{Kind: KindLocal, Host: tc.want, Root: l.Root()}) {
				t.Errorf("Descriptor = %+v, want Host %q", d, tc.want)
			}
		})
	}
}

// A nil environment is an empty one, never this process's, which holds
// the credentials Env is built without.
func TestANilEnvIsEmpty(t *testing.T) {
	t.Setenv("AGENTWORKSPACE_SECRET", "hunter2")
	l, _ := local(t, nil)
	out, err := l.Exec(context.Background(), Command{Args: []string{"/bin/sh", "-c", "echo [$AGENTWORKSPACE_SECRET]"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(out.Stdout)); got != "[]" {
		t.Errorf("a process saw %q of this process's environment", got)
	}
}

// The FS and the writes stay under the root, and nothing blocks on a
// FIFO.
func TestFilesStayInsideAndDoNotBlock(t *testing.T) {
	l, dir := local(t, nil)
	out := t.TempDir()
	os.WriteFile(filepath.Join(out, "secret"), []byte("x"), 0o644)
	os.Symlink(out, filepath.Join(dir, "link"))
	os.WriteFile(filepath.Join(dir, "in.txt"), []byte("ok"), 0o644)
	os.Symlink("in.txt", filepath.Join(dir, "alias"))
	ctx := context.Background()
	for name, err := range map[string]error{
		"read through a link out":  func() error { _, err := fs.ReadFile(l.FS(), "link/secret"); return err }(),
		"stat through a link out":  func() error { _, err := fs.Stat(l.FS(), "link/secret"); return err }(),
		"write through a link out": l.WriteFile(ctx, "link/new", []byte("x"), 0o644),
		"remove through a link":    l.Remove(ctx, "link/secret"),
	} {
		if !errors.Is(err, ErrOutside) {
			t.Errorf("%s: err = %v, want ErrOutside", name, err)
		}
	}
	if b, err := fs.ReadFile(l.FS(), "alias"); err != nil || string(b) != "ok" {
		t.Errorf("a link that stays in: %q %v", b, err)
	}
	if target, err := l.FS().(fs.ReadLinkFS).ReadLink("link"); err != nil || target != out {
		t.Errorf("ReadLink = %q %v", target, err)
	}
	if err := l.WriteFile(ctx, "a/b/c.txt", []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a/b/c.txt")); string(b) != "hi" {
		t.Errorf("WriteFile wrote %q", b)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "pipe"), 0o644); err != nil {
		t.Skipf("no FIFO here: %v", err)
	}
	done := make(chan [3]error, 1)
	go func() {
		_, rerr := fs.ReadDir(l.FS(), "pipe")
		f, oerr := l.FS().Open("pipe")
		if oerr == nil {
			f.Close()
		}
		done <- [3]error{l.WriteFile(ctx, "pipe", []byte("x"), 0o644), rerr, nil}
	}()
	select {
	case errs := <-done:
		if errs[0] == nil || errs[1] == nil {
			t.Errorf("a FIFO was written or listed: %v", errs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked on a FIFO")
	}
	if d := l.Descriptor(); d.Kind != KindLocal || d.Root != dir || d.Ref != "" {
		t.Errorf("Descriptor = %+v", d)
	}
}
