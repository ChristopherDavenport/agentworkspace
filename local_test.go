package agentworkspace

import (
	"bytes"
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
	out, err := l.Exec(context.Background(), Command{Args: []string{"sh", "-c", "echo $A$B"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(out.Stdout)); got != "1" {
		t.Errorf("a process started with A$B = %q, want 1", got)
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

func TestExec(t *testing.T) {
	l, dir := local(t, []string{"PATH=" + os.Getenv("PATH")})
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		cmd  Command
		want Output
	}{
		{"exit status and both streams", Command{Args: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}}, Output{Stdout: []byte("out\n"), Stderr: []byte("err\n"), ExitCode: 3}},
		{"Dir below the root", Command{Args: []string{"pwd"}, Dir: "sub"}, Output{Stdout: []byte(filepath.Join(dir, "sub") + "\n")}},
		{"Env added", Command{Args: []string{"sh", "-c", "echo $X"}, Env: []string{"X=y"}}, Output{Stdout: []byte("y\n")}},
		{"Stdin", Command{Args: []string{"cat"}, Stdin: []byte("in")}, Output{Stdout: []byte("in")}},
		{"Timeout kills what it started", Command{Args: []string{"sh", "-c", "sleep 30 & sleep 30"}, Timeout: 200 * time.Millisecond}, Output{ExitCode: -1, TimedOut: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			got, err := l.Exec(ctx, tc.cmd)
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
	out, err := l.Exec(ctx, Command{Args: []string{"sh", "-c", "echo a; echo b >&2"}, Stream: &stream})
	if err != nil || stream.String() != "a\nb\n" || len(out.Stdout)+len(out.Stderr) != 0 {
		t.Errorf("a stream got %q and the output kept %q %q (%v)", stream.String(), out.Stdout, out.Stderr, err)
	}
	if _, err := l.Exec(ctx, Command{Args: []string{"pwd"}, Dir: "../"}); !errors.Is(err, ErrOutside) {
		t.Errorf("a Dir out of the root: err = %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Exec(cctx, Command{Args: []string{"sleep", "5"}}); err == nil {
		t.Error("a cancelled context ran the command to the end")
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

// A name that is not fs.ValidPath is refused by every entry point: one
// that leaves is ErrOutside, any other fs.ErrInvalid, and neither
// reaches the file system.
func TestNamesThatAreNotValidPaths(t *testing.T) {
	l, dir := local(t, nil)
	os.WriteFile(filepath.Join(dir, "b"), []byte("x"), 0o644)
	ctx := context.Background()
	ops := map[string]func(name string) error{
		"open":     func(n string) error { _, err := l.FS().Open(n); return err },
		"stat":     func(n string) error { _, err := fs.Stat(l.FS(), n); return err },
		"lstat":    func(n string) error { _, err := l.FS().(fs.ReadLinkFS).Lstat(n); return err },
		"readlink": func(n string) error { _, err := l.FS().(fs.ReadLinkFS).ReadLink(n); return err },
		"readdir":  func(n string) error { _, err := fs.ReadDir(l.FS(), n); return err },
		"write":    func(n string) error { return l.WriteFile(ctx, n, []byte("y"), 0o644) },
		"remove":   func(n string) error { return l.Remove(ctx, n) },
	}
	for name, want := range map[string]error{
		"/etc/passwd": ErrOutside,
		"..":          ErrOutside,
		"../b":        ErrOutside,
		"a/../../b":   ErrOutside,
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
	if _, err := l.Exec(ctx, Command{Args: []string{"pwd"}, Dir: "b"}); err == nil {
		t.Error("Exec ran in a file")
	}
}
