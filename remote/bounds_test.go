package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// Every bound is kept, by the server whatever the client sends, and is
// ErrLimit on the client.
func TestBounds(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	for _, n := range []string{"a", "b", "c"} {
		os.WriteFile(filepath.Join(dir, n), nil, 0o644)
	}
	_, url := serve(t, l, WithInsecureNoAuth(), WithMaxWriteBytes(10), WithMaxProcesses(1), WithMaxSessions(1), WithMaxDirEntries(2))
	ctx := context.Background()
	ws := dial(t, url)

	big := bytes.Repeat([]byte("x"), 11)
	if err := ws.WriteFile(ctx, "big", big, 0o644); !errors.Is(err, ErrLimit) {
		t.Errorf("a write over the bound: %v", err)
	}
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"cat"}, Stdin: big}); !errors.Is(err, ErrLimit) {
		t.Errorf("stdin over the bound: %v", err)
	}
	if _, err := fs.ReadDir(ws.FS(), "."); !errors.Is(err, ErrLimit) {
		t.Errorf("a listing over the bound: %v", err)
	}
	p, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if q, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, ErrLimit) {
		if q != nil {
			q.Close()
		}
		t.Errorf("a process over the bound: %v", err)
	}
	// A dial takes no session; a second client's first Start would.
	second := dial(t, url)
	if q, err := agentworkspace.Start(ctx, second, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, ErrLimit) {
		if q != nil {
			q.Close()
		}
		t.Errorf("a session over the bound: %v", err)
	}

	// The server keeps them whatever the client sends.
	stdin, _ := json.Marshal(execRequest{Args: []string{"cat"}, Stdin: big})
	for name, r := range map[string]struct {
		method, route string
		body          []byte
	}{
		"a write":          {"PUT", "/v1/fs/file?path=big&perm=420", big},
		"stdin":            {"POST", "/v1/exec", stdin},
		"a JSON body":      {"POST", clientOf(ws).sessionRoute() + "/processes", bytes.Repeat([]byte(" "), MaxRequestBytes+1)},
		"an exec body":     {"POST", "/v1/exec", bytes.Repeat([]byte(" "), 2*MaxRequestBytes)},
		"a listing":        {"GET", "/v1/fs/readdir?path=.", nil},
		"a second session": {"POST", "/v1/sessions", []byte("{}")},
		"a second process": {"POST", clientOf(ws).sessionRoute() + "/processes", []byte(`{"args":["true"]}`)},
		"a JSON signal":    {"POST", p.(*process).route + "/signal", []byte(`{"signal":"` + strings.Repeat("x", MaxRequestBytes) + `"}`)},
	} {
		if _, code := raw(t, r.method, url+r.route, "", bytes.NewReader(r.body)); code != codeLimit {
			t.Errorf("%s over the bound: code %q, want limit", name, code)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "big")); err == nil {
		t.Error("a write over the bound was written")
	}
}

// Each mapped error a served workspace returns is the same sentinel on
// the client, and its message is the sentinel's words, not the served
// error's, which may name a path on the server's machine.
func TestErrorsRoundTrip(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	fw := &failingWorkspace{Local: l}
	_, url := serve(t, fw, WithInsecureNoAuth())
	ws := dial(t, url)
	for _, c := range []struct{ served, want error }{
		{agentworkspace.ErrOutside, agentworkspace.ErrOutside},
		{fs.ErrNotExist, fs.ErrNotExist},
		{fs.ErrExist, fs.ErrExist},
		{fs.ErrPermission, fs.ErrPermission},
		{fs.ErrInvalid, fs.ErrInvalid},
		{errors.ErrUnsupported, errors.ErrUnsupported},
		{os.ErrProcessDone, os.ErrProcessDone},
		{fs.ErrClosed, fs.ErrClosed},
		{&fs.PathError{Op: "unlinkat", Path: "/srv/host/x", Err: syscall.ENOTEMPTY}, fs.ErrExist},
		{&fs.PathError{Op: "openat", Path: "/srv/host/x", Err: syscall.EACCES}, fs.ErrPermission},
	} {
		fw.err = fmt.Errorf("remove /srv/host/secret: %w", c.served)
		err := ws.Remove(context.Background(), "x")
		if !errors.Is(err, c.want) {
			t.Errorf("Remove returning %v: client got %v, want %v", c.served, err, c.want)
		}
		if err != nil && strings.Contains(err.Error(), "/srv/host") {
			t.Errorf("the client was told a server path: %v", err)
		}
	}
	// An error of no kind the wire names keeps its words, bounded.
	fw.err = errors.New("the workspace's own words" + strings.Repeat(".", 2*MaxErrorBytes))
	err := ws.Remove(context.Background(), "x")
	if err == nil || !strings.Contains(err.Error(), "the workspace's own words") || len(err.Error()) > MaxErrorBytes+64 {
		t.Errorf("an unmapped error: %v", err)
	}
	for _, sentinel := range []error{agentworkspace.ErrOutside, fs.ErrNotExist, ErrLimit} {
		if errors.Is(err, sentinel) {
			t.Errorf("an unmapped error is %v", sentinel)
		}
	}
}

// failingWorkspace is Local whose Remove fails with err.
type failingWorkspace struct {
	*agentworkspace.Local
	err error
}

func (f *failingWorkspace) Remove(context.Context, string) error { return f.err }

// The errors of a real Local reach the client as they are there.
func TestLocalsErrorsReachTheClient(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "secret"), []byte("x"), 0o644)
	os.Symlink(filepath.Join(outside, "secret"), filepath.Join(dir, "link"))
	os.MkdirAll(filepath.Join(dir, "full/f"), 0o755)
	os.WriteFile(filepath.Join(dir, "locked"), []byte("x"), 0)
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	ctx := context.Background()
	cases := map[string]struct {
		err  error
		want error
	}{
		"outside":   {func() error { _, err := fs.Stat(ws.FS(), "link"); return err }(), agentworkspace.ErrOutside},
		"missing":   {func() error { _, err := fs.Stat(ws.FS(), "missing"); return err }(), fs.ErrNotExist},
		"not empty": {ws.Remove(ctx, "full"), fs.ErrExist},
		"invalid":   {func() error { _, err := fs.Stat(ws.FS(), "a/../b"); return err }(), fs.ErrInvalid},
		"a Dir out": {func() error {
			_, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"pwd"}, Dir: "link"})
			return err
		}(), agentworkspace.ErrOutside},
	}
	if os.Geteuid() != 0 {
		cases["permission"] = struct {
			err  error
			want error
		}{func() error { _, err := fs.ReadFile(ws.FS(), "locked"); return err }(), fs.ErrPermission}
	}
	for name, c := range cases {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: %v, want %v", name, c.err, c.want)
		}
	}
	// A signal to a process that has exited is os.ErrProcessDone, from
	// the server as from the client that knows.
	p, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.Wait()
	if status, code := raw(t, "POST", url+p.(*process).route+"/signal", "", strings.NewReader(`{"signal":"TERM"}`)); code != codeProcessDone {
		t.Errorf("the server's signal after the exit: %d %q", status, code)
	}
	if err := p.Signal(syscall.SIGTERM); !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("Signal after the exit: %v", err)
	}
	// A signal the wire does not name is refused, by both halves.
	q, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Signal(syscall.Signal(63)); err == nil {
		t.Error("an unknown signal was sent")
	}
	if _, code := raw(t, "POST", url+q.(*process).route+"/signal", "", strings.NewReader(`{"signal":"SEGV"}`)); code != codeInvalid {
		t.Errorf("the server took an unknown signal: %q", code)
	}
	if _, code := raw(t, "POST", url+q.(*process).route+"/signal", "", strings.NewReader(`{"signal":"15"}`)); code != codeInvalid {
		t.Errorf("the server took a signal's number: %q", code)
	}
}

// A workspace that is not a Starter is served as one that is not: the
// client is no Starter, and the session route says it cannot start.
func TestANonStarterIsServedAsOne(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, struct{ agentworkspace.Workspace }{l}, WithInsecureNoAuth())
	ws := dial(t, url)
	if _, ok := ws.(agentworkspace.Starter); ok {
		t.Error("a remote over a non-Starter is a Starter")
	}
	if _, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), agentworkspace.KindRemote) {
		t.Errorf("Start: %v", err)
	}
	if _, code := raw(t, "POST", url+"/v1/sessions", "", strings.NewReader("{}")); code != codeUnsupported {
		t.Errorf("a session over a non-Starter: %q", code)
	}
	if out, err := ws.Exec(context.Background(), agentworkspace.Command{Args: []string{"echo", "hi"}}); err != nil || string(out.Stdout) != "hi\n" {
		t.Errorf("Exec: %v, %v", out, err)
	}
}

// The Descriptor names the remote, its Ref the base URL with nothing
// but scheme, host and path; Served names the served workspace. A base
// URL under a path is served behind http.StripPrefix.
func TestDescriptor(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	h, err := NewHandler(l, WithInsecureNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	mux := http.NewServeMux()
	mux.Handle("/ws/", http.StripPrefix("/ws", h))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	ws := dial(t, srv.URL+"/ws/?token=x#frag")
	want := agentworkspace.Descriptor{Kind: agentworkspace.KindRemote, Ref: srv.URL + "/ws", Root: l.Root()}
	if d := ws.Descriptor(); d != want {
		t.Errorf("Descriptor = %+v, want %+v", d, want)
	}
	if d := ws.Served(); d != l.Descriptor() {
		t.Errorf("Served = %+v, want %+v", d, l.Descriptor())
	}
	if ws.Root() != l.Root() {
		t.Errorf("Root = %q, want %q", ws.Root(), l.Root())
	}
	for _, bad := range []string{"ftp://host/", "http:///nohost", "http://u:p@host/", "::"} {
		if c, err := Dial(context.Background(), bad); err == nil {
			c.Close()
			t.Errorf("Dial %q", bad)
		} else if strings.Contains(err.Error(), "u:p") {
			t.Errorf("Dial %q told the userinfo: %v", bad, err)
		}
	}
}

// A nil environment is served as an empty one, never the server's.
func TestANilEnvIsEmptyOverTheWire(t *testing.T) {
	t.Setenv("AGENTWORKSPACE_SECRET", "hunter2")
	l, _ := newLocal(t, nil)
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	if env := ws.Env(); len(env) != 0 {
		t.Errorf("Env = %q", env)
	}
	out, err := ws.Exec(context.Background(), agentworkspace.Command{Args: []string{"/bin/sh", "-c", "echo [$AGENTWORKSPACE_SECRET]"}})
	if err != nil || strings.TrimSpace(string(out.Stdout)) != "[]" {
		t.Errorf("a process saw %q of the server's environment (%v)", out.Stdout, err)
	}
}

// A name that is not UTF-8 survives a listing and a link's target.
func TestNamesThatAreNotUTF8(t *testing.T) {
	l, dir := newLocal(t, nil)
	odd := "caf\xe9"
	if err := os.WriteFile(filepath.Join(dir, odd), []byte("x"), 0o644); err != nil {
		t.Skipf("no such name here: %v", err)
	}
	os.Symlink(odd, filepath.Join(dir, "link"))
	_, url := serve(t, l, WithInsecureNoAuth())
	ws := dial(t, url)
	es, err := fs.ReadDir(ws.FS(), ".")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range es {
		names = append(names, e.Name())
	}
	if len(names) != 2 || names[0] != odd || names[1] != "link" {
		t.Errorf("names = %q", names)
	}
	if target, err := ws.FS().(fs.ReadLinkFS).ReadLink("link"); err != nil || target != odd {
		t.Errorf("ReadLink = %q, %v", target, err)
	}
	// As on Local, it is no name to open.
	if _, err := fs.ReadFile(ws.FS(), odd); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("ReadFile = %v, want fs.ErrInvalid", err)
	}
}

// The transfers served at once are bounded: a file whose content is
// streaming holds one, one more is ErrLimit, and its Close frees it.
// A session's routes are not counted.
func TestTransfersAreBounded(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	os.WriteFile(filepath.Join(dir, "f"), bytes.Repeat([]byte("x"), 1<<20), 0o644)
	h, url := serve(t, l, WithInsecureNoAuth(), WithMaxTransfers(1))
	if h.Lease() != DefaultLease {
		t.Errorf("Lease = %v, want the default", h.Lease())
	}
	ctx := context.Background()
	ws := dial(t, url)
	f, err := ws.FS().Open("f")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.ReadFile(ws.FS(), "f"); !errors.Is(err, ErrLimit) {
		t.Errorf("a read past the bound: %v", err)
	}
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, ErrLimit) {
		t.Errorf("an Exec past the bound: %v", err)
	}
	if err := ws.WriteFile(ctx, "g", []byte("x"), 0o644); !errors.Is(err, ErrLimit) {
		t.Errorf("a write past the bound: %v", err)
	}
	p, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"true"}})
	if err != nil {
		t.Fatalf("a Start while a transfer is served: %v", err)
	}
	within(t, 10*time.Second, "the process", func() { p.Wait() })
	p.Close()
	f.Close()
	// The server sees the stream end when its next write fails.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"true"}})
		if err == nil {
			break
		}
		if !errors.Is(err, ErrLimit) || time.Now().After(deadline) {
			t.Fatalf("an Exec once the transfer is closed: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := NewHandler(l, WithInsecureNoAuth(), WithMaxTransfers(0)); err == nil {
		t.Error("made a handler bounding no transfers")
	}
}
