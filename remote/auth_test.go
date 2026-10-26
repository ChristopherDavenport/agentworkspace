package remote

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
)

// The tokens of the auth tests; the secret is one no error may hold.
const (
	tokRead  = "tok-read-0123456789"
	tokWrite = "tok-write-0123456789"
	tokExec  = "tok-exec-0123456789"
	tokRW    = "tok-rw-0123456789"
	tokAll   = "tok-all-0123456789"
	tokBob   = "tok-bob-0123456789"
	tokWE    = "tok-we-0123456789"
	tokRE    = "tok-re-0123456789"
)

func tokens() Authenticator {
	return BearerTokens(map[string]Principal{
		tokRead:  {Name: "reader", Scopes: []Scope{ScopeRead}},
		tokWrite: {Name: "writer", Scopes: []Scope{ScopeWrite}},
		tokExec:  {Name: "execer", Scopes: []Scope{ScopeExec}},
		tokRW:    {Name: "rw", Scopes: []Scope{ScopeRead, ScopeWrite}},
		tokAll:   {Name: "alice", Scopes: allScopes()},
		tokBob:   {Name: "bob", Scopes: allScopes()},
		tokWE:    {Name: "we", Scopes: []Scope{ScopeWrite, ScopeExec}},
		tokRE:    {Name: "re", Scopes: []Scope{ScopeRead, ScopeExec}},
	})
}

// routes is one request on every route, its ids made up: what is
// checked is the scope, before anything else.
var routes = []struct {
	method, path string
	scope        Scope
}{
	{"GET", "/v1/info", ScopeRead},
	{"GET", "/v1/fs/stat?path=a", ScopeRead},
	{"GET", "/v1/fs/lstat?path=a", ScopeRead},
	{"GET", "/v1/fs/readlink?path=a", ScopeRead},
	{"GET", "/v1/fs/readdir?path=.", ScopeRead},
	{"GET", "/v1/fs/file?path=a", ScopeRead},
	{"PUT", "/v1/fs/file?path=a&perm=420", ScopeWrite},
	{"DELETE", "/v1/fs/file?path=a", ScopeWrite},
	{"POST", "/v1/exec", ScopeExec},
	{"POST", "/v1/sessions", ScopeExec},
	{"POST", "/v1/sessions/S/renew", ScopeExec},
	{"DELETE", "/v1/sessions/S", ScopeExec},
	{"POST", "/v1/sessions/S/processes", ScopeExec},
	{"POST", "/v1/sessions/S/processes/P/stdin", ScopeExec},
	{"GET", "/v1/sessions/S/processes/P/stdout", ScopeExec},
	{"GET", "/v1/sessions/S/processes/P/stderr", ScopeExec},
	{"GET", "/v1/sessions/S/processes/P/wait", ScopeExec},
	{"POST", "/v1/sessions/S/processes/P/signal", ScopeExec},
	{"POST", "/v1/sessions/S/processes/P/cancel", ScopeExec},
	{"DELETE", "/v1/sessions/S/processes/P", ScopeExec},
}

// A handler with no authenticator refuses every request, on a route or
// not, and a client cannot dial it.
func TestNoAuthenticatorRefusesEverything(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l)
	for _, r := range append(routes, struct {
		method, path string
		scope        Scope
	}{"GET", "/no/such/route", ""}) {
		for _, tok := range []string{"", tokAll} {
			if status, code := raw(t, r.method, url+r.path, tok, nil); status != http.StatusUnauthorized || code != codeUnauthenticated {
				t.Errorf("%s %s: %d %q, want 401 unauthenticated", r.method, r.path, status, code)
			}
		}
	}
	if _, err := Dial(context.Background(), url, WithBearerToken(tokAll)); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("Dial: %v, want ErrUnauthenticated", err)
	}
}

// NewHandler refuses what it cannot serve honestly, and skips a nil
// option.
func TestNewHandlerRefuses(t *testing.T) {
	l, _ := newLocal(t, nil)
	for name, opts := range map[string][]Option{
		"auth and insecure": {WithAuthenticator(tokens()), WithInsecureNoAuth()},
		"no lease":          {WithInsecureNoAuth(), WithLease(0)},
		"no sessions":       {WithInsecureNoAuth(), WithMaxSessions(0)},
		"no processes":      {WithInsecureNoAuth(), WithMaxProcesses(0)},
		"no writes":         {WithInsecureNoAuth(), WithMaxWriteBytes(0)},
		"no listing":        {WithInsecureNoAuth(), WithMaxDirEntries(-1)},
	} {
		if _, err := NewHandler(l, opts...); err == nil {
			t.Errorf("%s: made a handler", name)
		}
	}
	if _, err := NewHandler(nil, WithInsecureNoAuth()); err == nil {
		t.Error("served a nil workspace")
	}
	if _, err := NewHandler(l, nil, WithInsecureNoAuth()); err != nil {
		t.Errorf("a nil option: %v", err)
	}
}

// No token, a token not given, and a token in another scheme are each
// unauthenticated.
func TestAnUnknownTokenIsRefused(t *testing.T) {
	l, _ := newLocal(t, nil)
	_, url := serve(t, l, WithAuthenticator(tokens()))
	for name, header := range map[string]string{
		"none":         "",
		"wrong":        "Bearer not-a-token",
		"empty":        "Bearer ",
		"basic":        "Basic " + tokAll,
		"a prefix":     "Bearer " + tokAll[:len(tokAll)-1],
		"a longer one": "Bearer " + tokAll + "x",
	} {
		req, _ := http.NewRequest("GET", url+"/v1/info", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: %s", name, resp.Status)
		}
	}
	if _, err := Dial(context.Background(), url, WithBearerToken("not-a-token")); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("Dial with a wrong token: %v", err)
	}
}

// Every route needs its scope: a principal holding every other scope
// is refused it, and one holding it is not.
func TestEachRouteNeedsItsScope(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithAuthenticator(tokens()))
	without := map[Scope]string{
		ScopeRead:  tokWE,
		ScopeWrite: tokRE,
		ScopeExec:  tokRW,
	}
	with := map[Scope]string{ScopeRead: tokRead, ScopeWrite: tokWrite, ScopeExec: tokExec}
	for _, r := range routes {
		body := func() *bytes.Reader { return bytes.NewReader([]byte("{}")) }
		if status, code := raw(t, r.method, url+r.path, without[r.scope], body()); status != http.StatusForbidden || code != codeForbidden {
			t.Errorf("%s %s without %s: %d %q, want 403 forbidden", r.method, r.path, r.scope, status, code)
		}
		if status, code := raw(t, r.method, url+r.path, with[r.scope], body()); status == http.StatusUnauthorized || code == codeForbidden {
			t.Errorf("%s %s with %s: %d %q", r.method, r.path, r.scope, status, code)
		}
	}
}

// What a client is follows its principal's scopes: read alone reads
// and is no Starter, with an empty Env; read and write writes too, and
// still runs nothing; exec is a Starter with the Env. exec is implied
// by neither read nor write.
func TestAClientIsWhatItsScopesAllow(t *testing.T) {
	l, _ := newLocal(t, append([]string{"A=1"}, pathEnv()...))
	l.WriteFile(context.Background(), "f.txt", []byte("hi"), 0o644)
	_, url := serve(t, l, WithAuthenticator(tokens()))
	ctx := context.Background()

	reader := dial(t, url, WithBearerToken(tokRead))
	if _, ok := reader.(agentworkspace.Starter); ok {
		t.Error("a reader is a Starter")
	}
	if env := reader.Env(); len(env) != 0 {
		t.Errorf("a reader was told the environment: %q", env)
	}
	if b, err := fs.ReadFile(reader.FS(), "f.txt"); err != nil || string(b) != "hi" {
		t.Errorf("a reader read %q, %v", b, err)
	}
	for what, err := range map[string]error{
		"write":  reader.WriteFile(ctx, "g.txt", []byte("x"), 0o644),
		"remove": reader.Remove(ctx, "f.txt"),
		"exec":   func() error { _, err := reader.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); return err }(),
		"start": func() error {
			_, err := agentworkspace.Start(ctx, reader, agentworkspace.Command{Args: []string{"true"}})
			return err
		}(),
	} {
		want := ErrForbidden
		if what == "start" {
			want = errors.ErrUnsupported
		}
		if !errors.Is(err, want) {
			t.Errorf("a reader's %s: %v, want %v", what, err, want)
		}
	}

	rw := dial(t, url, WithBearerToken(tokRW))
	if _, ok := rw.(agentworkspace.Starter); ok {
		t.Error("read and write is a Starter")
	}
	if env := rw.Env(); len(env) != 0 {
		t.Errorf("read and write was told the environment: %q", env)
	}
	if err := rw.WriteFile(ctx, "g.txt", []byte("x"), 0o644); err != nil {
		t.Errorf("read and write's write: %v", err)
	}
	if _, err := rw.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, ErrForbidden) {
		t.Errorf("read and write ran a command: %v", err)
	}

	if _, err := Dial(ctx, url, WithBearerToken(tokWrite)); !errors.Is(err, ErrForbidden) {
		t.Errorf("Dial without read: %v, want ErrForbidden", err)
	}

	all := dial(t, url, WithBearerToken(tokAll))
	if _, ok := all.(agentworkspace.Starter); !ok {
		t.Error("exec over a Starter is no Starter")
	}
	if env := all.Env(); !slices.Equal(env, l.Env()) {
		t.Errorf("Env with exec = %q, want %q", env, l.Env())
	}
	if out, err := all.Exec(ctx, agentworkspace.Command{Args: []string{"sh", "-c", "echo $A"}}); err != nil || string(out.Stdout) != "1\n" {
		t.Errorf("Exec with exec = %v, %v", out, err)
	}
}

// The token is in no error, whatever fails, nor in the Descriptor.
func TestTheTokenIsInNoError(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithAuthenticator(tokens()))
	ctx := context.Background()
	var errs []error
	_, err := Dial(ctx, url, WithBearerToken(tokAll+"-wrong"))
	errs = append(errs, err)
	_, err = Dial(ctx, strings.Replace(url, "://", "://user:"+tokAll+"@", 1))
	errs = append(errs, err)
	_, err = Dial(ctx, "http://user:"+tokAll+"@[::1:bad", WithBearerToken(tokAll))
	errs = append(errs, err)
	_, err = Dial(ctx, url, WithBearerToken(tokWrite))
	errs = append(errs, err)
	reader := dial(t, url, WithBearerToken(tokRead))
	errs = append(errs,
		reader.WriteFile(ctx, "x", nil, 0o644),
		reader.Remove(ctx, "x"),
		func() error { _, err := reader.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); return err }(),
		func() error { _, err := fs.Stat(reader.FS(), "missing"); return err }(),
		func() error { _, err := fs.ReadFile(reader.FS(), "../out"); return err }(),
	)
	all := dial(t, url, WithBearerToken(tokAll))
	errs = append(errs, func() error {
		_, err := all.Exec(ctx, agentworkspace.Command{Args: []string{"no-such-command-here"}})
		return err
	}())
	for i, err := range errs {
		if err == nil {
			t.Errorf("error %d is nil", i)
			continue
		}
		for _, tok := range []string{tokAll, tokRead, tokWrite} {
			if strings.Contains(err.Error(), tok) {
				t.Errorf("error %d holds a token: %v", i, err)
			}
		}
	}
	if d := all.Descriptor(); strings.Contains(d.Ref, "tok-") {
		t.Errorf("the Descriptor holds a token: %+v", d)
	}
}
