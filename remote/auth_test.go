package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
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

// tokens is the tests' authentication, which the package does not
// ship: a request's bearer token names its principal.
func tokens() Authenticator {
	return grant(map[string]Principal{
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

// route is one request on a route, its ids made up, and the scopes
// any one of which it takes: what is checked is the scope, before
// anything else.
type route struct {
	method, path string
	scopes       []Scope
}

// either is a session's routes' scopes, and a process's: exec or
// servers, the process's own then checked.
var either = []Scope{ScopeExec, ScopeServers}

// routes is one request on every route.
var routes = []route{
	{"GET", "/v1/info", []Scope{ScopeRead, ScopeServers}},
	{"GET", "/v1/fs/stat?path=a", []Scope{ScopeRead}},
	{"GET", "/v1/fs/lstat?path=a", []Scope{ScopeRead}},
	{"GET", "/v1/fs/readlink?path=a", []Scope{ScopeRead}},
	{"GET", "/v1/fs/readdir?path=.", []Scope{ScopeRead}},
	{"GET", "/v1/fs/file?path=a", []Scope{ScopeRead}},
	{"PUT", "/v1/fs/file?path=a&perm=420", []Scope{ScopeWrite}},
	{"DELETE", "/v1/fs/file?path=a", []Scope{ScopeWrite}},
	{"POST", "/v1/exec", []Scope{ScopeExec}},
	{"GET", "/v1/servers", []Scope{ScopeServers}},
	{"POST", "/v1/sessions", either},
	{"POST", "/v1/sessions/S/renew", either},
	{"DELETE", "/v1/sessions/S", either},
	{"POST", "/v1/sessions/S/processes", []Scope{ScopeExec}},
	{"POST", "/v1/sessions/S/servers/echo/start", []Scope{ScopeServers}},
	{"POST", "/v1/sessions/S/processes/P/stdin", either},
	{"GET", "/v1/sessions/S/processes/P/stdout", either},
	{"GET", "/v1/sessions/S/processes/P/stderr", either},
	{"GET", "/v1/sessions/S/processes/P/wait", either},
	{"POST", "/v1/sessions/S/processes/P/signal", either},
	{"POST", "/v1/sessions/S/processes/P/cancel", either},
	{"DELETE", "/v1/sessions/S/processes/P", either},
}

// A handler with no authenticator refuses every request, on a route or
// not, and a client cannot dial it.
func TestNoAuthenticatorRefusesEverything(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l)
	for _, r := range append(routes, route{"GET", "/no/such/route", nil}) {
		for _, tok := range []string{"", tokAll} {
			if status, code := raw(t, r.method, url+r.path, tok, nil); status != http.StatusUnauthorized || code != codeUnauthenticated {
				t.Errorf("%s %s: %d %q, want 401 unauthenticated", r.method, r.path, status, code)
			}
		}
	}
	if _, err := Dial(context.Background(), url, withToken(tokAll)); !errors.Is(err, ErrUnauthenticated) {
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

// An authenticator's refusal, and a principal with no name, are each
// unauthenticated, and the refusal's text is not sent.
func TestAnAuthenticatorsRefusalIsUnauthenticated(t *testing.T) {
	l, _ := newLocal(t, nil)
	for name, auth := range map[string]Authenticator{
		"refused": func(*http.Request) (Principal, error) {
			return Principal{}, errors.New("the reason hunter2")
		},
		"no name": func(*http.Request) (Principal, error) {
			return Principal{Scopes: allScopes()}, nil
		},
	} {
		_, url := serve(t, l, WithAuthenticator(auth))
		resp, err := http.Get(url + "/v1/info")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized || strings.Contains(string(body), "hunter2") {
			t.Errorf("%s: %s %q", name, resp.Status, body)
		}
		if _, err := Dial(context.Background(), url); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("%s: Dial: %v, want ErrUnauthenticated", name, err)
		}
	}
}

// Every route needs its scope: a principal holding every other scope
// is refused it, and one holding it alone is not. A session's routes,
// and a process's, take exec or servers, either alone.
func TestEachRouteNeedsItsScope(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	o, err := agentworkspace.Offer(l, []agentworkspace.OfferedServer{{Server: agentworkspace.Server{Name: "echo"}, Command: agentworkspace.Command{Args: []string{"cat"}}}})
	if err != nil {
		t.Fatal(err)
	}
	// A request's token is its scopes, by name, comma-separated.
	_, url := serve(t, o, WithAuthenticator(func(r *http.Request) (Principal, error) {
		tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer scopes:")
		var scopes []Scope
		for _, s := range strings.Split(tok, ",") {
			if s != "" {
				scopes = append(scopes, Scope(s))
			}
		}
		return Principal{Name: "p", Scopes: scopes}, nil
	}))
	token := func(scopes []Scope) string {
		names := make([]string, len(scopes))
		for i, s := range scopes {
			names[i] = string(s)
		}
		return "scopes:" + strings.Join(names, ",")
	}
	for _, r := range routes {
		body := func() *bytes.Reader { return bytes.NewReader([]byte("{}")) }
		others := slices.DeleteFunc(allScopes(), func(s Scope) bool { return slices.Contains(r.scopes, s) })
		if status, code := raw(t, r.method, url+r.path, token(others), body()); status != http.StatusForbidden || code != codeForbidden {
			t.Errorf("%s %s with %v: %d %q, want 403 forbidden", r.method, r.path, others, status, code)
		}
		for _, s := range r.scopes {
			if status, code := raw(t, r.method, url+r.path, token([]Scope{s}), body()); status == http.StatusUnauthorized || code == codeForbidden {
				t.Errorf("%s %s with %s: %d %q", r.method, r.path, s, status, code)
			}
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

	reader := dial(t, url, withToken(tokRead))
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

	rw := dial(t, url, withToken(tokRW))
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

	if _, err := Dial(ctx, url, withToken(tokWrite)); !errors.Is(err, ErrForbidden) {
		t.Errorf("Dial without read: %v, want ErrForbidden", err)
	}

	all := dial(t, url, withToken(tokAll))
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

// A client's Scopes are the ones the server granted its principal at
// Dial, in the server's order, and a copy: changing it changes no later
// answer. A principal holding servers, without read or with it, is told
// its scopes as any other is, its client a ServerProvider.
func TestAClientSaysItsScopes(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	o, _ := offering(t, map[string][]string{"echo": {"cat"}})
	for _, tc := range []struct {
		name string
		ws   agentworkspace.Workspace
		want []Scope
	}{
		{"read", l, []Scope{ScopeRead}},
		{"read and write", l, []Scope{ScopeRead, ScopeWrite}},
		{"read and exec", l, []Scope{ScopeRead, ScopeExec}},
		{"all", o, allScopes()},
		{"servers alone", providerOnly{o, o}, []Scope{ScopeServers}},
		{"servers and read", providerOnly{o, o}, []Scope{ScopeServers, ScopeRead}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, url := serve(t, tc.ws, WithAuthenticator(scoped("p", tc.want...)))
			ws := dial(t, url)
			_, provider := ws.(agentworkspace.ServerProvider)
			if wantProvider := slices.Contains(tc.want, ScopeServers); provider != wantProvider {
				t.Errorf("%T: a ServerProvider %v, want %v", ws, provider, wantProvider)
			}
			got := ws.Scopes()
			if !slices.Equal(got, tc.want) {
				t.Fatalf("Scopes = %q, want %q", got, tc.want)
			}
			got[0] = "changed"
			if again := ws.Scopes(); !slices.Equal(again, tc.want) {
				t.Errorf("Scopes after changing a copy = %q, want %q", again, tc.want)
			}
		})
	}
}

// Dial refuses a description that names no scopes: what the client
// says it may do would be nothing the server said.
func TestDialRefusesADescriptionWithNoScopes(t *testing.T) {
	l, _ := newLocal(t, nil)
	h, err := NewHandler(l, WithInsecureNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		var info map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
			t.Error(err)
		}
		delete(info, "scopes")
		writeJSON(w, info)
	}))
	defer srv.Close()
	if _, err := Dial(context.Background(), srv.URL); err == nil || !strings.Contains(err.Error(), "no scopes") {
		t.Errorf("Dial: %v, want a refusal of a description with no scopes", err)
	}
}

// The token the program gives its HTTP client is in no error, whatever
// fails, nor in the Descriptor.
func TestTheTokenIsInNoError(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, url := serve(t, l, WithAuthenticator(tokens()))
	ctx := context.Background()
	var errs []error
	_, err := Dial(ctx, url, withToken(tokAll+"-wrong"))
	errs = append(errs, err)
	_, err = Dial(ctx, strings.Replace(url, "://", "://user:"+tokAll+"@", 1))
	errs = append(errs, err)
	_, err = Dial(ctx, "http://user:"+tokAll+"@[::1:bad", withToken(tokAll))
	errs = append(errs, err)
	_, err = Dial(ctx, url, withToken(tokWrite))
	errs = append(errs, err)
	reader := dial(t, url, withToken(tokRead))
	errs = append(errs,
		reader.WriteFile(ctx, "x", nil, 0o644),
		reader.Remove(ctx, "x"),
		func() error { _, err := reader.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); return err }(),
		func() error { _, err := fs.Stat(reader.FS(), "missing"); return err }(),
		func() error { _, err := fs.ReadFile(reader.FS(), "../out"); return err }(),
	)
	all := dial(t, url, withToken(tokAll))
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
