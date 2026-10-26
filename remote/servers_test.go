package remote

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// offering is a Local offering servers, each started from its args.
func offering(t *testing.T, servers map[string][]string, opts ...agentworkspace.OfferOption) (*agentworkspace.Offering, string) {
	t.Helper()
	l, dir := newLocal(t, pathEnv())
	var offered []agentworkspace.OfferedServer
	for name, args := range servers {
		offered = append(offered, agentworkspace.OfferedServer{Server: agentworkspace.Server{Name: name}, Command: agentworkspace.Command{Args: args}})
	}
	o, err := agentworkspace.Offer(l, offered, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return o, dir
}

// providerOnly is a workspace that offers servers and starts nothing
// else: a ServerProvider that is no Starter.
type providerOnly struct {
	agentworkspace.Workspace
	agentworkspace.ServerProvider
}

// scoped is an Authenticator whose every request is from one principal
// named name, holding scopes.
func scoped(name string, scopes ...Scope) Authenticator {
	return func(*http.Request) (Principal, error) {
		return Principal{Name: name, Scopes: slices.Clone(scopes)}, nil
	}
}

// pgidLine reads a process's first line, its group's id.
func pgidLine(t *testing.T, p agentworkspace.Process) int {
	t.Helper()
	var line string
	var err error
	within(t, 10*time.Second, "the first line", func() {
		line, err = bufio.NewReader(p.Stdout()).ReadString('\n')
	})
	pgid, perr := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || perr != nil {
		t.Fatalf("first line %q: %v %v", line, err, perr)
	}
	return pgid
}

// What a client is follows both the served workspace and the scopes:
// a Starter exactly when the served workspace is one and the principal
// holds exec, a ServerProvider exactly when it offers servers and the
// principal holds servers, in every combination; neither scope implies
// the other.
func TestAClientIsAServerProviderWhenItsScopesAllow(t *testing.T) {
	o, _ := offering(t, map[string][]string{"echo": {"cat"}})
	l, _ := newLocal(t, pathEnv())
	for _, served := range []struct {
		name              string
		ws                agentworkspace.Workspace
		starter, provider bool
	}{
		{"local", l, true, false},
		{"offering", o, true, true},
		{"servers alone", providerOnly{o, o}, false, true},
	} {
		for _, scopes := range [][]Scope{
			{ScopeRead},
			{ScopeRead, ScopeExec},
			{ScopeRead, ScopeServers},
			{ScopeRead, ScopeExec, ScopeServers},
		} {
			_, url := serve(t, served.ws, WithAuthenticator(scoped("p", scopes...)))
			ws := dial(t, url)
			_, starter := ws.(agentworkspace.Starter)
			_, provider := ws.(agentworkspace.ServerProvider)
			wantStarter := served.starter && slices.Contains(scopes, ScopeExec)
			wantProvider := served.provider && slices.Contains(scopes, ScopeServers)
			if starter != wantStarter || provider != wantProvider {
				t.Errorf("%s with %v: %T, a Starter %v and a ServerProvider %v; want %v and %v", served.name, scopes, ws, starter, provider, wantStarter, wantProvider)
			}
			var want Workspace
			switch {
			case wantStarter && wantProvider:
				want = &StarterServerClient{}
			case wantStarter:
				want = &StarterClient{}
			case wantProvider:
				want = &ServerClient{}
			default:
				want = &Client{}
			}
			if fmt.Sprintf("%T", ws) != fmt.Sprintf("%T", want) {
				t.Errorf("%s with %v: %T, want %T", served.name, scopes, ws, want)
			}
		}
	}
}

// A principal holding servers and not exec lists the servers, starts
// one and talks to it, and runs no command: no Exec, no Start, no
// process's route, and no environment.
func TestAServersPrincipalRunsNoCommand(t *testing.T) {
	o, _ := offering(t, map[string][]string{"echo": {"cat"}})
	_, url := serve(t, o, WithAuthenticator(grant(map[string]Principal{
		tokRead: {Name: "p", Scopes: []Scope{ScopeRead, ScopeServers}},
	})))
	ctx := context.Background()
	ws := dial(t, url, withToken(tokRead))
	sp, ok := ws.(agentworkspace.ServerProvider)
	if !ok {
		t.Fatalf("%T is no ServerProvider", ws)
	}
	if list, err := sp.Servers(ctx); err != nil || len(list) != 1 || list[0].Name != "echo" {
		t.Errorf("Servers = %+v, %v", list, err)
	}
	p, err := sp.StartServer(ctx, "echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	within(t, 10*time.Second, "the conversation", func() {
		io.WriteString(p.Stdin(), "ping\n")
		if got, err := bufio.NewReader(p.Stdout()).ReadString('\n'); got != "ping\n" || err != nil {
			t.Errorf("answer = %q, %v", got, err)
		}
	})
	if env := ws.Env(); len(env) != 0 {
		t.Errorf("a servers principal was told the environment: %q", env)
	}
	if _, err := ws.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, ErrForbidden) {
		t.Errorf("Exec = %v, want ErrForbidden", err)
	}
	if _, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("Start = %v, want ErrUnsupported", err)
	}
	route := clientOf(ws).sessionRoute()
	if status, code := raw(t, "POST", url+route+"/processes", tokRead, strings.NewReader(`{"args":["true"]}`)); status != http.StatusForbidden || code != codeForbidden {
		t.Errorf("a command's start in its session: %d %q, want forbidden", status, code)
	}
}

// A principal holding exec and not servers starts no server, and is
// not told the workspace offers any.
func TestAnExecPrincipalStartsNoServer(t *testing.T) {
	o, _ := offering(t, map[string][]string{"echo": {"cat"}})
	_, url := serve(t, o, WithAuthenticator(grant(map[string]Principal{
		tokRE: {Name: "p", Scopes: []Scope{ScopeRead, ScopeExec}},
	})))
	ws := dial(t, url, withToken(tokRE))
	if _, ok := ws.(agentworkspace.ServerProvider); ok {
		t.Fatalf("%T is a ServerProvider", ws)
	}
	if clientOf(ws).info.Servers {
		t.Error("the info route told an exec principal of the servers")
	}
	p, err := agentworkspace.Start(context.Background(), ws, agentworkspace.Command{Args: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	route := clientOf(ws).sessionRoute()
	for _, r := range []struct{ method, path string }{
		{"GET", routeServers},
		{"POST", route + "/servers/echo/start"},
	} {
		if status, code := raw(t, r.method, url+r.path, tokRE, strings.NewReader("{}")); status != http.StatusForbidden || code != codeForbidden {
			t.Errorf("%s %s: %d %q, want forbidden", r.method, r.path, status, code)
		}
	}
}

// post sends one JSON request with tok and decodes a success's reply
// into out, failing the test on anything else.
func post(t *testing.T, url, tok, body string, out any) {
	t.Helper()
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", jsonType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s: %s %s", url, resp.Status, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

// A process keeps the scope it was started under: a server started
// with servers is not exec's to use, and a command started with exec
// is not servers', though both are in one session of one principal. A
// server is reached only through its session, by the principal that
// opened that, as any process is.
func TestAProcessKeepsTheScopeItWasStartedUnder(t *testing.T) {
	o, _ := offering(t, map[string][]string{"echo": {"cat"}})
	const (
		tokPS = "tok-p-servers-0123456789"
		tokPE = "tok-p-exec-0123456789"
		tokQ  = "tok-q-servers-0123456789"
	)
	h, url := serve(t, o, WithAuthenticator(grant(map[string]Principal{
		tokPS: {Name: "p", Scopes: []Scope{ScopeServers}},
		tokPE: {Name: "p", Scopes: []Scope{ScopeExec}},
		tokQ:  {Name: "q", Scopes: []Scope{ScopeServers}},
	})))
	var s, other sessionReply
	post(t, url+routeSessions, tokPS, "{}", &s)
	post(t, url+routeSessions, tokPS, "{}", &other)
	session := url + routeSessions + "/" + s.ID
	server, command := strings.Repeat("S", 26), strings.Repeat("C", 26)
	var st startedReply
	post(t, session+"/servers/echo/start", tokPS, `{"key":"`+server+`"}`, &st)
	post(t, session+"/processes", tokPE, `{"args":["sleep","30"],"key":"`+command+`"}`, &st)
	if status, code := raw(t, "POST", session+"/servers/echo/start", tokPS, strings.NewReader(`{"key":"`+command+`"}`)); status != http.StatusBadRequest || code != codeInvalid {
		t.Errorf("a server's start by a command's key: %d %q, want invalid", status, code)
	}
	for _, r := range []struct{ method, suffix string }{
		{"GET", "/wait"},
		{"GET", "/stdout"},
		{"GET", "/stderr"},
		{"POST", "/stdin"},
		{"POST", "/signal"},
		{"POST", "/cancel"},
		{"DELETE", ""},
	} {
		body := func() *bytes.Reader { return bytes.NewReader([]byte(`{"signal":"KILL"}`)) }
		for _, c := range []struct {
			what, url, tok string
			status         int
			code           string
		}{
			{"exec on a server", session + "/processes/" + server, tokPE, http.StatusForbidden, codeForbidden},
			{"servers on a command", session + "/processes/" + command, tokPS, http.StatusForbidden, codeForbidden},
			{"another principal", session + "/processes/" + server, tokQ, http.StatusNotFound, codeNoSession},
			{"another session", url + routeSessions + "/" + other.ID + "/processes/" + server, tokPS, http.StatusNotFound, codeNoProcess},
		} {
			if status, code := raw(t, r.method, c.url+r.suffix, c.tok, body()); status != c.status || code != c.code {
				t.Errorf("%s: %s %s: %d %q, want %d %q", c.what, r.method, r.suffix, status, code, c.status, c.code)
			}
		}
	}
	if n := processes(h); n != 2 {
		t.Errorf("%d processes open, want both still running", n)
	}
}

// A server's start names its process by the client's key, as a
// command's does: the same start again is the same process, and once
// closed, a start by its key starts nothing.
func TestAServerStartsKeyNamesItsProcess(t *testing.T) {
	o, _ := offering(t, map[string][]string{"echo": {"cat"}})
	h, url := serve(t, o, WithInsecureNoAuth())
	var s sessionReply
	post(t, url+routeSessions, "", "{}", &s)
	route := url + routeSessions + "/" + s.ID
	key := strings.Repeat("K", 26)
	body := `{"key":"` + key + `"}`
	for range 2 {
		var st startedReply
		post(t, route+"/servers/echo/start", "", body, &st)
		if st.ID != key {
			t.Errorf("a keyed start named its process %q", st.ID)
		}
	}
	if n := processes(h); n != 1 {
		t.Errorf("%d processes after a start sent twice", n)
	}
	if status, code := raw(t, "DELETE", route+"/processes/"+key, "", nil); status != http.StatusOK {
		t.Errorf("closing by the key: %d %q", status, code)
	}
	if _, code := raw(t, "POST", route+"/servers/echo/start", "", strings.NewReader(body)); code != codeNoProcess {
		t.Errorf("a start by a closed key: %q", code)
	}
	if _, code := raw(t, "POST", route+"/servers/echo/start", "", strings.NewReader(`{"key":"short"}`)); code != codeInvalid {
		t.Errorf("a start keyed short: %q", code)
	}
	if n := processes(h); n != 0 {
		t.Errorf("%d processes after the key was closed", n)
	}
}

// A server's start whose reply is lost leaves nothing running: the
// client closes it by its key.
func TestALostServerStartReplyLeavesNothingRunning(t *testing.T) {
	o, dir := offering(t, map[string][]string{"s": {"sh", "-c", "echo $$ > pgid; exec sleep 30"}})
	h, err := NewHandler(o, WithInsecureNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/start") {
			h.ServeHTTP(httptest.NewRecorder(), r) // started, and its reply lost
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()
	ws := dial(t, srv.URL).(agentworkspace.ServerProvider)
	if p, err := ws.StartServer(context.Background(), "s", nil); err == nil {
		p.Close()
		t.Fatal("a server's start whose reply was lost succeeded")
	}
	if n := processes(h); n != 0 {
		t.Errorf("%d processes run unnamed", n)
	}
	groupGone(t, pgidFile(t, filepath.Join(dir, "pgid")))
}

// A server lives no longer than its session: one whose client is gone
// lapses on the server, which ends the server and everything it
// started, and the client, a lease unrenewed, takes it as lost (Wait is
// ErrNoSession). The handler's Close ends a server too.
func TestAServerEndsWithItsSession(t *testing.T) {
	servers := map[string][]string{"s": {"sh", "-c", "echo $$; sleep 30 & wait"}}
	t.Run("lapse", func(t *testing.T) {
		o, _ := offering(t, servers)
		h, err := NewHandler(o, WithAuthenticator(scoped("p", ScopeRead, ScopeServers)), WithLease(300*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		t.Cleanup(func() { h.Close() })
		ws := dial(t, srv.URL)
		p, err := ws.(agentworkspace.ServerProvider).StartServer(context.Background(), "s", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		pgid := pgidLine(t, p)
		srv.Listener.Close()
		srv.CloseClientConnections()
		within(t, 5*time.Second, "Wait on a lost session", func() {
			if status, err := p.Wait(); status != -1 || !errors.Is(err, ErrNoSession) {
				t.Errorf("Wait = %d, %v; want -1, ErrNoSession", status, err)
			}
		})
		groupGone(t, pgid)
	})
	t.Run("handler", func(t *testing.T) {
		o, _ := offering(t, servers)
		h, url := serve(t, o, WithAuthenticator(scoped("p", ScopeRead, ScopeServers)))
		ws := dial(t, url)
		p, err := ws.(agentworkspace.ServerProvider).StartServer(context.Background(), "s", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		pgid := pgidLine(t, p)
		within(t, 15*time.Second, "the handler's close", func() { h.Close() })
		groupGone(t, pgid)
	})
}

// A name not offered is ErrNoServer on the client, as on the served
// workspace: the code round-trips, a name ValidServerName refuses is
// refused before it is sent, and a workspace that offers none says so.
func TestErrNoServerRoundTrips(t *testing.T) {
	o, _ := offering(t, map[string][]string{"echo": {"cat"}})
	_, url := serve(t, o, WithInsecureNoAuth())
	ws := dial(t, url).(agentworkspace.ServerProvider)
	ctx := context.Background()
	for _, name := range []string{"nope", "Bad_Name", "../processes", ""} {
		if p, err := ws.StartServer(ctx, name, nil); !errors.Is(err, agentworkspace.ErrNoServer) {
			if p != nil {
				p.Close()
			}
			t.Errorf("StartServer(%q) = %v, want ErrNoServer", name, err)
		}
	}
	var s sessionReply
	post(t, url+routeSessions, "", "{}", &s)
	for _, name := range []string{"nope", "Bad_Name"} {
		if status, code := raw(t, "POST", url+routeSessions+"/"+s.ID+"/servers/"+name+"/start", "", strings.NewReader("{}")); status != http.StatusNotFound || code != codeNoServer {
			t.Errorf("the start of %q: %d %q, want 404 no_server", name, status, code)
		}
	}
	if err := (wireError{Code: codeNoServer, Message: "x"}).err(); !errors.Is(err, agentworkspace.ErrNoServer) {
		t.Errorf("the code maps to %v", err)
	}
	if e := errorOf("", fmt.Errorf("server %q: %w", "x", agentworkspace.ErrNoServer)); e.Code != codeNoServer {
		t.Errorf("ErrNoServer is sent as %q", e.Code)
	}

	// A workspace offering none: its routes are unsupported.
	l, _ := newLocal(t, pathEnv())
	_, url = serve(t, l, WithInsecureNoAuth())
	post(t, url+routeSessions, "", "{}", &s)
	for _, r := range []struct{ method, path string }{
		{"GET", routeServers},
		{"POST", routeSessions + "/" + s.ID + "/servers/echo/start"},
	} {
		if status, code := raw(t, r.method, url+r.path, "", strings.NewReader("{}")); status != http.StatusNotImplemented || code != codeUnsupported {
			t.Errorf("%s %s of a Local: %d %q, want unsupported", r.method, r.path, status, code)
		}
	}
}

// listed is a ServerProvider listing a fixed list, in its own order,
// and starting nothing.
type listed struct {
	agentworkspace.Workspace
	list []agentworkspace.Server
}

func (l listed) Servers(context.Context) ([]agentworkspace.Server, error) { return l.list, nil }

func (l listed) StartServer(_ context.Context, name string, _ io.Writer) (agentworkspace.Process, error) {
	return nil, fmt.Errorf("server %q: %w", name, agentworkspace.ErrNoServer)
}

// The client lists the servers in the served workspace's order, and
// refuses a list naming a server ValidServerName refuses.
func TestServersKeepTheServedOrder(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	want := []agentworkspace.Server{{Name: "zed", Description: "last by name"}, {Name: "alpha"}, {Name: "mid-1", Description: "between"}}
	_, url := serve(t, listed{l, want}, WithInsecureNoAuth())
	ws := dial(t, url).(agentworkspace.ServerProvider)
	if got, err := ws.Servers(context.Background()); err != nil || !slices.Equal(got, want) {
		t.Errorf("Servers = %+v, %v; want %+v", got, err, want)
	}
	_, url = serve(t, listed{l, []agentworkspace.Server{{Name: "Not_Valid"}}}, WithInsecureNoAuth())
	if got, err := dial(t, url).(agentworkspace.ServerProvider).Servers(context.Background()); err == nil {
		t.Errorf("Servers of a bad name = %+v", got)
	}
}

// A started server is a process of its session: it counts against
// WithMaxProcesses with the commands, and one closed makes room.
func TestServersCountAgainstMaxProcesses(t *testing.T) {
	o, _ := offering(t, map[string][]string{"echo": {"cat"}})
	_, url := serve(t, o, WithInsecureNoAuth(), WithMaxProcesses(1))
	ws := dial(t, url)
	sp := ws.(agentworkspace.ServerProvider)
	ctx := context.Background()
	p, err := sp.StartServer(ctx, "echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if q, err := sp.StartServer(ctx, "echo", nil); !errors.Is(err, ErrLimit) {
		if q != nil {
			q.Close()
		}
		t.Errorf("a second server = %v, want ErrLimit", err)
	}
	if q, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"true"}}); !errors.Is(err, ErrLimit) {
		if q != nil {
			q.Close()
		}
		t.Errorf("a command beside a server = %v, want ErrLimit", err)
	}
	p.Close()
	q, err := sp.StartServer(ctx, "echo", nil)
	if err != nil {
		t.Fatalf("a server after the first closed: %v", err)
	}
	q.Close()
}

// tee is a RoundTripper that keeps every byte of every reply.
type tee struct {
	mu   sync.Mutex
	seen bytes.Buffer
	next http.RoundTripper
}

func (t *tee) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(r)
	if err == nil {
		resp.Body = struct {
			io.Reader
			io.Closer
		}{io.TeeReader(resp.Body, t), resp.Body}
	}
	return resp, err
}

func (t *tee) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen.Write(p)
}

func (t *tee) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen.String()
}

// redact is an OutputFilter for the tests: a line at a time, each
// secret's value written as its name in brackets.
func redact(_ string, secrets map[string]string, r io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		br := bufio.NewReader(r)
		for {
			line, err := br.ReadString('\n')
			for name, v := range secrets {
				line = strings.ReplaceAll(line, v, "["+name+"]")
			}
			if _, werr := io.WriteString(pw, line); werr != nil {
				return
			}
			if err != nil {
				pw.Close()
				return
			}
		}
	}()
	return pr
}

// A server's secret never crosses the wire: the served Offering's
// filter reads its output on the serving side, so a server that prints
// its whole environment, on standard output and standard error, sends
// the filter's mark and no byte of the value, in any reply.
func TestASecretNeverCrossesTheWire(t *testing.T) {
	const secret = "tok-remote-secret-0123456789"
	l, _ := newLocal(t, pathEnv())
	o, err := agentworkspace.Offer(l, []agentworkspace.OfferedServer{{
		Server:  agentworkspace.Server{Name: "holder", Description: "holds a token"},
		Command: agentworkspace.Command{Args: []string{"sh", "-c", "env; env >&2; echo ready; exec cat"}, Env: []string{"TOKEN=" + secret}},
		Secrets: []string{"TOKEN"},
	}}, agentworkspace.WithOutputFilter(redact))
	if err != nil {
		t.Fatal(err)
	}
	_, url := serve(t, o, WithInsecureNoAuth())
	wire := &tee{next: http.DefaultTransport}
	ws := dial(t, url, WithHTTPClient(&http.Client{Transport: wire}))
	var stderr syncBuffer
	p, err := ws.(agentworkspace.ServerProvider).StartServer(context.Background(), "holder", &stderr)
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	within(t, 10*time.Second, "the server's output", func() {
		br := bufio.NewReader(p.Stdout())
		for {
			line, err := br.ReadString('\n')
			out = append(out, line...)
			if line == "ready\n" || err != nil {
				break
			}
		}
		p.Stdin().Close()
		p.Wait()
	})
	if !strings.Contains(string(out), "TOKEN=[TOKEN]") || !strings.Contains(stderr.String(), "TOKEN=[TOKEN]") {
		t.Errorf("the environment was not printed through the filter: stdout %q, stderr %q", out, stderr.String())
	}
	for what, got := range map[string]string{"the wire": wire.String(), "stdout": string(out), "stderr": stderr.String(), "Env": strings.Join(ws.Env(), " ")} {
		if strings.Contains(got, secret) {
			t.Errorf("%s holds the secret", what)
		}
	}
}

// syncBuffer is a bytes.Buffer safe for a stream's writer and the
// test's reader at once.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
