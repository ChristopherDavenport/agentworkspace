package remote

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

func newLocal(t *testing.T, env []string) (*agentworkspace.Local, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := agentworkspace.NewLocal(dir, env)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, dir
}

func pathEnv() []string { return []string{"PATH=" + os.Getenv("PATH")} }

// serve serves ws over httptest with opts and returns the handler and
// its base URL; both are closed when the test ends.
func serve(t *testing.T, ws agentworkspace.Workspace, opts ...Option) (*Handler, string) {
	t.Helper()
	h, err := NewHandler(ws, opts...)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { h.Close() })
	return h, srv.URL
}

// dial dials url, failing the test if it cannot, and closes the client
// when the test ends.
func dial(t *testing.T, url string, opts ...DialOption) Workspace {
	t.Helper()
	ws, err := Dial(context.Background(), url, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.Close() })
	return ws
}

func clientOf(ws Workspace) *Client {
	if s, ok := ws.(*StarterClient); ok {
		return s.Client
	}
	return ws.(*Client)
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

// groupGone waits, at most ten seconds, for process group pgid to have
// no process left.
func groupGone(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := syscall.Kill(-pgid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group %d is still running", pgid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// groupLeader starts a process whose first line of output is its
// process group's id, and reads it.
func groupLeader(t *testing.T, ws Workspace, ctx context.Context) (agentworkspace.Process, int) {
	t.Helper()
	p, err := agentworkspace.Start(ctx, ws, agentworkspace.Command{Args: []string{"sh", "-c", "echo $$; sleep 30 & wait"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	var line string
	within(t, 10*time.Second, "the first line", func() {
		line, err = bufio.NewReader(p.Stdout()).ReadString('\n')
	})
	pgid, perr := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || perr != nil {
		t.Fatalf("first line %q: %v %v", line, err, perr)
	}
	return p, pgid
}

// raw sends one request with token (none when empty) and returns its
// status and the code of the error it carries, if any.
func raw(t *testing.T, method, url, token string, body io.Reader) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		// Each route's own type: raw bytes for a file and a process's
		// input, JSON for the rest.
		req.Header.Set("Content-Type", jsonType)
		if method == "PUT" || strings.HasSuffix(req.URL.Path, "/stdin") {
			req.Header.Set("Content-Type", rawType)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	code := ""
	if resp.StatusCode/100 != 2 {
		var r errorReply
		if err := json.NewDecoder(resp.Body).Decode(&r); err == nil {
			code = r.Error.Code
		}
	}
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, code
}

// grant is an Authenticator over tokens, for the tests: a request whose
// Authorization header is "Bearer <token>" for a token in tokens is
// that token's principal. It stands in for the program's
// authentication, which the package does not ship.
func grant(tokens map[string]Principal) Authenticator {
	return func(r *http.Request) (Principal, error) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		p, found := tokens[tok]
		if !ok || !found {
			return Principal{}, errors.New("no such token")
		}
		return p, nil
	}
}

// withToken dials with an HTTP client that sends tok as a bearer token
// on every request, as a program's client carries its credentials.
func withToken(tok string) DialOption {
	return WithHTTPClient(&http.Client{
		Transport:     bearer{tok, http.DefaultTransport},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	})
}

// bearer sets the Authorization header of every request it sends.
type bearer struct {
	tok  string
	next http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.tok)
	return b.next.RoundTrip(r)
}

func decodeReply(resp *http.Response, v any) error { return json.NewDecoder(resp.Body).Decode(v) }
