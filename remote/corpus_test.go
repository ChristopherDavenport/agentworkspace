package remote

// The wire corpus, testdata/wire/remote.json: request and reply
// exchanges, in order, that the reference server must answer as they
// say, and whose refusals the reference client must map to their kind.
// It is the language-neutral half of conformance (RFC 0001 §16): a
// server in another language replays the requests and compares its
// replies; a client, the replies. Run with -update to record the
// replies of a changed server, then read the diff.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/workspacetest"
)

var update = flag.Bool("update", false, "record the corpus's replies from the reference server")

const corpusFile = "../testdata/wire/remote.json"

// corpus is the file: the layout the exchanges run in, and the
// exchanges.
type corpus struct {
	Doc       []string   `json:"doc"`
	Exchanges []exchange `json:"exchanges"`
}

// exchange is one request and the reply it gets. Path and a body may
// name ${session}, the id the last session's opening gave, and a body
// "${port}", a port on the loopback that answers what it is sent,
// upper-cased, once it is all sent. A background exchange's reply is
// read while the exchanges after it run: the server answers its
// header first, and the rest comes when it ends. A reply is
// JSON as it is, NDJSON as the array of its lines, raw bytes as a
// string, and nothing as absent; ${root}, ${host} and ${PATH} stand for
// the served root, the host and the PATH, and "*" for a value that
// differs from run to run (a modification time, the reach).
type exchange struct {
	Name   string          `json:"name"`
	Token  string          `json:"token,omitempty"`
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Type   string          `json:"type,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"`
	Raw    *string         `json:"raw,omitempty"`
	Status int             `json:"status"`
	Form   string          `json:"form"` // json, ndjson, raw or none
	Reply  json.RawMessage `json:"reply,omitempty"`

	Background bool `json:"background,omitempty"`
}

// corpusTokens are the principals the exchanges are sent as.
var corpusTokens = map[string]Principal{
	"all":     {Name: "all", Scopes: allScopes()},
	"read":    {Name: "read", Scopes: []Scope{ScopeRead}},
	"servers": {Name: "servers", Scopes: []Scope{ScopeServers}},
}

// kindWorkspace is a Local whose files refuse kind/<code> with that
// code's sentinel, the error's text naming a path on this machine, so
// that every kind, and that no such path crosses, is in the corpus.
type kindWorkspace struct{ *agentworkspace.Local }

func (k kindWorkspace) FS() fs.FS { return kindFS{k.Local.FS().(fs.ReadLinkFS)} }

type kindFS struct{ fs.ReadLinkFS }

func (k kindFS) refused(op, name string) error {
	code, ok := strings.CutPrefix(name, "kind/")
	if !ok {
		return nil
	}
	if code == codeOther {
		return errors.New("a failure of no kind, at /srv/hidden/" + name)
	}
	for _, c := range codes {
		if c.code == code {
			return &fs.PathError{Op: op, Path: "/srv/hidden/" + name, Err: c.err}
		}
	}
	return nil
}

func (k kindFS) Open(name string) (fs.File, error) {
	if err := k.refused("open", name); err != nil {
		return nil, err
	}
	return k.ReadLinkFS.Open(name)
}

func (k kindFS) Stat(name string) (fs.FileInfo, error) {
	if err := k.refused("stat", name); err != nil {
		return nil, err
	}
	return fs.Stat(k.ReadLinkFS, name)
}

func (k kindFS) Lstat(name string) (fs.FileInfo, error) {
	if err := k.refused("lstat", name); err != nil {
		return nil, err
	}
	return k.ReadLinkFS.Lstat(name)
}

// corpusWorkspace is kindWorkspace offering two servers: echo, and
// holder, which is given a secret.
type corpusWorkspace struct {
	kindWorkspace
	offer *agentworkspace.Offering
}

func (c corpusWorkspace) Servers(ctx context.Context) ([]agentworkspace.Server, error) {
	return c.offer.Servers(ctx)
}

func (c corpusWorkspace) StartServer(ctx context.Context, name string, stderr io.Writer) (agentworkspace.Process, error) {
	return c.offer.StartServer(ctx, name, stderr)
}

func newCorpusWorkspace(t *testing.T, l *agentworkspace.Local) corpusWorkspace {
	t.Helper()
	k := kindWorkspace{l}
	o, err := agentworkspace.Offer(k, []agentworkspace.OfferedServer{
		{Server: agentworkspace.Server{Name: "echo", Description: "says it back"}, Command: agentworkspace.Command{Args: []string{"cat"}}},
		{Server: agentworkspace.Server{Name: "holder", Description: "holds a token"}, Command: agentworkspace.Command{Args: []string{"cat"}, Env: []string{"TOKEN=s3cr3t"}}, Secrets: []string{"TOKEN"}},
	}, agentworkspace.WithOutputFilter(func(_ string, _ map[string]string, r io.Reader) io.Reader { return r }))
	if err != nil {
		t.Fatal(err)
	}
	return corpusWorkspace{k, o}
}

func (k kindFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if err := k.refused("readdir", name); err != nil {
		return nil, err
	}
	return fs.ReadDir(k.ReadLinkFS, name)
}

// corpusLayout makes the workspace the exchanges run in.
func corpusLayout(t *testing.T) (dir string) {
	t.Helper()
	base := t.TempDir()
	dir = filepath.Join(base, "work")
	for _, d := range []string{dir, filepath.Join(dir, "sub"), filepath.Join(base, "outside")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range map[string]string{"inside.txt": "ok", "sub/a.txt": "a", "../outside/secret.txt": "hunter2"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	os.Symlink("inside.txt", filepath.Join(dir, "alias.txt"))
	os.Symlink("../outside/secret.txt", filepath.Join(dir, "linkout"))
	return dir
}

func TestTheWireCorpus(t *testing.T) {
	raw, err := os.ReadFile(corpusFile)
	if err != nil {
		t.Fatal(err)
	}
	var c corpus
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	dir := corpusLayout(t)
	l, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH"), "A=1"}, agentworkspace.WithHost("corpus-host"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	_, url := serve(t, newCorpusWorkspace(t, l), WithAuthenticator(grant(corpusTokens)), WithLease(time.Minute), WithMaxExecOutputBytes(64), WithMaxProcesses(2))
	port := strconv.Itoa(workspacetest.Upper(t))
	session := ""
	type pending struct {
		e    *exchange
		resp *http.Response
		done chan struct{}
		form string
		data json.RawMessage
	}
	var background []*pending
	check := func(e *exchange, resp *http.Response, form string, reply json.RawMessage) {
		t.Helper()
		if *update {
			e.Status, e.Form, e.Reply = resp.StatusCode, form, reply
			return
		}
		if resp.StatusCode != e.Status || form != e.Form || !jsonEqual(reply, e.Reply) {
			t.Errorf("%s: %s %s\n got %d %s %s\nwant %d %s %s", e.Name, e.Method, e.Path, resp.StatusCode, form, reply, e.Status, e.Form, e.Reply)
		}
	}
	for i := range c.Exchanges {
		e := &c.Exchanges[i]
		path := strings.ReplaceAll(e.Path, "${session}", session)
		var body io.Reader
		switch {
		case e.Raw != nil:
			body = strings.NewReader(*e.Raw)
		case e.Body != nil:
			body = bytes.NewReader(bytes.ReplaceAll(e.Body, []byte(`"${port}"`), []byte(port)))
		}
		req, err := http.NewRequest(e.Method, url+path, body)
		if err != nil {
			t.Fatal(err)
		}
		if e.Token != "" {
			req.Header.Set("Authorization", "Bearer "+e.Token)
		}
		if e.Type != "" {
			req.Header.Set("Content-Type", e.Type)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", e.Name, err)
		}
		if e.Background {
			p := &pending{e: e, resp: resp, done: make(chan struct{})}
			go func() {
				defer close(p.done)
				p.form, p.data = replyOf(t, resp, l.Root())
				resp.Body.Close()
			}()
			background = append(background, p)
			continue
		}
		form, reply := replyOf(t, resp, l.Root())
		resp.Body.Close()
		if e.Method == http.MethodPost && path == routeSessions && resp.StatusCode == http.StatusOK {
			var s sessionReply
			json.Unmarshal(reply, &s)
			session = s.ID
			reply = bytes.Replace(reply, []byte(session), []byte("${session}"), 1)
		}
		check(e, resp, form, reply)
	}
	for _, p := range background {
		within(t, 15*time.Second, p.e.Name, func() { <-p.done })
		check(p.e, p.resp, p.form, p.data)
	}
	if *update {
		var out bytes.Buffer
		enc := json.NewEncoder(&out)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		if err := enc.Encode(c); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(corpusFile, out.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// replyOf is resp's body, normalized, and its form.
func replyOf(t *testing.T, resp *http.Response, root string) (string, json.RawMessage) {
	t.Helper()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	ctype := resp.Header.Get("Content-Type")
	switch {
	case len(data) == 0:
		return "none", nil
	case strings.HasPrefix(ctype, "application/x-ndjson"):
		var lines []any
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			var v any
			if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, normalize(v, root))
		}
		return "ndjson", mustJSON(t, lines)
	case strings.HasPrefix(ctype, "application/json"):
		var v any
		if err := json.Unmarshal(data, &v); err != nil {
			t.Fatal(err)
		}
		return "json", mustJSON(t, normalize(v, root))
	}
	return "raw", mustJSON(t, string(data))
}

// normalize replaces what differs from run to run in v with its
// placeholder.
func normalize(v any, root string) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			switch k {
			case "modTime", "reach":
				x[k] = "*"
			case "host":
				x[k] = "${host}"
			default:
				x[k] = normalize(e, root)
			}
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = normalize(e, root)
		}
		return x
	case string:
		if strings.HasPrefix(x, "PATH=") {
			return "PATH=${PATH}"
		}
		return strings.ReplaceAll(x, root, "${root}")
	}
	return v
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// jsonEqual reports whether a and b are the same JSON value, types
// and all: numbers by their text, so 0 and "0" differ, as do true and
// "true", and ["a","b"] and ["a b"].
func jsonEqual(a, b json.RawMessage) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	decode := func(m json.RawMessage) (any, bool) {
		d := json.NewDecoder(bytes.NewReader(m))
		d.UseNumber()
		var v any
		return v, d.Decode(&v) == nil
	}
	x, okx := decode(a)
	y, oky := decode(b)
	return okx && oky && reflect.DeepEqual(x, y)
}

// Every refusal in the corpus maps, on the client, to its code's
// sentinel: the client half of the corpus.
func TestTheCorpusRefusalsMapBack(t *testing.T) {
	raw, err := os.ReadFile(corpusFile)
	if err != nil {
		t.Fatal(err)
	}
	var c corpus
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range c.Exchanges {
		var r errorReply
		switch e.Form {
		case "json":
			if json.Unmarshal(e.Reply, &r) != nil {
				continue
			}
		case "ndjson":
			// An exec's error frame is its last line; its status is 200.
			var frames []execFrame
			if json.Unmarshal(e.Reply, &frames) != nil || len(frames) == 0 || frames[len(frames)-1].Error == nil {
				continue
			}
			r.Error = *frames[len(frames)-1].Error
			e.Status = statusOf(r.Error.Code)
		}
		if r.Error.Code == "" {
			continue
		}
		seen[r.Error.Code] = true
		if got := statusOf(r.Error.Code); got != e.Status {
			t.Errorf("%s: %s is sent with %d, the corpus says %d", e.Name, r.Error.Code, got, e.Status)
		}
		for _, k := range codes {
			if k.code == r.Error.Code && !errors.Is(r.Error.err(), k.err) {
				t.Errorf("%s: %s maps to %v, want %v", e.Name, r.Error.Code, r.Error.err(), k.err)
			}
		}
		if strings.Contains(r.Error.Message, "/srv/hidden") {
			t.Errorf("%s: a served path crossed: %q", e.Name, r.Error.Message)
		}
	}
	for _, k := range codes {
		if !seen[k.code] {
			t.Errorf("the corpus has no refusal of code %s", k.code)
		}
	}
}
