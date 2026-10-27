package mcpworkspace

// The wire corpus, testdata/wire/mcpworkspace.json: JSON-RPC requests
// and their replies, in order, after an MCP initialization, that the
// reference server must answer as they say and whose refusals the
// reference client must map to their kind (RFC 0001 §16). Run with
// -update to record the replies of a changed server, then read the
// diff.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var update = flag.Bool("update", false, "record the corpus's replies from the reference server")

const corpusFile = "testdata/wire/mcpworkspace.json"

type corpus struct {
	Doc       []string   `json:"doc"`
	Exchanges []exchange `json:"exchanges"`
}

// exchange is one request, sent with the next id, and the reply's
// result or error. ${root} stands for the served root, and "*" for a
// value that differs from run to run (a modification time, the reach).
type exchange struct {
	Name   string          `json:"name"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
}

// kindStarter is a Local whose files refuse k/<kind>, and whose starts
// refuse args[0] kind:<kind>, with that kind's sentinel, the error's
// text naming a path on this machine.
type kindStarter struct{ *agentworkspace.Local }

func kindRefusal(op, name string) error {
	kind, ok := strings.CutPrefix(name, "k/")
	if !ok {
		kind, ok = strings.CutPrefix(name, "kind:")
	}
	if !ok {
		return nil
	}
	return &fs.PathError{Op: op, Path: "/srv/hidden/" + kind, Err: sentinelOf(kind)}
}

func (k kindStarter) FS() fs.FS { return kindFiles{k.Local.FS().(fs.ReadLinkFS)} }

func (k kindStarter) Start(ctx context.Context, cmd agentworkspace.Command) (agentworkspace.Process, error) {
	if len(cmd.Args) > 0 {
		if err := kindRefusal("start", cmd.Args[0]); err != nil {
			return nil, err
		}
	}
	return k.Local.Start(ctx, cmd)
}

type kindFiles struct{ fs.ReadLinkFS }

func (k kindFiles) Open(name string) (fs.File, error) {
	if err := kindRefusal("open", name); err != nil {
		return nil, err
	}
	return k.ReadLinkFS.Open(name)
}

func (k kindFiles) Lstat(name string) (fs.FileInfo, error) {
	if err := kindRefusal("lstat", name); err != nil {
		return nil, err
	}
	return k.ReadLinkFS.Lstat(name)
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
	base := t.TempDir()
	dir := filepath.Join(base, "work")
	for _, d := range []string{dir, filepath.Join(dir, "sub"), filepath.Join(base, "outside")} {
		os.MkdirAll(d, 0o755)
	}
	for name, data := range map[string]string{"inside.txt": "ok", "sub/a.txt": "a", "../outside/secret.txt": "hunter2"} {
		os.WriteFile(filepath.Join(dir, name), []byte(data), 0o644)
	}
	os.Symlink("inside.txt", filepath.Join(dir, "alias.txt"))
	os.Symlink("../outside/secret.txt", filepath.Join(dir, "linkout"))
	l, err := agentworkspace.NewLocal(dir, []string{"PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	ws := kindStarter{l}
	procs := NewProcesses(ws)
	t.Cleanup(procs.Close)
	srv := mcp.NewServer(&mcp.Implementation{Name: "corpus", Version: "test"}, &mcp.ServerOptions{
		Capabilities: &mcp.ServerCapabilities{Resources: &mcp.ResourceCapabilities{}},
	})
	ServeFiles(srv, ws.FS())
	if err := procs.Register(srv); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ss.Close() })
	conn, err := ct.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	id := int64(0)
	call := func(method string, params json.RawMessage) *jsonrpc.Response {
		t.Helper()
		id++
		rid, _ := jsonrpc.MakeID(float64(id))
		if err := conn.Write(ctx, &jsonrpc.Request{ID: rid, Method: method, Params: params}); err != nil {
			t.Fatal(err)
		}
		for {
			msg, err := conn.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if r, ok := msg.(*jsonrpc.Response); ok {
				return r
			}
		}
	}
	call("initialize", json.RawMessage(`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"corpus","version":"0"}}`))
	if err := conn.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized", Params: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for i := range c.Exchanges {
		e := &c.Exchanges[i]
		r := call(e.Method, e.Params)
		var result, rerr json.RawMessage
		if r.Error != nil {
			rerr = mustJSON(t, normalized(t, mustJSON(t, r.Error), dir))
		} else {
			result = mustJSON(t, normalized(t, r.Result, dir))
		}
		if *update {
			e.Result, e.Error = result, rerr
			continue
		}
		if !jsonEqual(result, e.Result) || !jsonEqual(rerr, e.Error) {
			t.Errorf("%s: %s %s\n got %s %s\nwant %s %s", e.Name, e.Method, e.Params, result, rerr, e.Result, e.Error)
		}
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

// normalized is data with what differs from run to run replaced by its
// placeholder, a resource's text, itself JSON, normalized inside.
func normalized(t *testing.T, data json.RawMessage, root string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	var walk func(any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				switch k {
				case "modTime", "reach":
					x[k] = "*"
				case "text":
					if s, ok := e.(string); ok && strings.HasPrefix(s, "{") {
						var inner any
						if json.Unmarshal([]byte(s), &inner) == nil {
							b, _ := json.Marshal(walk(inner))
							x[k] = string(b)
						}
					}
				default:
					x[k] = walk(e)
				}
			}
			return x
		case []any:
			for i, e := range x {
				x[i] = walk(e)
			}
			return x
		case string:
			return strings.ReplaceAll(x, root, "${root}")
		}
		return v
	}
	return walk(v)
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func jsonEqual(a, b json.RawMessage) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return fmt.Sprint(x) == fmt.Sprint(y)
}

// Every refusal in the corpus, a method's error or a file's, maps on
// the client to its kind's sentinel, names no served path, and every
// kind is among them: the client half of the corpus.
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
	check := func(name, kind string, err error) {
		seen[kind] = true
		if want := sentinelOf(kind); want != nil && !errors.Is(err, want) {
			t.Errorf("%s: kind %s maps to %v, want %v", name, kind, err, want)
		}
		if err != nil && strings.Contains(err.Error(), "/srv/hidden") {
			t.Errorf("%s: a served path crossed: %q", name, err)
		}
	}
	for _, e := range c.Exchanges {
		if e.Error != nil {
			var we jsonrpc.Error
			if err := json.Unmarshal(e.Error, &we); err != nil {
				t.Fatal(err)
			}
			var r wire.Refusal
			json.Unmarshal(we.Data, &r)
			if r.Kind != "" {
				check(e.Name, r.Kind, fromWire(&we))
			}
			continue
		}
		var res mcp.ReadResourceResult
		if e.Method != "resources/read" || json.Unmarshal(e.Result, &res) != nil || len(res.Contents) != 1 {
			continue
		}
		var reply wire.FileReply
		if json.Unmarshal([]byte(res.Contents[0].Text), &reply) == nil && reply.Error != nil {
			f := &remoteFS{name: "corpus"}
			check(e.Name, reply.Error.Kind, f.kindErr(reply.Error))
		}
	}
	for _, kind := range wire.Kinds() {
		if !seen[kind] {
			t.Errorf("the corpus has no refusal of kind %s", kind)
		}
	}
}
