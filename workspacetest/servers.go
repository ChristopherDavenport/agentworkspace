package workspacetest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
)

// ServerProvider is a ServerProvider the server tables run on. Open
// offers servers, each started from its command, through opts, from a
// workspace over dir whose environment is env, and returns it as its
// client sees it: an agentworkspace.Offering, or one served and dialled
// over it. It closes what it opened when the test ends. The provider's
// Close, when it has one (an io.Closer), is its owner's: it ends every
// server the provider started.
type ServerProvider struct {
	Name string
	Open func(t *testing.T, dir string, env []string, servers []agentworkspace.OfferedServer, opts ...agentworkspace.OfferOption) agentworkspace.ServerProvider
}

// RunServerProvider runs every table a ServerProvider keeps (the list,
// the pipes, the workspace a server runs in, ErrNoServer, the
// lifetimes, standard error, and secrets kept out of the output) on
// every provider, each a subtest named after the table and then the
// provider.
func RunServerProvider(t *testing.T, providers ...ServerProvider) {
	for _, table := range []struct {
		name string
		run  func(*testing.T, ServerProvider)
	}{
		{"ListsWhatItOffers", serversListed},
		{"EchoesThroughThePipes", serverEchoes},
		{"RunsInTheWorkspace", serverRunsInTheWorkspace},
		{"ANameNotOfferedIsErrNoServer", serverNotOffered},
		{"EndsWithItsOwner", serverEndsWithItsOwner},
		{"StandardErrorNowhere", serverStderrNowhere},
		{"SecretsStayOutOfItsOutput", serverSecretsStayOut},
	} {
		t.Run(table.name, func(t *testing.T) {
			for _, p := range providers {
				t.Run(p.Name, func(t *testing.T) { table.run(t, p) })
			}
		})
	}
}

// pathOnly is the environment the tables' workspaces have unless they
// say otherwise.
func pathOnly() []string { return []string{"PATH=" + os.Getenv("PATH")} }

// offered is one server called name, started from args.
func offered(name, description string, args ...string) agentworkspace.OfferedServer {
	return agentworkspace.OfferedServer{
		Server:  agentworkspace.Server{Name: name, Description: description},
		Command: agentworkspace.Command{Args: args},
	}
}

// startServer starts name in p, failing the test if it cannot, and
// closes the process when the test ends.
func startServer(t *testing.T, ctx context.Context, p agentworkspace.ServerProvider, name string, stderr io.Writer) agentworkspace.Process {
	t.Helper()
	proc, err := p.StartServer(ctx, name, stderr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { proc.Close() })
	return proc
}

// lockedBuffer is a bytes.Buffer safe for a stream's writer and the
// test's reader at once.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Servers lists every server offered, once, with its description, in
// the same order every time; a provider offering none lists none.
func serversListed(t *testing.T, sp ServerProvider) {
	ctx := context.Background()
	p := sp.Open(t, t.TempDir(), pathOnly(), []agentworkspace.OfferedServer{
		offered("b-two", "the second", "cat"),
		offered("a-one", "", "cat"),
		offered("c3", "the third", "cat"),
	})
	first, err := p.Servers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a-one": "", "b-two": "the second", "c3": "the third"}
	got := map[string]string{}
	for _, s := range first {
		if _, twice := got[s.Name]; twice {
			t.Errorf("%s is listed twice", s.Name)
		}
		got[s.Name] = s.Description
	}
	if len(got) != len(want) {
		t.Errorf("Servers = %+v, want %v", first, want)
	}
	for name, d := range want {
		if g, ok := got[name]; !ok || g != d {
			t.Errorf("Servers = %+v: %s missing or described %q, want %q", first, name, g, d)
		}
	}
	again, err := p.Servers(ctx)
	if err != nil || !slices.Equal(first, again) {
		t.Errorf("Servers again = %+v, %v; want the same list in the same order, %+v", again, err, first)
	}

	none := sp.Open(t, t.TempDir(), pathOnly(), nil)
	if list, err := none.Servers(ctx); err != nil || len(list) != 0 {
		t.Errorf("Servers of a provider offering none = %+v, %v", list, err)
	}
}

// A started server is a conversation over its pipes, a line answered
// before the next is written, and its standard error goes to the
// writer StartServer is given, all of it by the time Wait returns.
func serverEchoes(t *testing.T, sp ServerProvider) {
	p := sp.Open(t, t.TempDir(), pathOnly(), []agentworkspace.OfferedServer{
		offered("echo", "says it back", "sh", "-c", "echo started >&2; exec cat"),
	})
	var stderr lockedBuffer
	proc := startServer(t, context.Background(), p, "echo", &stderr)
	out := bufio.NewReader(proc.Stdout())
	within(t, 10*time.Second, "the conversation", func() {
		for _, line := range []string{`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, "second"} {
			if _, err := io.WriteString(proc.Stdin(), line+"\n"); err != nil {
				t.Error(err)
				return
			}
			if got, err := out.ReadString('\n'); err != nil || got != line+"\n" {
				t.Errorf("answer = %q, %v; want %q", got, err, line)
				return
			}
		}
		proc.Stdin().Close()
		if rest, err := io.ReadAll(out); err != nil || len(rest) != 0 {
			t.Errorf("after end of input: %q, %v", rest, err)
		}
		if status, err := proc.Wait(); status != 0 || err != nil {
			t.Errorf("Wait = %d, %v", status, err)
		}
	})
	if got := stderr.String(); got != "started\n" {
		t.Errorf("standard error = %q, want the server's", got)
	}
}

// A server's command runs in the workspace, at its Dir, with the
// workspace's environment and its own Env added.
func serverRunsInTheWorkspace(t *testing.T, sp ServerProvider) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	s := offered("where", "", "sh", "-c", "pwd; echo $A$B")
	s.Command.Dir, s.Command.Env = "sub", []string{"B=2"}
	p := sp.Open(t, dir, append([]string{"A=1"}, pathOnly()...), []agentworkspace.OfferedServer{s})
	proc := startServer(t, context.Background(), p, "where", nil)
	var got []byte
	within(t, 10*time.Second, "the server's output", func() {
		got, _ = io.ReadAll(proc.Stdout())
		proc.Wait()
	})
	if want := filepath.Join(dir, "sub") + "\n12\n"; string(got) != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

// A name the provider does not offer is ErrNoServer, and starts
// nothing; so is any name of a provider that offers none.
func serverNotOffered(t *testing.T, sp ServerProvider) {
	ctx := context.Background()
	for what, servers := range map[string][]agentworkspace.OfferedServer{
		"another offered": {offered("there", "", "cat")},
		"none offered":    nil,
	} {
		p := sp.Open(t, t.TempDir(), pathOnly(), servers)
		var stderr lockedBuffer
		if proc, err := p.StartServer(ctx, "not-there", &stderr); !errors.Is(err, agentworkspace.ErrNoServer) {
			if proc != nil {
				proc.Close()
			}
			t.Errorf("%s: StartServer = %v, want ErrNoServer", what, err)
		}
	}
}

// A started server lives no longer than its owner: Close, the end of
// StartServer's context and the provider's Close each end it and what
// it started, so its output ends.
func serverEndsWithItsOwner(t *testing.T, sp ServerProvider) {
	servers := []agentworkspace.OfferedServer{offered("s", "", "sh", "-c", "sleep 30 & echo started; wait")}
	// running starts s and waits for it to say it is running, through
	// its own output.
	running := func(t *testing.T, p agentworkspace.ServerProvider, ctx context.Context) (agentworkspace.Process, *bufio.Reader) {
		t.Helper()
		proc := startServer(t, ctx, p, "s", nil)
		out := bufio.NewReader(proc.Stdout())
		var line string
		var err error
		within(t, 10*time.Second, "the first line", func() { line, err = out.ReadString('\n') })
		if err != nil || line != "started\n" {
			t.Fatalf("first line = %q, %v", line, err)
		}
		return proc, out
	}
	// ended waits for the output's end and Wait, and returns Wait's.
	ended := func(t *testing.T, what string, proc agentworkspace.Process, out *bufio.Reader) (status int, err error) {
		t.Helper()
		within(t, 10*time.Second, what, func() {
			io.Copy(io.Discard, out)
			status, err = proc.Wait()
		})
		return status, err
	}
	t.Run("Close", func(t *testing.T) {
		p := sp.Open(t, t.TempDir(), pathOnly(), servers)
		proc, out := running(t, p, context.Background())
		within(t, 10*time.Second, "Close", func() { proc.Close() })
		if status, err := ended(t, "the end after Close", proc, out); status != -1 || err != nil {
			t.Errorf("Wait after Close = %d, %v; want -1, nil", status, err)
		}
	})
	t.Run("Context", func(t *testing.T) {
		p := sp.Open(t, t.TempDir(), pathOnly(), servers)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		proc, out := running(t, p, ctx)
		cancel()
		if status, err := ended(t, "the end after cancel", proc, out); status != -1 || !errors.Is(err, context.Canceled) {
			t.Errorf("Wait after cancel = %d, %v; want -1, context.Canceled", status, err)
		}
	})
	t.Run("Owner", func(t *testing.T) {
		p := sp.Open(t, t.TempDir(), pathOnly(), servers)
		owner, ok := p.(io.Closer)
		if !ok {
			t.Skipf("%T has no Close", p)
		}
		proc, out := running(t, p, context.Background())
		within(t, 10*time.Second, "the owner's Close", func() { owner.Close() })
		if status, _ := ended(t, "the end after the owner's Close", proc, out); status != -1 {
			t.Errorf("Wait after the owner's Close = %d, want -1", status)
		}
		if proc, err := p.StartServer(context.Background(), "s", nil); err == nil {
			proc.Close()
			t.Error("a closed provider started a server")
		}
	})
}

// With no writer, a server's standard error goes nowhere, and the
// conversation is as with one.
func serverStderrNowhere(t *testing.T, sp ServerProvider) {
	p := sp.Open(t, t.TempDir(), pathOnly(), []agentworkspace.OfferedServer{
		offered("loud", "", "sh", "-c", "echo noise >&2; exec cat"),
	})
	proc := startServer(t, context.Background(), p, "loud", nil)
	out := bufio.NewReader(proc.Stdout())
	within(t, 10*time.Second, "the conversation", func() {
		io.WriteString(proc.Stdin(), "ping\n")
		if got, err := out.ReadString('\n'); got != "ping\n" || err != nil {
			t.Errorf("answer = %q, %v", got, err)
		}
		proc.Stdin().Close()
		if status, err := proc.Wait(); status != 0 || err != nil {
			t.Errorf("Wait = %d, %v", status, err)
		}
	})
}

// secretToken is the secret the secrets table's server holds.
const secretToken = "tok-workspacetest-0123456789"

// redactLines is an OutputFilter for the tables: a line at a time, each
// secret's value written as its name in brackets.
func redactLines(_ string, secrets map[string]string, r io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		br := bufio.NewReader(r)
		for {
			line, err := br.ReadString('\n')
			for name, v := range secrets {
				line = strings.ReplaceAll(line, v, "["+name+"]")
			}
			if line != "" {
				if _, werr := io.WriteString(pw, line); werr != nil {
					pr.CloseWithError(werr)
					return
				}
			}
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				pw.CloseWithError(err)
				return
			}
		}
	}()
	return pr
}

// A server's secrets are named on its offer and never reach its
// client: the server prints its secret on standard output and standard
// error, and echoes one written to it, and what the client reads of
// both has the filter's mark in its place, the conversation still a
// line at a time. Nothing the client is told holds it.
func serverSecretsStayOut(t *testing.T, sp ServerProvider) {
	s := offered("holder", "holds a token", "sh", "-c", `echo "err $TOKEN $PLAIN" >&2; echo "out $TOKEN"; exec cat`)
	s.Command.Env = []string{"TOKEN=" + secretToken, "PLAIN=shown"}
	s.Secrets = []string{"TOKEN"}
	p := sp.Open(t, t.TempDir(), pathOnly(), []agentworkspace.OfferedServer{s}, agentworkspace.WithOutputFilter(redactLines))
	var stderr lockedBuffer
	proc := startServer(t, context.Background(), p, "holder", &stderr)
	var seen strings.Builder
	out := bufio.NewReader(proc.Stdout())
	within(t, 10*time.Second, "the conversation", func() {
		got, err := out.ReadString('\n')
		seen.WriteString(got)
		if got != "out [TOKEN]\n" || err != nil {
			t.Errorf("first line = %q, %v", got, err)
		}
		io.WriteString(proc.Stdin(), "echo "+secretToken+"\n")
		got, err = out.ReadString('\n')
		seen.WriteString(got)
		if got != "echo [TOKEN]\n" || err != nil {
			t.Errorf("answer = %q, %v", got, err)
		}
		proc.Stdin().Close()
		rest, _ := io.ReadAll(out)
		seen.Write(rest)
		proc.Wait()
	})
	if got := stderr.String(); got != "err [TOKEN] shown\n" {
		t.Errorf("standard error = %q", got)
	}
	told := []string{seen.String(), stderr.String()}
	if list, err := p.Servers(context.Background()); err == nil {
		for _, sv := range list {
			told = append(told, sv.Name, sv.Description)
		}
	}
	if ws, ok := p.(agentworkspace.Workspace); ok {
		told = append(told, ws.Env()...)
	}
	for _, s := range told {
		if strings.Contains(s, secretToken) {
			t.Errorf("the client was told the secret: %q", s)
		}
	}
}
