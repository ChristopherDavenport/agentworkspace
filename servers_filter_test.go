package agentworkspace

import (
	"bufio"
	"context"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// replacer is an OutputFilter's reader for the tests: a line at a time,
// each secret's value written as its name in brackets.
type replacer struct {
	r      *bufio.Reader
	values map[string]string
	buf    []byte
	err    error
}

func (f *replacer) Read(p []byte) (int, error) {
	for len(f.buf) == 0 {
		if f.err != nil {
			return 0, f.err
		}
		line, err := f.r.ReadString('\n')
		for name, v := range f.values {
			line = strings.ReplaceAll(line, v, "["+name+"]")
		}
		f.buf, f.err = []byte(line), err
	}
	n := copy(p, f.buf)
	f.buf = f.buf[n:]
	return n, nil
}

// filterCalls records what an OutputFilter was told.
type filterCalls struct {
	mu    sync.Mutex
	calls []string
}

func (c *filterCalls) filter(server string, secrets map[string]string, r io.Reader) io.Reader {
	c.mu.Lock()
	c.calls = append(c.calls, server+" "+secrets["TOKEN"])
	c.mu.Unlock()
	return &replacer{r: bufio.NewReader(r), values: secrets}
}

func (c *filterCalls) seen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

const token = "tok-0123-fake"

func offerFiltered(t *testing.T, args []string, c *filterCalls) *Offering {
	t.Helper()
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	o, err := Offer(l, []OfferedServer{{
		Server:  Server{Name: "s"},
		Command: Command{Args: args, Env: []string{"TOKEN=" + token, "PLAIN=shown"}},
		Secrets: []string{"TOKEN"},
	}}, WithOutputFilter(c.filter))
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// A server naming a secret has its standard output and its standard
// error read through the filter, told the server's name and the
// secret's value: the conversation still goes a line at a time, and the
// standard error the filter gives is all written once Wait returns.
func TestAFilterReadsAnOfferedServersOutput(t *testing.T) {
	var c filterCalls
	o := offerFiltered(t, []string{"sh", "-c", `echo "err $TOKEN $PLAIN" >&2; echo "out $TOKEN"; exec cat`}, &c)
	var stderr syncBuffer
	p, err := o.StartServer(context.Background(), "s", &stderr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	out := bufio.NewReader(p.Stdout())
	within(t, 10*time.Second, "the conversation", func() {
		if got, err := out.ReadString('\n'); got != "out [TOKEN]\n" || err != nil {
			t.Errorf("first line = %q, %v", got, err)
		}
		io.WriteString(p.Stdin(), "echo "+token+"\n")
		if got, err := out.ReadString('\n'); got != "echo [TOKEN]\n" || err != nil {
			t.Errorf("answer = %q, %v", got, err)
		}
		p.Stdin().Close()
		p.Wait()
	})
	if got := stderr.String(); got != "err [TOKEN] shown\n" {
		t.Errorf("standard error = %q", got)
	}
	if got := c.seen(); len(got) != 2 || got[0] != "s "+token || got[1] != "s "+token {
		t.Errorf("the filter was told %q; want the server and its secret, for each stream", got)
	}
}

// A filter that holds everything back until its end of file still has
// its standard error written by the time Close returns.
func TestTheStandardErrorTailIsWrittenBeforeClose(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	hold := func(_ string, secrets map[string]string, r io.Reader) io.Reader {
		pr, pw := io.Pipe()
		go func() {
			b, err := io.ReadAll(r)
			pw.Write([]byte(strings.ReplaceAll(string(b), secrets["TOKEN"], "[TOKEN]")))
			pw.CloseWithError(err)
		}()
		return pr
	}
	o, err := Offer(l, []OfferedServer{{
		Server:  Server{Name: "s"},
		Command: Command{Args: []string{"sh", "-c", `echo "last $TOKEN" >&2; exec sleep 1000`}, Env: []string{"TOKEN=" + token}},
		Secrets: []string{"TOKEN"},
	}}, WithOutputFilter(hold))
	if err != nil {
		t.Fatal(err)
	}
	var stderr syncBuffer
	p, err := o.StartServer(context.Background(), "s", &stderr)
	if err != nil {
		t.Fatal(err)
	}
	// Read standard output to its end, as a client does, so the
	// holding filter's copy of it is not left waiting.
	go io.Copy(io.Discard, p.Stdout())
	time.Sleep(200 * time.Millisecond)
	within(t, 15*time.Second, "Close", func() { p.Close() })
	if got := stderr.String(); got != "last [TOKEN]\n" {
		t.Errorf("standard error after Close = %q", got)
	}
}

// A server that names no secret, or an Offering with no filter, starts
// as before: nothing reads its output.
func TestNoSecretsOrNoFilterIsUnfiltered(t *testing.T) {
	var c filterCalls
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	cmd := Command{Args: []string{"sh", "-c", `echo "out $TOKEN"`}, Env: []string{"TOKEN=" + token}}
	plain, err := Offer(l, []OfferedServer{{Server: Server{Name: "s"}, Command: cmd}}, WithOutputFilter(c.filter))
	if err != nil {
		t.Fatal(err)
	}
	unfiltered, err := Offer(l, []OfferedServer{{Server: Server{Name: "s"}, Command: cmd, Secrets: []string{"TOKEN"}}})
	if err != nil {
		t.Fatal(err)
	}
	for name, o := range map[string]*Offering{"no secrets": plain, "no filter": unfiltered} {
		p, err := o.StartServer(context.Background(), "s", nil)
		if err != nil {
			t.Fatal(err)
		}
		within(t, 10*time.Second, name, func() {
			b, _ := io.ReadAll(p.Stdout())
			if string(b) != "out "+token+"\n" {
				t.Errorf("%s: output = %q", name, b)
			}
			p.Close()
		})
	}
	if got := c.seen(); len(got) != 0 {
		t.Errorf("the filter was called: %q", got)
	}
}

// With no stderr, standard output is still filtered.
func TestAFilteredServerWithNoStderr(t *testing.T) {
	var c filterCalls
	o := offerFiltered(t, []string{"sh", "-c", `echo "out $TOKEN"; echo "err $TOKEN" >&2`}, &c)
	p, err := o.StartServer(context.Background(), "s", nil)
	if err != nil {
		t.Fatal(err)
	}
	within(t, 10*time.Second, "the output", func() {
		b, _ := io.ReadAll(p.Stdout())
		if string(b) != "out [TOKEN]\n" {
			t.Errorf("output = %q", b)
		}
		p.Close()
	})
}

// Nothing a filtered start begins outlives the process: once it is
// closed, the goroutines are gone.
func TestAFilteredServerLeavesNoGoroutine(t *testing.T) {
	var c filterCalls
	o := offerFiltered(t, []string{"sh", "-c", `echo "err $TOKEN" >&2; exec cat`}, &c)
	before := runtime.NumGoroutine()
	for range 5 {
		var stderr syncBuffer
		p, err := o.StartServer(context.Background(), "s", &stderr)
		if err != nil {
			t.Fatal(err)
		}
		go io.Copy(io.Discard, p.Stdout())
		within(t, 15*time.Second, "Close", func() { p.Close() })
	}
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("%d goroutines, %d before:\n%s", runtime.NumGoroutine(), before, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOfferRefusesBadSecrets(t *testing.T) {
	l, _ := local(t, nil)
	srv := func(env []string, secrets ...string) []OfferedServer {
		return []OfferedServer{{Server: Server{Name: "s"}, Command: Command{Args: []string{"cat"}, Env: env}, Secrets: secrets}}
	}
	for what, servers := range map[string][]OfferedServer{
		"not a name":     srv([]string{"A-B=x"}, "A-B"),
		"a digit first":  srv([]string{"1X=x"}, "1X"),
		"empty":          srv(nil, ""),
		"not in Env":     srv([]string{"OTHER=x"}, "TOKEN"),
		"a prefix only":  srv([]string{"TOKENS=x"}, "TOKEN"),
		"set twice":      srv([]string{"TOKEN=a", "TOKEN=b"}, "TOKEN"),
		"named twice":    srv([]string{"TOKEN=a"}, "TOKEN", "TOKEN"),
		"an equals sign": srv([]string{"A=B=x"}, "A=B"),
	} {
		if _, err := Offer(l, servers); err == nil {
			t.Errorf("%s: Offer took it", what)
		}
	}
	if _, err := Offer(l, srv([]string{"TOKEN=a", "_X1=b"}, "TOKEN", "_X1")); err != nil {
		t.Errorf("good secrets refused: %v", err)
	}
}

// Offer copies Secrets as it copies the command: a caller changing its
// slice afterwards changes nothing the filter is told.
func TestOfferCopiesTheSecrets(t *testing.T) {
	var c filterCalls
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	secrets := []string{"TOKEN"}
	o, err := Offer(l, []OfferedServer{{
		Server:  Server{Name: "s"},
		Command: Command{Args: []string{"sh", "-c", `echo "out $TOKEN"`}, Env: []string{"TOKEN=" + token, "PLAIN=x"}},
		Secrets: secrets,
	}}, WithOutputFilter(c.filter))
	if err != nil {
		t.Fatal(err)
	}
	secrets[0] = "PLAIN"
	p, err := o.StartServer(context.Background(), "s", nil)
	if err != nil {
		t.Fatal(err)
	}
	within(t, 10*time.Second, "the output", func() {
		b, _ := io.ReadAll(p.Stdout())
		if string(b) != "out [TOKEN]\n" {
			t.Errorf("output = %q; want the secret as offered", b)
		}
		p.Close()
	})
}
