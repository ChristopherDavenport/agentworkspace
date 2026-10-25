package agentworkspace

import (
	"bufio"
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
			if v != "" {
				line = strings.ReplaceAll(line, v, "["+name+"]")
			}
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

// A server that names no secret starts as before, filter or not:
// nothing reads its output.
func TestNoSecretsIsUnfiltered(t *testing.T) {
	var c filterCalls
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	cmd := Command{Args: []string{"sh", "-c", `echo "out $TOKEN"`}, Env: []string{"TOKEN=" + token}}
	o, err := Offer(l, []OfferedServer{{Server: Server{Name: "s"}, Command: cmd}}, WithOutputFilter(c.filter))
	if err != nil {
		t.Fatal(err)
	}
	p, err := o.StartServer(context.Background(), "s", nil)
	if err != nil {
		t.Fatal(err)
	}
	within(t, 10*time.Second, "the output", func() {
		b, _ := io.ReadAll(p.Stdout())
		if string(b) != "out "+token+"\n" {
			t.Errorf("output = %q", b)
		}
		p.Close()
	})
	if got := c.seen(); len(got) != 0 {
		t.Errorf("the filter was called: %q", got)
	}
}

// A server that names secrets is refused with no filter to keep them
// out of its output: Offer fails closed.
func TestSecretsWithoutAFilterAreRefused(t *testing.T) {
	l, _ := local(t, nil)
	_, err := Offer(l, []OfferedServer{{
		Server:  Server{Name: "s"},
		Command: Command{Args: []string{"cat"}, Env: []string{"TOKEN=" + token}},
		Secrets: []string{"TOKEN"},
	}})
	if err == nil || !strings.Contains(err.Error(), "WithOutputFilter") {
		t.Fatalf("Offer = %v, want a refusal naming WithOutputFilter", err)
	}
}

// A nil option is skipped.
func TestANilOfferOptionIsSkipped(t *testing.T) {
	var c filterCalls
	l, _ := local(t, nil)
	if _, err := Offer(l, []OfferedServer{{Server: Server{Name: "s"}, Command: Command{Args: []string{"cat"}}}}, nil); err != nil {
		t.Errorf("Offer with a nil option: %v", err)
	}
	if _, err := Offer(l, []OfferedServer{{
		Server:  Server{Name: "s"},
		Command: Command{Args: []string{"cat"}, Env: []string{"TOKEN=" + token}},
		Secrets: []string{"TOKEN"},
	}}, nil, WithOutputFilter(c.filter), nil); err != nil {
		t.Errorf("Offer with nil options around a filter: %v", err)
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
	var c filterCalls
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
		"an empty value": srv([]string{"TOKEN="}, "TOKEN"),
	} {
		if _, err := Offer(l, servers, WithOutputFilter(c.filter)); err == nil {
			t.Errorf("%s: Offer took it", what)
		}
	}
	if _, err := Offer(l, srv([]string{"TOKEN=a", "_X1=b"}, "TOKEN", "_X1"), WithOutputFilter(c.filter)); err != nil {
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

// noLeak fails the test if, within a bound, more goroutines run than
// before did.
func noLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("%d goroutines, %d before:\n%s", runtime.NumGoroutine(), before, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// secretServer is a server named "s" whose TOKEN is a secret, offered
// through filter.
func secretServer(t *testing.T, args []string, filter OutputFilter) (*Offering, *Local, string) {
	t.Helper()
	l, dir := local(t, []string{"PATH=" + os.Getenv("PATH")})
	o, err := Offer(l, []OfferedServer{{
		Server:  Server{Name: "s"},
		Command: Command{Args: args, Env: []string{"TOKEN=" + token}},
		Secrets: []string{"TOKEN"},
	}}, WithOutputFilter(filter))
	if err != nil {
		t.Fatal(err)
	}
	return o, l, dir
}

// A filter that gives no reader, for either stream, or that panics,
// fails the start, and a server already started is ended: the host
// neither crashes nor keeps it.
func TestABadFilterFailsTheStart(t *testing.T) {
	for _, tc := range []struct {
		name string
		bad  int // which call misbehaves: 1 is standard error's, 2 standard output's
		how  string
	}{
		{"nil for standard error", 1, "nil"},
		{"nil for standard output", 2, "nil"},
		{"panic for standard error", 1, "panic"},
		{"panic for standard output", 2, "panic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			n := 0
			filter := func(_ string, secrets map[string]string, r io.Reader) io.Reader {
				mu.Lock()
				n++
				call := n
				mu.Unlock()
				if call == tc.bad {
					if tc.how == "panic" {
						panic("a filter's bug")
					}
					return nil
				}
				return &replacer{r: bufio.NewReader(r), values: secrets}
			}
			o, _, dir := secretServer(t, []string{"sh", "-c", `echo $$ > pid; exec sleep 1000`}, filter)
			before := runtime.NumGoroutine()
			var stderr syncBuffer
			var p Process
			var err error
			within(t, 15*time.Second, "the start", func() { p, err = o.StartServer(context.Background(), "s", &stderr) })
			if err == nil {
				p.Close()
				t.Fatal("StartServer took a filter that misbehaved")
			}
			if tc.bad == 2 {
				// The server may have started before the filter was
				// asked: it is ended.
				within(t, 15*time.Second, "the server's end", func() {
					deadline := time.Now().Add(2 * time.Second)
					for time.Now().Before(deadline) {
						b, rerr := os.ReadFile(dir + "/pid")
						if pid, perr := strconv.Atoi(strings.TrimSpace(string(b))); rerr == nil && perr == nil {
							for syscall.Kill(pid, 0) == nil {
								time.Sleep(20 * time.Millisecond)
							}
							return
						}
						time.Sleep(20 * time.Millisecond)
					}
				})
			}
			noLeak(t, before)
		})
	}
}

// A filter that holds standard error after its end of file, or that
// stops reading it altogether, holds up Close for no longer than the
// grace: past it, what is left is dropped.
func TestAFilterThatHoldsOnIsGivenAGrace(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold func(r io.Reader, release <-chan struct{}) io.Reader
	}{
		{"holds after its end of file", func(r io.Reader, release <-chan struct{}) io.Reader {
			pr, pw := io.Pipe()
			go func() {
				io.Copy(io.Discard, r)
				<-release
				pw.Close()
			}()
			return pr
		}},
		{"never reads", func(r io.Reader, release <-chan struct{}) io.Reader {
			pr, pw := io.Pipe()
			go func() {
				<-release
				pw.Close()
			}()
			return pr
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			var mu sync.Mutex
			n := 0
			filter := func(_ string, secrets map[string]string, r io.Reader) io.Reader {
				mu.Lock()
				n++
				first := n == 1
				mu.Unlock()
				if first {
					return tc.hold(r, release)
				}
				return &replacer{r: bufio.NewReader(r), values: secrets}
			}
			// More standard error than a pipe holds, so a filter that
			// stops reading would block the process's writes.
			o, _, _ := secretServer(t, []string{"sh", "-c", `i=0; while [ $i -lt 4000 ]; do echo "err $TOKEN line $i" >&2; i=$((i+1)); done; exec sleep 1000`}, filter)
			var stderr syncBuffer
			p, err := o.StartServer(context.Background(), "s", &stderr)
			if err != nil {
				t.Fatal(err)
			}
			go io.Copy(io.Discard, p.Stdout())
			time.Sleep(300 * time.Millisecond)
			start := time.Now()
			within(t, filterGrace+10*time.Second, "Close", func() { p.Close() })
			if d := time.Since(start); d > filterGrace+5*time.Second {
				t.Errorf("Close took %v, more than the grace of %v by far", d, filterGrace)
			}
		})
	}
}

// A filtered server with standard error set leaves nothing running when
// its start fails on a closed workspace, when its stderr writer fails,
// and when its context ends.
func TestAFilteredServersLifetimeWithStandardError(t *testing.T) {
	t.Run("a closed workspace", func(t *testing.T) {
		var c filterCalls
		o, l, _ := secretServer(t, []string{"cat"}, c.filter)
		l.Close()
		before := runtime.NumGoroutine()
		var stderr syncBuffer
		if p, err := o.StartServer(context.Background(), "s", &stderr); err == nil {
			p.Close()
			t.Fatal("StartServer on a closed workspace")
		}
		noLeak(t, before)
	})
	t.Run("a failing stderr writer", func(t *testing.T) {
		var c filterCalls
		o, _, _ := secretServer(t, []string{"sh", "-c", `i=0; while [ $i -lt 2000 ]; do echo "err $TOKEN $i" >&2; i=$((i+1)); done; exec cat`}, c.filter)
		before := runtime.NumGoroutine()
		p, err := o.StartServer(context.Background(), "s", failingWriter{})
		if err != nil {
			t.Fatal(err)
		}
		out := bufio.NewReader(p.Stdout())
		within(t, 15*time.Second, "the conversation", func() {
			io.WriteString(p.Stdin(), "ping "+token+"\n")
			if got, err := out.ReadString('\n'); got != "ping [TOKEN]\n" || err != nil {
				t.Errorf("answer = %q, %v", got, err)
			}
			p.Close()
		})
		noLeak(t, before)
	})
	t.Run("the context's end", func(t *testing.T) {
		var c filterCalls
		o, _, _ := secretServer(t, []string{"sh", "-c", `echo "err $TOKEN" >&2; exec sleep 1000`}, c.filter)
		before := runtime.NumGoroutine()
		ctx, cancel := context.WithCancel(context.Background())
		var stderr syncBuffer
		p, err := o.StartServer(ctx, "s", &stderr)
		if err != nil {
			t.Fatal(err)
		}
		go io.Copy(io.Discard, p.Stdout())
		time.Sleep(200 * time.Millisecond)
		cancel()
		within(t, 15*time.Second, "the end of the context", func() {
			if _, err := p.Wait(); !errors.Is(err, context.Canceled) {
				t.Errorf("Wait = %v, want context.Canceled", err)
			}
		})
		if got := stderr.String(); got != "err [TOKEN]\n" {
			t.Errorf("standard error = %q", got)
		}
		p.Close()
		noLeak(t, before)
	})
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("the writer fails") }

// Each filter call is given a copy of the values of its own: one call
// changing its map changes nothing the other is told.
func TestEachFilterCallHasItsOwnValues(t *testing.T) {
	var mu sync.Mutex
	var seen []map[string]string
	filter := func(_ string, secrets map[string]string, r io.Reader) io.Reader {
		mu.Lock()
		seen = append(seen, secrets)
		mu.Unlock()
		f := &replacer{r: bufio.NewReader(r), values: maps.Clone(secrets)}
		secrets["TOKEN"] = "changed"
		return f
	}
	o, _, _ := secretServer(t, []string{"sh", "-c", `echo "err $TOKEN" >&2; echo "out $TOKEN"`}, filter)
	var stderr syncBuffer
	p, err := o.StartServer(context.Background(), "s", &stderr)
	if err != nil {
		t.Fatal(err)
	}
	within(t, 10*time.Second, "the output", func() {
		io.ReadAll(p.Stdout())
		p.Close()
	})
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("the filter was called %d times, want 2", len(seen))
	}
	// Both calls changed their map; had they shared one, the second
	// would have been told "changed".
	if got := stderr.String(); got != "err [TOKEN]\n" {
		t.Errorf("standard error = %q", got)
	}
}
