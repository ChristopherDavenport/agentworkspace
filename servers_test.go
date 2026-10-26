package agentworkspace

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestValidServerName(t *testing.T) {
	for name, want := range map[string]bool{
		"github":                              true,
		"rag-2":                               true,
		"a":                                   true,
		"0":                                   true,
		strings.Repeat("a", 32):               true,
		"":                                    false,
		strings.Repeat("a", 33):               false,
		"-rag":                                false,
		"rag-":                                false,
		"my_server":                           false,
		"GitHub":                              false,
		"rag.v2":                              false,
		"a b":                                 false,
		"mcp__x":                              false,
		"café":                                false,
		"ok-" + strings.Repeat("b", 28) + "c": true,
	} {
		if got := ValidServerName(name); got != want {
			t.Errorf("ValidServerName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestOfferRefusesABadList(t *testing.T) {
	l, _ := local(t, nil)
	cat := Command{Args: []string{"cat"}}
	for what, servers := range map[string][]OfferedServer{
		"a bad name":   {{Server: Server{Name: "My_Server"}, Command: cat}},
		"a name twice": {{Server: Server{Name: "a"}, Command: cat}, {Server: Server{Name: "a"}, Command: cat}},
		"no command":   {{Server: Server{Name: "a"}}},
	} {
		if _, err := Offer(l, servers); err == nil {
			t.Errorf("%s: Offer took it", what)
		}
	}
	if _, err := Offer(nil, nil); err == nil {
		t.Error("Offer of a nil workspace took it")
	}
}

// Offer copies the commands: a caller changing its slices afterwards
// changes nothing a start runs.
func TestOfferCopiesTheCommands(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	args := []string{"sh", "-c", `echo "$MARK"; exec cat`}
	env := []string{"MARK=offered"}
	o, err := Offer(l, []OfferedServer{{Server: Server{Name: "s"}, Command: Command{Args: args, Env: env}}})
	if err != nil {
		t.Fatal(err)
	}
	args[2], env[0] = "echo changed", "MARK=changed"
	p, err := o.StartServer(context.Background(), "s", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	within(t, 10*time.Second, "the first line", func() {
		if got, err := bufio.NewReader(p.Stdout()).ReadString('\n'); got != "offered\n" || err != nil {
			t.Errorf("first line = %q, %v; want the command as offered", got, err)
		}
	})
}

// syncBuffer is a bytes.Buffer safe for the stream's writer and the
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

// An offered server is listed, in name order, and started by name: its
// standard input and output are the conversation, its standard error
// goes where StartServer says, and its command runs in the workspace.
func TestAnOfferedServerStartsByName(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	o, err := Offer(l, []OfferedServer{
		{Server: Server{Name: "echo", Description: "says it back"}, Command: Command{
			Args: []string{"sh", "-c", `echo started >&2; exec cat`},
			// Stream is the caller's to set, through StartServer.
			Stream: io.Discard,
		}},
		{Server: Server{Name: "another"}, Command: Command{Args: []string{"cat"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ws Workspace = o
	if _, ok := ws.(Starter); !ok {
		t.Error("an Offering is not a Starter")
	}
	list, err := o.Servers(context.Background())
	if err != nil || len(list) != 2 || list[0] != (Server{Name: "another"}) || list[1] != (Server{Name: "echo", Description: "says it back"}) {
		t.Fatalf("Servers = %+v, %v", list, err)
	}

	var stderr syncBuffer
	p, err := o.StartServer(context.Background(), "echo", &stderr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	out := bufio.NewReader(p.Stdout())
	within(t, 10*time.Second, "the conversation", func() {
		io.WriteString(p.Stdin(), "ping\n")
		if got, err := out.ReadString('\n'); got != "ping\n" || err != nil {
			t.Errorf("answer = %q, %v", got, err)
		}
		p.Close()
	})
	if !strings.Contains(stderr.String(), "started") {
		t.Errorf("standard error = %q, want the server's", stderr.String())
	}
}
