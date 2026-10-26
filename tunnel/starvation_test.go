package tunnel

import (
	"bufio"
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/remote"
)

// The arithmetic the bounds rest on holds: the connection's window
// never runs out before a stream's, and the streams Serve's defaults
// let the server hold open leave room under MaxStreams.
func TestTheBoundsArithmetic(t *testing.T) {
	if ConnWindow < MaxStreams*StreamWindow {
		t.Errorf("ConnWindow %d is under MaxStreams × StreamWindow %d", ConnWindow, MaxStreams*StreamWindow)
	}
	held := DefaultSessions*DefaultProcesses*4 + DefaultTransfers
	if room := MaxStreams - held; room < MaxStreams/4 {
		t.Errorf("the defaults hold %d of %d streams open, leaving %d", held, MaxStreams, room)
	}
}

// Output nobody reads stalls only its own stream: with more processes'
// output left unread than ConnWindow/StreamWindow used to allow, a
// process nobody misused outlives several leases, and a renewal, a
// Signal, a new Start, its Wait and an Exec all get through.
func TestUnreadOutputStarvesNothing(t *testing.T) {
	const (
		lease = time.Second
		hogs  = 70
	)
	r := newRig(t)
	r.serve(t, local(t, t.TempDir(), pathEnv()), WithScopes(allScopes...), bearer(tokA), fast,
		WithHandlerOptions(remote.WithLease(lease), remote.WithMaxProcesses(hogs+8)))
	s := r.workspace(t, "a").(agentworkspace.Starter)
	victim, _, pid := pidOf(t, s, "echo $$; exec sleep 60")
	for i := range hogs {
		// Far more output than a stream's window, of which the first
		// line alone is read. The client's Close ends them, together.
		p, err := s.Start(context.Background(), agentworkspace.Command{Args: []string{"sh", "-c", "echo x; head -c 1200000 /dev/zero; exec sleep 60"}})
		if err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
		within(t, 10*time.Second, "a hog's first line", func() {
			if _, err := bufio.NewReader(p.Stdout()).ReadString('\n'); err != nil {
				t.Errorf("hog %d: %v", i, err)
			}
		})
	}
	ctx := context.Background()
	for range 4 {
		time.Sleep(lease)
		if !alive(pid) {
			t.Fatal("the victim was ended: a renewal did not get through")
		}
		within(t, 10*time.Second, "a Start and its Wait", func() {
			p, err := s.Start(ctx, agentworkspace.Command{Args: []string{"true"}})
			if err != nil {
				t.Errorf("a Start: %v", err)
				return
			}
			defer p.Close()
			if status, err := p.Wait(); status != 0 || err != nil {
				t.Errorf("its Wait = %d, %v", status, err)
			}
		})
		within(t, 10*time.Second, "an Exec", func() {
			if _, err := s.Exec(ctx, agentworkspace.Command{Args: []string{"true"}}); err != nil {
				t.Errorf("an Exec: %v", err)
			}
		})
	}
	if err := victim.Signal(syscall.SIGCONT); err != nil {
		t.Errorf("a Signal to the victim: %v", err)
	}
}
