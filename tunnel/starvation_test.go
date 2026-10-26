package tunnel

import (
	"bufio"
	"context"
	"io/fs"
	"math"
	"strings"
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
	if room := MaxStreams - held; room < ReservedStreams {
		t.Errorf("the defaults hold %d of %d streams open, leaving %d", held, MaxStreams, room)
	}
	defaults := remote.Bounds{MaxSessions: DefaultSessions, MaxSessionsPerPrincipal: DefaultSessions, MaxProcesses: DefaultProcesses, MaxTransfers: DefaultTransfers}
	if err := fits(defaults); err != nil {
		t.Errorf("the defaults do not fit: %v", err)
	}
}

// Serve takes remote bounds that keep the streams the server can hold
// open within MaxStreams − ReservedStreams, however they are spread,
// and refuses any past it before it dials, so that raising one through
// WithHandlerOptions cannot give up the guarantee unnoticed. A bound of
// any size is refused, not overflowed.
func TestServeHoldsTheStreamsArithmetic(t *testing.T) {
	huge := math.MaxInt
	for _, c := range []struct {
		name string
		opts []remote.Option
		fits bool
	}{
		{"the defaults", nil, true},
		{"one more session, to the bound", []remote.Option{remote.WithMaxSessions(5), remote.WithMaxSessionsPerPrincipal(5)}, true},
		{"more transfers, to the bound", []remote.Option{remote.WithMaxTransfers(256)}, true},
		{"twice the processes in half the sessions", []remote.Option{remote.WithMaxProcesses(64), remote.WithMaxSessions(2)}, true},
		{"more sessions, fewer processes", []remote.Option{remote.WithMaxSessions(8), remote.WithMaxSessionsPerPrincipal(8), remote.WithMaxProcesses(16)}, true},
		{"many sessions, the principal's few", []remote.Option{remote.WithMaxSessions(1000)}, true},
		{"fewer of everything", []remote.Option{remote.WithMaxSessions(1), remote.WithMaxProcesses(1), remote.WithMaxTransfers(1)}, true},
		{"more processes", []remote.Option{remote.WithMaxProcesses(41)}, false},
		{"one transfer past", []remote.Option{remote.WithMaxTransfers(257)}, false},
		{"more sessions", []remote.Option{remote.WithMaxSessions(6), remote.WithMaxSessionsPerPrincipal(6)}, false},
		{"remote's defaults", []remote.Option{
			remote.WithMaxSessions(remote.DefaultMaxSessions), remote.WithMaxSessionsPerPrincipal(remote.DefaultMaxSessionsPerPrincipal),
			remote.WithMaxProcesses(remote.DefaultMaxProcesses), remote.WithMaxTransfers(remote.DefaultMaxTransfers),
		}, false},
		{"huge processes", []remote.Option{remote.WithMaxProcesses(huge)}, false},
		{"huge transfers", []remote.Option{remote.WithMaxTransfers(huge)}, false},
		{"huge sessions", []remote.Option{remote.WithMaxSessions(huge), remote.WithMaxSessionsPerPrincipal(huge)}, false},
		{"huge everything", []remote.Option{remote.WithMaxSessions(huge), remote.WithMaxSessionsPerPrincipal(huge), remote.WithMaxProcesses(huge), remote.WithMaxTransfers(huge)}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			l := local(t, t.TempDir(), pathEnv())
			if !c.fits {
				var err error
				within(t, 5*time.Second, "Serve", func() {
					err = Serve(context.Background(), "ws://127.0.0.1:1/", l, WithScopes(remote.ScopeRead), WithHandlerOptions(c.opts...))
				})
				if err == nil || !strings.Contains(err.Error(), "MaxStreams") {
					t.Errorf("Serve: %v, want a refusal of the bounds", err)
				}
				return
			}
			r := newRig(t)
			r.serve(t, l, WithScopes(remote.ScopeRead), WithHandlerOptions(c.opts...), bearer(tokA), fast)
			if _, err := fs.ReadDir(r.workspace(t, "a").FS(), "."); err != nil {
				t.Errorf("through the tunnel: %v", err)
			}
		})
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
		// The processes raised, the sessions lowered to keep them within
		// the arithmetic, which Serve holds: 2 × 78 × 4 + 128 is 752.
		WithHandlerOptions(remote.WithLease(lease), remote.WithMaxProcesses(hogs+8), remote.WithMaxSessions(2)))
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
