package agentworkspace

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

// beats ignores SIGINT and SIGTERM and beats into the file beat until
// it is killed, or for 30 seconds.
const beats = `trap '' INT TERM HUP
i=0
while [ $i -lt 300 ]; do
	i=$((i+1))
	echo $i > beat.tmp && mv beat.tmp beat
	sleep 0.1
done
`

// stillBeats reports whether beats, run in dir, still beats.
func stillBeats(dir string) bool {
	name := filepath.Join(dir, "beat")
	first, _ := os.ReadFile(name)
	for range 10 {
		time.Sleep(100 * time.Millisecond)
		if b, _ := os.ReadFile(name); len(b) > 0 && !bytes.Equal(b, first) {
			return true
		}
	}
	return false
}

// A member of the group that leaves it after a walk has seen it (here,
// by calling setsid on SIGTERM), and whose parent then exits, is still
// ended: what a walk saw of the group is remembered, not only what it
// saw out of it.
func TestAMemberThatLeavesTheGroupLateIsEnded(t *testing.T) {
	if reachNow() != ReachDescendants {
		t.Skip("no walk here")
	}
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("no setsid here")
	}
	// The leader ignores SIGINT and leaves a moment after SIGTERM; its
	// child, a member of the group, leaves the group on SIGTERM and
	// beats on.
	const leader = `trap '' INT; trap 'sleep 0.3; exit 0' TERM; sh child.sh </dev/null >/dev/null 2>&1 & while :; do sleep 0.05; done`
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, l *Local)
	}{
		{"Exec's timeout", func(t *testing.T, l *Local) {
			l.Exec(context.Background(), Command{Args: []string{"sh", "-c", leader}, Timeout: 500 * time.Millisecond})
		}},
		{"Close", func(t *testing.T, l *Local) {
			p, err := l.Start(context.Background(), Command{Args: []string{"sh", "-c", leader}})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(300 * time.Millisecond)
			p.Close()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, dir := local(t, []string{"PATH=" + os.Getenv("PATH")})
			os.WriteFile(filepath.Join(dir, "beat.sh"), []byte(beats), 0o644)
			os.WriteFile(filepath.Join(dir, "child.sh"), []byte(`trap 'exec setsid sh beat.sh' TERM; while :; do sleep 0.05; done`), 0o644)
			tc.run(t, l)
			if stillBeats(dir) {
				t.Error("a member that left the group after the walk outlived the end")
			}
		})
	}
}

// With no /proc of this process's to walk, Local says it reaches the
// group alone, and does not walk.
func TestReachIsTheGroupWithoutProc(t *testing.T) {
	saved := procRoot
	t.Cleanup(func() { procRoot = saved })
	procRoot = t.TempDir() // an empty /proc, as a tmpfs over it gives
	l, _ := local(t, nil)
	if r := l.Reach(); r != ReachGroup {
		t.Errorf("Reach with an empty /proc = %q, want group", r)
	}
	if _, ok := processes(); ok {
		t.Error("processes() walked an empty /proc")
	}
}

// A local Descriptor always names a host: WithHost("") is refused, and
// an empty system host name falls back to a stable name of its own.
func TestALocalHostIsNeverEmpty(t *testing.T) {
	if _, err := NewLocal(t.TempDir(), nil, WithHost("")); err == nil {
		t.Error("WithHost(\"\") was taken")
	}
	a, b := hostOr(""), hostOr("")
	if a == "" || a != b {
		t.Errorf("the fallback host = %q, then %q; want one stable, non-empty name", a, b)
	}
	if got := hostOr("build-7"); got != "build-7" {
		t.Errorf("hostOr(build-7) = %q", got)
	}
}

// A command that exited before the context ended is not interrupted,
// though a child it left holds its output open past its exit: its
// status is what it exited with.
func TestAnExitBeforeTheContextEndsKeepsItsStatus(t *testing.T) {
	l, _ := local(t, []string{"PATH=" + os.Getenv("PATH")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(500*time.Millisecond, cancel)
	out, err := l.Exec(ctx, Command{Args: []string{"sh", "-c", "echo hi; sleep 5 & exit 3"}})
	if err != nil || out == nil || out.ExitCode != 3 || out.Interrupted || string(out.Stdout) != "hi\n" {
		t.Errorf("Exec = %+v, %v; want status 3, not interrupted", out, err)
	}
}

// A stop that reached a process which took a descendant's ID, between
// the walk's read of the table and the signal, is undone: the process
// is continued, and the kill passes it by. The table is a fake one, as
// a real ID cannot be made to be reused on cue.
func TestAStopThatReachedAReusedIDIsUndone(t *testing.T) {
	table := map[int]procInfo{
		100: {ppid: 1, pgrp: 100, start: 1},   // the leader
		200: {ppid: 100, pgrp: 300, start: 5}, // a descendant that left its group
	}
	names := map[syscall.Signal]string{syscall.SIGSTOP: "STOP", syscall.SIGCONT: "CONT", syscall.SIGKILL: "KILL"}
	var sent []string
	savedTable, savedStart, savedSend := procTable, procStart, sendSignal
	t.Cleanup(func() { procTable, procStart, sendSignal = savedTable, savedStart, savedSend })
	procTable = func() (map[int]procInfo, bool) { return maps.Clone(table), true }
	procStart = func(pid int) (uint64, bool) { p, ok := table[pid]; return p.start, ok }
	sendSignal = func(pid int, sig syscall.Signal) error {
		sent = append(sent, fmt.Sprintf("%s %d", names[sig], pid))
		if pid == 200 && sig == syscall.SIGSTOP {
			// 200 exited as the walk read it, and another process,
			// started later, has its ID.
			table[200] = procInfo{ppid: 1, pgrp: 200, start: 9}
		}
		return nil
	}
	g := &group{pgid: 100, start: 1}
	g.kill(true)
	if want := []string{"STOP -100", "STOP 200", "CONT 200", "KILL -100"}; !slices.Equal(sent, want) {
		t.Errorf("signals = %q, want %q", sent, want)
	}
}
