package agentworkspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/ChristopherDavenport/agentworkspace/internal/lifetime"
)

// grace is how long a process's end waits at each step: for the
// process to exit after its input closes, after SIGINT and after
// SIGTERM, before it is killed. workspacetest checks against the same
// value.
const grace = lifetime.Grace

// ladder is how Local ends a process and what it started, the one way
// for Exec, a started process's context and timeout, Process.Close and
// the workspace's Close: SIGINT, the interrupt a terminal's user sends,
// which lets a program that buffers its output write it; then SIGTERM,
// which a job a non-interactive shell put in the background does not
// ignore, as it does SIGINT, and which the MCP stdio transport's
// shutdown names; then SIGKILL. Each but the last is a grace apart.
var ladder = [...]syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}

// group is a process Local started as the leader of a process group of
// its own, and what it started: the group's members, and the
// descendants that left it (setsid, setpgid, a shell's job control)
// that a walk of the process table finds. A descendant is found while
// its parent lives: one whose parent exited before any signal was sent
// has been given to another parent, and is not (see Reach).
type group struct {
	pgid int
	// mu is held while a signal is sent and while reaped is set, so no
	// signal goes to the group's ID once its leader is reaped.
	mu     sync.Mutex
	reaped bool
	found  map[int]uint64 // descendants out of the group found so far, by pid, with their start time
}

// procInfo is what a walk needs of a process.
type procInfo struct {
	ppid, pgrp int
	start      uint64 // since boot: a pid reused is another start
	zombie     bool
}

// signal sends sig to the group and to every descendant that left it,
// while the group's leader is not yet reaped, and is os.ErrProcessDone
// after.
func (g *group) signal(sig syscall.Signal) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reaped {
		return os.ErrProcessDone
	}
	if sig == syscall.SIGKILL {
		g.kill(true)
		return nil
	}
	g.descend(sig, true)
	return syscall.Kill(-g.pgid, sig)
}

// reap records that the leader has been waited for, killing what is
// left of the group, and the descendants that left it, first when kill
// is set. Here, as the leader is reaped, the group's ID cannot yet name
// another group; later it could.
func (g *group) reap(kill bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if kill {
		g.kill(false)
	}
	g.reaped = true
}

// kill kills the group and every descendant out of it, stopping them
// first and walking again until a walk finds no more, so that none
// forks a child the kill would miss: a child whose parent is killed
// before it is found is given to another parent, and found no more.
func (g *group) kill(leader bool) {
	syscall.Kill(-g.pgid, syscall.SIGSTOP)
	for range 8 {
		if g.descend(syscall.SIGSTOP, leader) == 0 {
			break
		}
	}
	g.descend(syscall.SIGKILL, leader)
	syscall.Kill(-g.pgid, syscall.SIGKILL)
}

// descend sends sig to every descendant of the leader that left its
// group: a process whose parent is the leader (while leader says it is
// not reaped), a member of the group, a descendant found before, or one
// of these, and so on down. A descendant that leads its own group, as
// one that called setsid or setpgid does, is sent sig by that group, so
// what it starts there is too; any other alone. It never signals this
// process or its group. It returns how many descendants it found that
// earlier walks had not.
func (g *group) descend(sig syscall.Signal, leader bool) int {
	ps, ok := processes()
	if !ok {
		return 0
	}
	in := map[int]bool{}
	if leader {
		in[g.pgid] = true
	}
	for pid, p := range ps {
		if start, ok := g.found[pid]; p.pgrp == g.pgid || ok && start == p.start {
			in[pid] = true
		}
	}
	for grew := true; grew; {
		grew = false
		for pid, p := range ps {
			if !in[pid] && in[p.ppid] {
				in[pid], grew = true, true
			}
		}
	}
	self, selfGroup := os.Getpid(), syscall.Getpgrp()
	found := make(map[int]uint64, len(g.found))
	added := 0
	signalled := map[int]bool{}
	for pid := range in {
		p, ok := ps[pid]
		if !ok || p.pgrp == g.pgid || pid == self {
			continue // the group's own are sent sig by the group
		}
		if start, ok := g.found[pid]; !ok || start != p.start {
			added++
		}
		found[pid] = p.start
		if p.zombie {
			continue
		}
		target := pid
		if in[p.pgrp] && p.pgrp != selfGroup {
			target = -p.pgrp
		}
		if !signalled[target] {
			signalled[target] = true
			syscall.Kill(target, sig)
		}
	}
	g.found = found
	return added
}

// end climbs the ladder until exited, given how long to wait, reports
// the leader gone; the last step waits for nothing.
func (g *group) end(exited func(time.Duration) bool) {
	for i, sig := range ladder {
		g.signal(sig)
		if i == len(ladder)-1 || exited(grace) {
			return
		}
	}
}

// closedWithin reports whether ch is closed within d.
func closedWithin(ch <-chan struct{}, d time.Duration) bool {
	select {
	case <-ch:
		return true
	default:
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
		return true
	case <-t.C:
		return false
	}
}

// Start runs a command in its own process group, with pipes for its
// standard input and output, and returns at once. Its standard error
// goes to Command.Stream, or nowhere. The process, and everything it
// started, is ended by Process.Close, by the end of ctx, by
// Command.Timeout, or by the workspace's Close, each with Local's
// ladder (SIGINT, SIGTERM, SIGKILL, a grace apart), which Close
// begins by closing the process's input; when it exits by itself,
// anything left in its group is killed.
func (l *Local) Start(ctx context.Context, c Command) (Process, error) {
	if c.Stdin != nil {
		return nil, errors.New("agentworkspace: Start takes no Command.Stdin; write to Process.Stdin")
	}
	dir, env, prog, err := l.prepare(c)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cancel := context.CancelFunc(func() {})
	if c.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
	}
	// os.Pipe rather than exec's StdinPipe and StdoutPipe: exec closes
	// those when the process exits, which would cut a reader short of
	// output still in the pipe, and here something waits from the
	// start. The child holds the other ends directly.
	inR, inW, err := os.Pipe()
	if err != nil {
		cancel()
		return nil, err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		inR.Close()
		inW.Close()
		cancel()
		return nil, err
	}
	// Standard error is a pipe this copies to Stream, so that exec
	// starts no copying of its own and the wait returns as the process
	// exits, while its group can still be told apart from a later one.
	var errR, errW *os.File
	if c.Stream != nil {
		if errR, errW, err = os.Pipe(); err != nil {
			inR.Close()
			inW.Close()
			outR.Close()
			outW.Close()
			cancel()
			return nil, err
		}
	}
	cmd := exec.Command(prog, c.Args[1:]...)
	cmd.Args[0] = c.Args[0]
	cmd.Dir, cmd.Env = dir, env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin, cmd.Stdout = inR, outW
	if errW != nil {
		cmd.Stderr = errW
	}

	l.mu.Lock()
	if l.closed {
		err = fs.ErrClosed
	} else {
		err = cmd.Start()
	}
	inR.Close()
	outW.Close()
	if errW != nil {
		errW.Close()
	}
	if err != nil {
		l.mu.Unlock()
		inW.Close()
		outR.Close()
		if errR != nil {
			errR.Close()
		}
		cancel()
		return nil, err
	}
	p := &localProcess{
		owner:   l,
		cmd:     cmd,
		group:   &group{pgid: cmd.Process.Pid},
		stdin:   inW,
		stdout:  outR,
		done:    make(chan struct{}),
		closing: make(chan struct{}),
		ended:   make(chan struct{}),
	}
	l.procs[p] = struct{}{}
	l.mu.Unlock()
	var copied chan struct{}
	if errR != nil {
		copied = make(chan struct{})
		go func() {
			defer close(copied)
			if _, err := io.Copy(c.Stream, errR); err != nil {
				// Keep draining, or the process blocks on a full pipe.
				io.Copy(io.Discard, errR)
			}
		}()
	}
	go p.wait(errR, copied)
	go p.watch(ctx, cancel)
	return p, nil
}

// localProcess is a process Local started.
type localProcess struct {
	owner  *Local
	cmd    *exec.Cmd
	group  *group
	stdin  *os.File // the write end
	stdout *os.File // the read end

	mu     sync.Mutex
	ending error // the context's error, once the context began ending it
	status int
	err    error

	done      chan struct{} // closed once the process has exited and status and err are set
	closing   chan struct{} // closed by Close
	closeOnce sync.Once
	ended     chan struct{} // closed once watch has ended the process group
}

func (p *localProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *localProcess) Stdout() io.ReadCloser { return p.stdout }

func (p *localProcess) Wait() (int, error) {
	<-p.done
	return p.status, p.err
}

func (p *localProcess) Signal(sig os.Signal) error {
	s, ok := sig.(syscall.Signal)
	if !ok {
		return fmt.Errorf("agentworkspace: cannot send %v", sig)
	}
	return p.group.signal(s)
}

func (p *localProcess) Close() error {
	p.closeOnce.Do(func() { close(p.closing) })
	<-p.ended
	p.stdin.Close()
	p.stdout.Close()
	p.owner.mu.Lock()
	delete(p.owner.procs, p)
	p.owner.mu.Unlock()
	return nil
}

// wait reaps the process, kills what it left running, and records how
// it ended. errR, when not nil, is the standard error pipe being copied
// until copied is closed.
func (p *localProcess) wait(errR *os.File, copied chan struct{}) {
	err := p.cmd.Wait()
	// Anything the process started goes with it.
	p.group.reap(true)
	if errR != nil {
		// What the group wrote is read to the end, unless something
		// outside the group holds the pipe open.
		t := time.NewTimer(waitDelay)
		select {
		case <-copied:
		case <-t.C:
			errR.Close()
			<-copied
		}
		t.Stop()
		errR.Close()
	}
	status := -1
	if p.cmd.ProcessState != nil {
		status = p.cmd.ProcessState.ExitCode()
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		// A status is not a failure to wait.
		err = nil
	}
	p.mu.Lock()
	if p.ending != nil {
		err = p.ending
	}
	p.status, p.err = status, err
	p.mu.Unlock()
	close(p.done)
}

// watch ends the process when the context ends or Close is called,
// whichever is first.
func (p *localProcess) watch(ctx context.Context, cancel context.CancelFunc) {
	defer close(p.ended)
	defer cancel()
	select {
	case <-ctx.Done():
		p.mu.Lock()
		select {
		case <-p.done:
		default:
			p.ending = ctx.Err()
		}
		p.mu.Unlock()
		p.end(false)
	case <-p.closing:
		p.end(true)
	}
}

// end ends the process group, unless the process has exited, when
// wait already has: politely, by closing the process's input and
// waiting a moment, when Close asked; then with the ladder.
func (p *localProcess) end(polite bool) {
	if polite {
		p.stdin.Close()
		if p.exited(grace) {
			return
		}
	}
	p.group.end(p.exited)
	<-p.done
}

// exited reports whether the process exits within d.
func (p *localProcess) exited(d time.Duration) bool { return closedWithin(p.done, d) }
