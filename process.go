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
)

// grace is how long a process's end waits at each step: for the
// process to exit after its input closes, and again after SIGTERM,
// before it is killed.
const grace = 2 * time.Second

// Start runs a command in its own process group, with pipes for its
// standard input and output, and returns at once. Its standard error
// goes to Command.Stream, or nowhere. The process, and everything it
// started, is ended by Process.Close, by the end of ctx, by
// Command.Timeout, or by the workspace's Close; when it exits by
// itself, anything left in its group is killed.
func (l *Local) Start(ctx context.Context, c Command) (Process, error) {
	if c.Stdin != nil {
		return nil, errors.New("agentworkspace: Start takes no Command.Stdin; write to Process.Stdin")
	}
	dir, env, err := l.prepare(c)
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
	cmd := exec.Command(c.Args[0], c.Args[1:]...)
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
		pgid:    cmd.Process.Pid,
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
	pgid   int
	stdin  *os.File // the write end
	stdout *os.File // the read end

	// reap is held across the reaping's group kill and while a signal is
	// sent, so no signal goes to the group's ID after reaped is set.
	reap   sync.Mutex
	reaped bool

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
	return p.signal(s)
}

// signal sends sig to the process group while its leader is not yet
// reaped, and is os.ErrProcessDone after.
func (p *localProcess) signal(sig syscall.Signal) error {
	p.reap.Lock()
	defer p.reap.Unlock()
	if p.reaped {
		return os.ErrProcessDone
	}
	return syscall.Kill(-p.pgid, sig)
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
	// Anything the process started goes with it. Here, as it is reaped,
	// the group's ID cannot yet name another group; later it could.
	p.reap.Lock()
	syscall.Kill(-p.pgid, syscall.SIGKILL)
	p.reaped = true
	p.reap.Unlock()
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
// waiting a moment, when Close asked; then with SIGTERM, then SIGKILL.
// Signals go to the group only while its leader has not been reaped,
// so they cannot reach a later group that reused its ID.
func (p *localProcess) end(polite bool) {
	if polite {
		p.stdin.Close()
		if p.exited(grace) {
			return
		}
	}
	p.signal(syscall.SIGTERM)
	if p.exited(grace) {
		return
	}
	p.signal(syscall.SIGKILL)
	<-p.done
}

// exited reports whether the process exits within d.
func (p *localProcess) exited(d time.Duration) bool {
	select {
	case <-p.done:
		return true
	default:
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-p.done:
		return true
	case <-t.C:
		return false
	}
}
