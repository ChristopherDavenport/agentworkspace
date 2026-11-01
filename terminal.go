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
)

// startTerminal is Start for a command with a terminal: the command
// runs in a session of its own (so its group is its own too, and the
// ladder reaches it as Start's), with the slave as its standard input,
// output and error and its controlling terminal. cancel ends ctx's
// timeout, if it has one.
func (l *Local) startTerminal(ctx context.Context, cancel context.CancelFunc, c Command, dir string, env []string, prog string) (Process, error) {
	fail := func(err error) (Process, error) {
		cancel()
		return nil, err
	}
	master, slave, err := openTerminal(*c.Terminal)
	if errors.Is(err, errors.ErrUnsupported) {
		return fail(fmt.Errorf("agentworkspace: Local gives a terminal on Linux alone: %w", err))
	}
	if err != nil {
		return fail(err)
	}
	out, err := dupFile(master)
	if err != nil {
		master.Close()
		slave.Close()
		return fail(err)
	}
	cmd := exec.Command(prog, c.Args[1:]...)
	cmd.Args[0] = c.Args[0]
	cmd.Dir, cmd.Env = dir, env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	l.mu.Lock()
	if l.closed {
		err = fs.ErrClosed
	} else {
		err = cmd.Start()
	}
	slave.Close()
	if err != nil {
		l.mu.Unlock()
		master.Close()
		out.Close()
		return fail(err)
	}
	p := &localProcess{
		owner:   l,
		cmd:     cmd,
		group:   newGroup(cmd.Process.Pid),
		stdin:   &terminalIn{f: master},
		stdout:  terminalOut{out},
		term:    out, // open while the output is, past the input's close
		reaped:  make(chan struct{}),
		done:    make(chan struct{}),
		closing: make(chan struct{}),
		ended:   make(chan struct{}),
	}
	l.procs[p] = struct{}{}
	l.mu.Unlock()
	go p.wait(nil, nil)
	go p.watch(ctx, cancel)
	return p, nil
}

// Resize changes the size of the process's terminal, and is
// errors.ErrUnsupported for one started without a terminal.
func (p *localProcess) Resize(size TerminalSize) error {
	if p.term == nil {
		return fmt.Errorf("agentworkspace: a process with no terminal: %w", errors.ErrUnsupported)
	}
	return setTerminalSize(p.term, size)
}

// terminalIn is a terminal's input: Close sends the terminal's end of
// file (^D) before it closes this descriptor, the output's being apart.
type terminalIn struct {
	f    *os.File
	once sync.Once
	err  error
}

func (t *terminalIn) Write(p []byte) (int, error) { return t.f.Write(p) }

func (t *terminalIn) Close() error {
	t.once.Do(func() {
		t.f.Write([]byte{4})
		t.err = t.f.Close()
	})
	return t.err
}

// terminalOut is a terminal's output: the master's EIO once every
// holder of the slave has closed it is end of file.
type terminalOut struct{ f *os.File }

func (t terminalOut) Read(p []byte) (int, error) {
	n, err := t.f.Read(p)
	if errors.Is(err, syscall.EIO) {
		err = io.EOF
	}
	return n, err
}

func (t terminalOut) Close() error { return t.f.Close() }
