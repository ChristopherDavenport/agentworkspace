package mcpworkspace

// Exec over the process methods: one command run to its end, as
// agentworkspace.Workspace's Exec runs it, composed of a start, the
// pipes, a cancel and a wait.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os/exec"
	"sync"
	"time"

	"github.com/ChristopherDavenport/agentworkspace"
	"github.com/ChristopherDavenport/agentworkspace/internal/lifetime"
)

// outputDelay is how long Exec reads the output on after the command
// has exited, while something it left holds it open: Local's.
const outputDelay = lifetime.Grace

// Exec runs cmd to completion in the workspace at the other end of c,
// over the process methods, and holds to agentworkspace.Workspace's
// Exec (RFC 0001 §6): cmd.Stdin is written to the process's input,
// which is then closed; an exit status other than zero is an Output,
// not an error; cmd.Timeout, kept by the server, is an Output with
// ExitCode -1 and TimedOut; and the end of ctx is a cancel (the ladder
// from INT), after which the output is read to its end, so Exec returns
// what the command wrote until then, with ExitCode -1 and Interrupted,
// beside ctx.Err(). A ctx that has ended before the start runs nothing.
//
// Without a Stream, the Output holds standard output and standard error
// in full, in this process's memory, as Local's does. With one, both go
// to it as they arrive and the Output holds neither; the two travel
// apart, so the order between them is the order they arrive in, while
// each keeps its own. A Stream whose write fails ends the command, and
// Exec returns that error. The output is read for at most two seconds
// past the command's exit, as Local reads it, while something the
// command left holds it open.
//
// A process the server ended for another reason than this cancel (its
// session's end, its own close) is an Output with Interrupted beside an
// error that is fs.ErrClosed.
func Exec(ctx context.Context, c Conn, cmd agentworkspace.Command) (*agentworkspace.Output, error) {
	if len(cmd.Args) == 0 {
		return nil, errors.New("mcpworkspace: no command")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A sink that fails closes failed, which ends the command.
	failed := make(chan struct{})
	var failOnce sync.Once
	fail := func() { failOnce.Do(func() { close(failed) }) }
	var stdout, stderr bytes.Buffer
	var out, errOut *sink
	if cmd.Stream != nil {
		out = &sink{w: cmd.Stream, failed: fail}
		errOut = out
	} else {
		out, errOut = &sink{w: &stdout, failed: fail}, &sink{w: &stderr, failed: fail}
	}
	start := cmd
	start.Stdin, start.Stream = nil, errOut
	// Its own context: the end of ctx is a cancel, not Start's close.
	started, err := Start(context.WithoutCancel(ctx), c, start)
	if err != nil {
		return nil, err
	}
	p := started.(*process)
	defer p.Close()
	var cancelOnce sync.Once
	interrupt := func() { cancelOnce.Do(func() { p.interrupt() }) }
	stop := context.AfterFunc(ctx, interrupt)
	defer stop()
	returned := make(chan struct{})
	defer close(returned)
	go func() {
		select {
		case <-failed:
			interrupt()
		case <-returned:
		}
	}()
	go func() {
		if len(cmd.Stdin) > 0 {
			p.Stdin().Write(cmd.Stdin)
		}
		p.Stdin().Close()
	}()
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		io.Copy(out, p.Stdout())
	}()
	status, werr := p.Wait()
	cut := false
	t := time.NewTimer(outputDelay)
	select {
	case <-copied:
	case <-t.C:
		cut = true
	}
	t.Stop()
	// The close ends a read still waiting, and writes the standard error
	// no read took to its sink.
	p.Close()
	<-copied
	if err := out.failure(); err != nil {
		return nil, err
	}
	if err := errOut.failure(); err != nil {
		return nil, err
	}
	o := &agentworkspace.Output{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), ExitCode: status}
	switch {
	case errors.Is(werr, context.DeadlineExceeded):
		o.ExitCode, o.TimedOut = -1, true
		return o, nil
	case errors.Is(werr, context.Canceled) && ctx.Err() != nil:
		o.ExitCode, o.Interrupted = -1, true
		return o, ctx.Err()
	case errors.Is(werr, context.Canceled):
		o.ExitCode, o.Interrupted = -1, true
		return o, fmt.Errorf("exec %s: the server ended it: %w", cmd.Args[0], fs.ErrClosed)
	case werr != nil:
		return nil, werr
	case cut && status == 0:
		// As os/exec says it: the output was cut short.
		return nil, exec.ErrWaitDelay
	}
	return o, nil
}

// sink is where one of a command's outputs goes, the Stream or a
// buffer, shared by both outputs when it is the Stream. A write that
// fails is not tried again, and calls failed once, which ends the
// command; what comes after it is drained, so the process does not
// wait on a full pipe.
type sink struct {
	mu     sync.Mutex
	w      io.Writer
	err    error
	failed func()
}

func (s *sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return len(p), nil
	}
	_, err := s.w.Write(p)
	s.err = err
	s.mu.Unlock()
	if err != nil && s.failed != nil {
		s.failed()
	}
	return len(p), nil
}

func (s *sink) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
