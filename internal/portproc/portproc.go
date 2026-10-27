// Package portproc gives a stream to a port in a workspace the shape of
// a started process, so that a wire carries it on its process routes
// and methods (its input, its output, its close) rather than on a
// carrier of its own: a connect is a start whose process is the
// stream.
package portproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ChristopherDavenport/agentworkspace"
)

// Process is c as an agentworkspace.Process: Stdin writes to the port
// and its Close is c's CloseWrite, Stdout reads from it, Close ends the
// stream, and Wait returns once it has, as the end of ctx does: status
// 0 for a close, -1 and ctx's error for ctx. Signal is
// errors.ErrUnsupported.
func Process(ctx context.Context, c agentworkspace.PortConn) agentworkspace.Process {
	p := &process{c: c, done: make(chan struct{})}
	p.stop = context.AfterFunc(ctx, func() { p.end(-1, ctx.Err()) })
	return p
}

type process struct {
	c    agentworkspace.PortConn
	stop func() bool
	once sync.Once
	done chan struct{}

	status int
	err    error
}

func (p *process) Stdin() io.WriteCloser { return input{p.c} }
func (p *process) Stdout() io.ReadCloser { return output{p.c} }

func (p *process) Wait() (int, error) {
	<-p.done
	return p.status, p.err
}

func (p *process) Signal(sig os.Signal) error {
	select {
	case <-p.done:
		return os.ErrProcessDone
	default:
	}
	return fmt.Errorf("a stream to a port takes no signal %v: %w", sig, errors.ErrUnsupported)
}

func (p *process) Close() error {
	p.stop()
	p.end(0, nil)
	return nil
}

func (p *process) end(status int, err error) {
	p.once.Do(func() {
		p.c.Close()
		p.status, p.err = status, err
		close(p.done)
	})
}

// input is the stream's sending half: its close is a half-close.
type input struct{ c agentworkspace.PortConn }

func (i input) Write(b []byte) (int, error) { return i.c.Write(b) }
func (i input) Close() error                { return i.c.CloseWrite() }

// output is the stream's receiving half: its close leaves the stream
// to the process's.
type output struct{ c agentworkspace.PortConn }

func (o output) Read(b []byte) (int, error) { return o.c.Read(b) }
func (o output) Close() error               { return nil }

// Conn is p, a process a wire's client started by a connect, as the
// stream it carries: Write is its input's, CloseWrite its input's
// close, Read its output's and Close the process's.
func Conn(p agentworkspace.Process) agentworkspace.PortConn { return conn{p} }

type conn struct{ p agentworkspace.Process }

func (c conn) Read(b []byte) (int, error)  { return c.p.Stdout().Read(b) }
func (c conn) Write(b []byte) (int, error) { return c.p.Stdin().Write(b) }
func (c conn) CloseWrite() error           { return c.p.Stdin().Close() }
func (c conn) Close() error                { return c.p.Close() }
