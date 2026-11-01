package agentworkspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"strconv"
	"sync"
	"syscall"
)

// PortConn is one byte stream to a TCP port in a workspace: what is
// written is sent to the port, and what the port sends is read.
type PortConn interface {
	io.ReadWriteCloser
	// CloseWrite ends what is sent, as a TCP half-close does: the port
	// reads end of file, and this side may still read.
	CloseWrite() error
}

// Connector is a Workspace that can open a byte stream to a TCP port on
// its loopback, as its own processes reach it: a server a command
// started there, reached from the program's side. Connecting is as
// much power as running a command, and a wire grants it with exec.
type Connector interface {
	Workspace
	// Connect opens one stream to port on the workspace's loopback.
	// ctx bounds the connecting alone; the stream lives until its
	// Close, or the workspace's.
	Connect(ctx context.Context, port int) (PortConn, error)
}

// Connect opens a stream to port in ws if ws is a Connector, and
// otherwise returns an error that errors.Is errors.ErrUnsupported. A
// port outside 1 to 65535 is fs.ErrInvalid.
func Connect(ctx context.Context, ws Workspace, port int) (PortConn, error) {
	if err := validPort(port); err != nil {
		return nil, err
	}
	c, ok := ws.(Connector)
	if !ok {
		return nil, fmt.Errorf("agentworkspace: a %s workspace cannot connect to a port: %w", ws.Descriptor().Kind, errors.ErrUnsupported)
	}
	return c.Connect(ctx, port)
}

func validPort(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("agentworkspace: port %d: want 1 to 65535: %w", port, fs.ErrInvalid)
	}
	return nil
}

// Connect opens a stream to port on this machine's loopback, which is
// the workspace's: 127.0.0.1, and ::1 when nothing listens there. The
// workspace's Close ends it.
func (l *Local) Connect(ctx context.Context, port int) (PortConn, error) {
	if err := validPort(port); err != nil {
		return nil, err
	}
	var d net.Dialer
	p := strconv.Itoa(port)
	c, err := d.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", p))
	if errors.Is(err, syscall.ECONNREFUSED) {
		if c6, err6 := d.DialContext(ctx, "tcp", net.JoinHostPort("::1", p)); err6 == nil {
			c, err = c6, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("agentworkspace: connect to port %d: %w", port, err)
	}
	lc := &localConn{TCPConn: c.(*net.TCPConn), owner: l}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		c.Close()
		return nil, fs.ErrClosed
	}
	l.conns[lc] = struct{}{}
	return lc, nil
}

// localConn is a stream Local opened, which its Close ends.
type localConn struct {
	*net.TCPConn
	owner *Local
}

func (c *localConn) Close() error {
	c.owner.mu.Lock()
	delete(c.owner.conns, c)
	c.owner.mu.Unlock()
	return c.TCPConn.Close()
}

// Forward listens on this machine's loopback alone (127.0.0.1, on a
// port the system picks) and, for each connection it accepts, opens one
// stream to port in ws (Connect) and copies both ways until either
// side ends. It returns the listener, whose Addr is where to connect;
// closing it, or the end of ctx, stops it and ends every stream it
// opened. It is a binding helper over Connect: a workspace that is no
// Connector is errors.ErrUnsupported.
func Forward(ctx context.Context, ws Workspace, port int) (net.Listener, error) {
	if err := validPort(port); err != nil {
		return nil, err
	}
	if _, ok := ws.(Connector); !ok {
		return nil, fmt.Errorf("agentworkspace: a %s workspace cannot connect to a port: %w", ws.Descriptor().Kind, errors.ErrUnsupported)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f := &forwarder{Listener: ln, conns: map[io.Closer]struct{}{}}
	stop := context.AfterFunc(ctx, func() { f.Close() })
	go func() {
		defer stop()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(ctx, ws, port, c)
		}
	}()
	return f, nil
}

// forwarder is Forward's listener: its Close ends what it opened too.
type forwarder struct {
	net.Listener
	mu     sync.Mutex
	closed bool
	conns  map[io.Closer]struct{}
}

func (f *forwarder) Close() error {
	err := f.Listener.Close()
	f.mu.Lock()
	f.closed = true
	for c := range f.conns {
		c.Close()
	}
	clear(f.conns)
	f.mu.Unlock()
	return err
}

func (f *forwarder) track(c io.Closer) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		c.Close()
		return false
	}
	f.conns[c] = struct{}{}
	return true
}

func (f *forwarder) untrack(c io.Closer) {
	f.mu.Lock()
	delete(f.conns, c)
	f.mu.Unlock()
	c.Close()
}

func (f *forwarder) serve(ctx context.Context, ws Workspace, port int, c net.Conn) {
	if !f.track(c) {
		return
	}
	defer f.untrack(c)
	w, err := Connect(ctx, ws, port)
	if err != nil {
		return
	}
	if !f.track(w) {
		return
	}
	defer f.untrack(w)
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(w, c)
		w.CloseWrite()
		done <- struct{}{}
	}()
	go func() {
		io.Copy(c, w)
		if tc, ok := c.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()
	<-done
	<-done
}
