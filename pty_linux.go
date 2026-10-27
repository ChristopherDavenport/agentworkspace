//go:build linux

package agentworkspace

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// openTerminal opens a pseudo-terminal of size: its master, which this
// process reads and writes, and its slave, which the command gets as
// its controlling terminal. Both are close-on-exec and not this
// process's controlling terminal.
func openTerminal(size TerminalSize) (master, slave *os.File, err error) {
	master, err = os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	var n uint32
	err = control(master, func(fd uintptr) error {
		unlock := int32(0)
		if err := ioctl(fd, syscall.TIOCSPTLCK, unsafe.Pointer(&unlock)); err != nil {
			return err
		}
		return ioctl(fd, syscall.TIOCGPTN, unsafe.Pointer(&n))
	})
	if err == nil {
		err = setTerminalSize(master, size)
	}
	if err == nil {
		slave, err = os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	}
	if err != nil {
		master.Close()
		return nil, nil, err
	}
	return master, slave, nil
}

// setTerminalSize sets the size of the terminal whose master is f; the
// kernel sends the terminal's foreground group SIGWINCH.
func setTerminalSize(f *os.File, size TerminalSize) error {
	ws := struct{ Row, Col, X, Y uint16 }{size.Rows, size.Cols, 0, 0}
	return control(f, func(fd uintptr) error { return ioctl(fd, syscall.TIOCSWINSZ, unsafe.Pointer(&ws)) })
}

// dupFile is a second descriptor of f, close-on-exec, so that one can
// be closed (a terminal's input) while the other is read (its output).
func dupFile(f *os.File) (*os.File, error) {
	var nfd uintptr
	err := control(f, func(fd uintptr) error {
		r, _, e := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_DUPFD_CLOEXEC, 0)
		if e != 0 {
			return e
		}
		nfd = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return os.NewFile(nfd, f.Name()), nil
}

func ioctl(fd, req uintptr, arg unsafe.Pointer) error {
	if _, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, req, uintptr(arg)); e != 0 {
		return e
	}
	return nil
}

// control runs f on file's descriptor without taking it out of the
// poller, as Fd would.
func control(file *os.File, f func(fd uintptr) error) error {
	rc, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var ferr error
	if err := rc.Control(func(fd uintptr) { ferr = f(fd) }); err != nil {
		return err
	}
	return ferr
}
