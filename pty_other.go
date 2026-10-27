//go:build !linux

package agentworkspace

import (
	"errors"
	"os"
)

// openTerminal is refused here: Local gives a terminal on Linux alone.
func openTerminal(TerminalSize) (master, slave *os.File, err error) {
	return nil, nil, errors.ErrUnsupported
}

func setTerminalSize(*os.File, TerminalSize) error { return errors.ErrUnsupported }

func dupFile(*os.File) (*os.File, error) { return nil, errors.ErrUnsupported }
