package wire

import (
	"go/build"
	"strings"
	"syscall"
	"testing"
)

// TestImportBoundary holds the package to its one rule: it and its
// tests import the standard library alone, so a program that only
// relays or answers the methods names them without the MCP SDK.
func TestImportBoundary(t *testing.T) {
	pkg, err := build.Default.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range append(append(pkg.Imports, pkg.TestImports...), pkg.XTestImports...) {
		if first, _, _ := strings.Cut(imp, "/"); strings.Contains(first, ".") {
			t.Errorf("wire imports %q; only the standard library is allowed", imp)
		}
	}
}

// The tables a server checks against cannot be changed through what the
// package hands out: each call is a fresh copy.
func TestTheTablesAreCopies(t *testing.T) {
	Ops()[0] = "write"
	if _, _, err := ParseFileURI("agentworkspace:///write?path=x"); err == nil {
		t.Error("an op was added through Ops")
	}
	if Ops()[0] != OpStat {
		t.Errorf("Ops()[0] = %q", Ops()[0])
	}
	Methods()[0] = "x"
	if Methods()[0] != MethodStart {
		t.Errorf("Methods()[0] = %q", Methods()[0])
	}
	b := DefaultBounds()
	b.MaxProcesses = 1 << 20
	if DefaultBounds().MaxProcesses != MaxProcesses || !DefaultBounds().Valid() {
		t.Errorf("DefaultBounds() = %+v", DefaultBounds())
	}
}

// A signal's name and the signal it names go both ways, for the three
// the wire has and no other.
func TestSignalNames(t *testing.T) {
	for name, sig := range map[string]syscall.Signal{SignalINT: syscall.SIGINT, SignalTERM: syscall.SIGTERM, SignalKILL: syscall.SIGKILL} {
		if got, ok := SignalOf(name); !ok || got != sig {
			t.Errorf("SignalOf(%s) = %v, %v", name, got, ok)
		}
		if got, ok := SignalName(sig); !ok || got != name {
			t.Errorf("SignalName(%v) = %q, %v", sig, got, ok)
		}
	}
	if _, ok := SignalOf("HUP"); ok {
		t.Error("HUP has a signal")
	}
	if _, ok := SignalName(syscall.SIGHUP); ok {
		t.Error("SIGHUP has a name")
	}
}
