package agentworkspace

import "testing"

// WithoutProcessTable points Local's process table at an empty
// directory until the test ends, as on macOS or with a /proc that is
// not this process's own: Local then reaches the group alone and reads
// no table. Tests that use it must not run in parallel.
func WithoutProcessTable(t testing.TB) {
	saved := procRoot
	t.Cleanup(func() { procRoot = saved })
	procRoot = t.TempDir()
}
