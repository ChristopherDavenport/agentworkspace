//go:build !linux

package agentworkspace

// procRoot is unused here.
var procRoot = ""

// reachNow is how far Local's end of a process reaches here: its
// process group alone. macOS has no /proc, and its process table
// (sysctl kern.proc) is not read here, so a descendant that left the
// group is not found.
func reachNow() Reach { return ReachGroup }

// processes cannot list the processes here.
func processes() (map[int]procInfo, bool) { return nil, false }

// startOf cannot read a start time here.
func startOf(int) (uint64, bool) { return 0, false }
