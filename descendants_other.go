//go:build !linux

package agentworkspace

// reach is how far Local's end of a process reaches here: its process
// group alone. macOS has no /proc, and its process table (sysctl
// kern.proc) is not read here, so a descendant that left the group is
// not found.
const reach = ReachGroup

// processes cannot list the processes here.
func processes() (map[int]procInfo, bool) { return nil, false }

// startOf cannot read a start time here.
func startOf(int) (uint64, bool) { return 0, false }
