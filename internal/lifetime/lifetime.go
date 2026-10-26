// Package lifetime holds what Local's processes and the conformance
// tables that check them must agree on, so the two cannot drift.
package lifetime

import "time"

// Grace is how long a process's end waits at each step: for the
// process to exit after its input closes, and again after SIGTERM,
// before it is killed.
const Grace = 2 * time.Second
