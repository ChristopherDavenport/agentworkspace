//go:build linux

package agentworkspace

import (
	"bytes"
	"os"
	"strconv"
)

// procRoot is where the process table is read; tests point it elsewhere.
var procRoot = "/proc"

// reachNow is how far Local's end of a process reaches here, now: while
// /proc is this process's (its self link names this process's ID, so
// it is a table of this PID namespace), a walk finds the descendants
// that left the group while their parents live; without one (none
// mounted, an empty one, another namespace's) the group alone.
func reachNow() Reach {
	if procIsOurs() {
		return ReachDescendants
	}
	return ReachGroup
}

// procIsOurs reports whether procRoot lists this process as itself.
func procIsOurs() bool {
	self, err := os.Readlink(procRoot + "/self")
	return err == nil && self == strconv.Itoa(os.Getpid())
}

// processes reads every process this one can see from /proc, and
// reports false when /proc is not this process's to read.
func processes() (map[int]procInfo, bool) {
	if !procIsOurs() {
		return nil, false
	}
	ents, err := os.ReadDir(procRoot)
	if err != nil {
		return nil, false
	}
	ps := make(map[int]procInfo, len(ents))
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(procRoot + "/" + e.Name() + "/stat")
		if err != nil {
			continue // gone since the listing
		}
		if p, ok := parseStat(b); ok {
			ps[pid] = p
		}
	}
	return ps, true
}

// startOf is pid's start time, and false when it cannot be read.
func startOf(pid int) (uint64, bool) {
	b, err := os.ReadFile(procRoot + "/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	p, ok := parseStat(b)
	return p.start, ok
}

// parseStat reads /proc/<pid>/stat (proc_pid_stat(5)): after the
// command's name, which is in parentheses and may hold anything, the
// state, the parent, the group, and in the 22nd field the start time.
func parseStat(b []byte) (procInfo, bool) {
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return procInfo{}, false
	}
	f := bytes.Fields(b[i+1:])
	if len(f) < 20 {
		return procInfo{}, false
	}
	ppid, err1 := strconv.Atoi(string(f[1]))
	pgrp, err2 := strconv.Atoi(string(f[2]))
	start, err3 := strconv.ParseUint(string(f[19]), 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return procInfo{}, false
	}
	return procInfo{ppid: ppid, pgrp: pgrp, start: start, zombie: string(f[0]) == "Z"}, true
}
