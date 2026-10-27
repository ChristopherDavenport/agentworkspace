//go:build linux

package agentworkspace

import (
	"bytes"
	"os"
	"strconv"
)

// reach is how far Local's end of a process reaches here: /proc names
// every process's parent, so a walk finds the descendants that left the
// group while their parents live.
const reach = ReachDescendants

// processes reads every process this one can see from /proc, and
// reports false when it cannot read /proc at all.
func processes() (map[int]procInfo, bool) {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return nil, false
	}
	ps := make(map[int]procInfo, len(ents))
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue // gone since the listing
		}
		if p, ok := parseStat(b); ok {
			ps[pid] = p
		}
	}
	return ps, true
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
