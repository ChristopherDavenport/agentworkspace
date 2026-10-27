//go:build linux

package agentworkspace

import "testing"

// A command's name may hold spaces and parentheses; the fields are
// read after the last one.
func TestParseStat(t *testing.T) {
	for _, tc := range []struct {
		name string
		stat string
		want procInfo
		ok   bool
	}{
		{"plain", "42 (sleep) S 7 42 42 0 -1 4194304 101 0 0 0 0 0 0 0 20 0 1 0 12345 2207744 128", procInfo{ppid: 7, pgrp: 42, start: 12345}, true},
		{"a name with ) (", "42 (a) (b c) Z 7 9 9 0 -1 4194304 101 0 0 0 0 0 0 0 20 0 1 0 99 0 0", procInfo{ppid: 7, pgrp: 9, start: 99, zombie: true}, true},
		{"cut short", "42 (sleep) S 7 42", procInfo{}, false},
		{"no name", "42 sleep S 7 42", procInfo{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseStat([]byte(tc.stat))
			if got != tc.want || ok != tc.ok {
				t.Errorf("parseStat = %+v, %v; want %+v, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}
