package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var latencyStart = time.Now()

type latencyEntry struct {
	name string
	dur  time.Duration
	at   time.Duration
}

var (
	latencyMu      sync.Mutex
	latencyEntries []latencyEntry
	latencyLast    = latencyStart
)

func latMark(name string) {
	now := time.Now()

	latencyMu.Lock()
	dur := now.Sub(latencyLast)
	latencyLast = now
	at := now.Sub(latencyStart)
	latencyEntries = append(latencyEntries, latencyEntry{name: name, dur: dur, at: at})
	latencyMu.Unlock()

	latPrintf("  + %9s  t=%9s  %s\n", latDur(dur), latDur(at), name)
}

func latSpan(name string) func() {
	start := time.Now()
	return func() {
		now := time.Now()
		dur := now.Sub(start)

		latencyMu.Lock()
		latencyLast = now
		at := now.Sub(latencyStart)
		latencyEntries = append(latencyEntries, latencyEntry{name: name, dur: dur, at: at})
		latencyMu.Unlock()

		latPrintf("  + %9s  t=%9s  %s\n", latDur(dur), latDur(at), name)
	}
}

func latReport() {
	latencyMu.Lock()
	entries := make([]latencyEntry, len(latencyEntries))
	copy(entries, latencyEntries)
	latencyMu.Unlock()

	total := time.Since(latencyStart)

	latPrintf("%s\n", strings.Repeat("─", 48))
	latPrintf("TOTAL %s over %d calls\n", latDur(total), len(entries))

	sort.SliceStable(entries, func(i, j int) bool { return entries[i].dur > entries[j].dur })
	for i, e := range entries {
		if i >= 10 {
			break
		}
		latPrintf("  %9s  %s\n", latDur(e.dur), e.name)
	}
}

func latPrintf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[lat] "+format, args...)
}

func latDur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	}
}
