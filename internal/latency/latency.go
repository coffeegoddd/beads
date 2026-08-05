package latency

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var start = time.Now()

type entry struct {
	name string
	dur  time.Duration
	at   time.Duration
}

var (
	mu      sync.Mutex
	entries []entry
	last    = start
)

func Mark(name string) {
	now := time.Now()

	mu.Lock()
	dur := now.Sub(last)
	last = now
	at := now.Sub(start)
	entries = append(entries, entry{name: name, dur: dur, at: at})
	mu.Unlock()

	Printf("  + %9s  t=%9s  %s\n", Dur(dur), Dur(at), name)
}

func Span(name string) func() {
	begin := time.Now()
	return func() {
		now := time.Now()
		dur := now.Sub(begin)

		mu.Lock()
		last = now
		at := now.Sub(start)
		entries = append(entries, entry{name: name, dur: dur, at: at})
		mu.Unlock()

		Printf("  + %9s  t=%9s  %s\n", Dur(dur), Dur(at), name)
	}
}

func Report() {
	mu.Lock()
	snapshot := make([]entry, len(entries))
	copy(snapshot, entries)
	mu.Unlock()

	total := time.Since(start)

	Printf("%s\n", strings.Repeat("─", 48))
	Printf("TOTAL %s over %d calls\n", Dur(total), len(snapshot))

	sort.SliceStable(snapshot, func(i, j int) bool { return snapshot[i].dur > snapshot[j].dur })
	for i, e := range snapshot {
		if i >= 10 {
			break
		}
		Printf("  %9s  %s\n", Dur(e.dur), e.name)
	}
}

func Printf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[lat] "+format, args...)
}

func Dur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	}
}
