package main

import "github.com/steveyegge/beads/internal/latency"

func latMark(name string) {
	latency.Mark(name)
}

func latSpan(name string) func() {
	return latency.Span(name)
}

func latReport() {
	latency.Report()
}
