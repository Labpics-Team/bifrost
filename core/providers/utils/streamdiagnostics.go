package utils

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

type streamDiagnosticsKey struct{}

// StreamDiagnostics records the bounded prefix before the native startup check
// commits an attempt. It retains no stream content. Readers capture this pointer
// when created, so a later retry replacing the context value cannot rebind them.
type StreamDiagnostics struct {
	tracer       schemas.Tracer
	handle       schemas.SpanHandle
	started      time.Time
	stamped      sync.Once
	frozen       atomic.Bool
	status       atomic.Int64
	headersNs    atomic.Int64
	bodyReader   atomic.Bool
	bodyBytes    atomic.Int64
	lastReadNs   atomic.Int64
	sseReader    atomic.Bool
	comments     atomic.Int64
	dataLines    atomic.Int64
	preamble     atomic.Int64
	commitNs     atomic.Int64
	commitReason atomic.Int64
}

// BeginStreamDiagnostics binds diagnostics to the existing attempt span. NoOp
// tracing has no handle and allocates no diagnostic state.
func BeginStreamDiagnostics(ctx *schemas.BifrostContext, tracer schemas.Tracer, handle schemas.SpanHandle) *StreamDiagnostics {
	if tracer == nil || handle == nil {
		return nil
	}
	d := &StreamDiagnostics{tracer: tracer, handle: handle, started: time.Now()}
	ctx.SetValue(streamDiagnosticsKey{}, d)
	return d
}

// StreamDiagnosticsFromContext must be captured at attempt/reader construction,
// not looked up on each chunk or from a finalizer that can outlive its attempt.
func StreamDiagnosticsFromContext(ctx context.Context) *StreamDiagnostics {
	if ctx == nil {
		return nil
	}
	if bc, ok := ctx.(*schemas.BifrostContext); ok && bc == nil {
		return nil
	}
	d, _ := ctx.Value(streamDiagnosticsKey{}).(*StreamDiagnostics)
	return d
}

func (d *StreamDiagnostics) read(n int) {
	if d == nil || n <= 0 || d.frozen.Load() {
		return
	}
	d.bodyBytes.Add(int64(n))
	d.lastReadNs.Store(int64(time.Since(d.started)))
}

func (d *StreamDiagnostics) comment() {
	if d != nil && !d.frozen.Load() {
		d.comments.Add(1)
	}
}

func (d *StreamDiagnostics) dataLine() {
	if d != nil && !d.frozen.Load() {
		d.dataLines.Add(1)
	}
}

func (d *StreamDiagnostics) commit(reason int64) {
	if d == nil || d.frozen.Load() {
		return
	}
	d.commitNs.Store(int64(time.Since(d.started)))
	d.commitReason.Store(reason)
	d.frozen.Store(true)
}

// Stamp writes once, immediately before this attempt's EndSpan. In particular,
// never call it from the post-hook finalizer: that may run after CloseStream and
// after the retry's span was registered. Late reads cannot change the snapshot.
func (d *StreamDiagnostics) Stamp() {
	if d == nil {
		return
	}
	d.stamped.Do(func() {
		d.frozen.Store(true)
		set := func(key string, value any) { d.tracer.SetAttribute(d.handle, key, value) }
		if status := d.status.Load(); status != 0 {
			set("bifrost.stream.start.http_status", status)
			set("bifrost.stream.start.headers_ms", float64(d.headersNs.Load())/float64(time.Millisecond))
		}
		if d.bodyReader.Load() {
			set("bifrost.stream.start.body_bytes", d.bodyBytes.Load())
		}
		if last := d.lastReadNs.Load(); last != 0 {
			set("bifrost.stream.start.last_read_ms", float64(last)/float64(time.Millisecond))
		}
		if d.sseReader.Load() {
			set("bifrost.stream.start.sse_comments", d.comments.Load())
			set("bifrost.stream.start.sse_data_lines", d.dataLines.Load())
		}
		set("bifrost.stream.start.preamble_chunks", d.preamble.Load())
		if reason := d.commitReason.Load(); reason != 0 {
			set("bifrost.stream.start.commit_ms", float64(d.commitNs.Load())/float64(time.Millisecond))
			set("bifrost.stream.start.commit_reason", reason)
		}
	})
}
