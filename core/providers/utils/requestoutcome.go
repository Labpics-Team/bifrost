package utils

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"

	"github.com/maximhq/bifrost/core/schemas"
)

const RequestOutcomeUnknownErrorCode = "request_outcome_unknown"

type requestOutcomeContextKey struct{}
type providerRequestOutcomeContextKey struct{}

const (
	requestNotSent uint32 = iota
	requestDispatched
	requestComplete
	requestUnknown
)

// RequestOutcome is scoped to one provider attempt. A write is evidence of a
// possible external effect; absence of output is never evidence of rejection.
type RequestOutcome struct {
	phase     atomic.Uint32
	streaming bool
}

func BeginRequestOutcome(ctx *schemas.BifrostContext, streaming bool) *RequestOutcome {
	outcome := &RequestOutcome{streaming: streaming}
	ctx.SetValue(requestOutcomeContextKey{}, outcome)
	return outcome
}

func requestOutcomeFromContext(ctx context.Context) *RequestOutcome {
	if ctx == nil {
		return nil
	}
	outcome, _ := ctx.Value(requestOutcomeContextKey{}).(*RequestOutcome)
	return outcome
}

func ClearRequestOutcome(ctx *schemas.BifrostContext) {
	ctx.ClearValue(requestOutcomeContextKey{})
}

func providerRequestContext(ctx context.Context) context.Context {
	if outcome := requestOutcomeFromContext(ctx); outcome != nil {
		return context.WithValue(ctx, providerRequestOutcomeContextKey{}, outcome)
	}
	return ctx
}

func providerRequestOutcome(ctx context.Context) *RequestOutcome {
	outcome, _ := ctx.Value(providerRequestOutcomeContextKey{}).(*RequestOutcome)
	return outcome
}

func PreserveRequestOutcome(ctx *schemas.BifrostContext, result *schemas.BifrostError) *schemas.BifrostError {
	if ctx == nil {
		return result
	}
	return requestOutcomeFromContext(ctx).Preserve(result)
}

func (o *RequestOutcome) dispatch() {
	if o != nil {
		for phase := o.phase.Load(); phase != requestUnknown; phase = o.phase.Load() {
			if o.phase.CompareAndSwap(phase, requestDispatched) {
				return
			}
		}
	}
}

func (o *RequestOutcome) acknowledge(status int) {
	if o != nil && !o.streaming && status >= 200 && status < 300 {
		o.phase.CompareAndSwap(requestDispatched, requestComplete)
	}
}

// Preserve allows a new attempt after an explicit client/credential/quota
// rejection or a complete successful response, but never after an ambiguous
// failure or lost stream. A full model answer rejected by an explicit post-hook
// is a known result: its configured fallback policy remains applicable.
// Read-only HTTP operations and failures before dispatch keep their retry policy.
func (o *RequestOutcome) Preserve(result *schemas.BifrostError) *schemas.BifrostError {
	if result == nil {
		return result
	}
	if result.Error != nil && RequestOutcomeUnknown(result.Error.Error) {
		if o != nil {
			o.phase.Store(requestUnknown)
		}
		return preserveRequestOutcome(result, result.Error.Error)
	}
	if o == nil {
		return result
	}
	phase := o.phase.Load()
	if phase == requestNotSent || phase == requestComplete {
		return result
	}
	if phase != requestUnknown && result.StatusCode != nil {
		status := *result.StatusCode
		// 408 and 424 can describe a model processing timeout/failure (Bedrock),
		// not a rejected request. They cannot establish a safe retry.
		if status >= 400 && status < 500 && status != 408 && status != 424 {
			// A later local error does not make this completed refusal unknown.
			// Never overwrite unknown evidence from a concurrent transport failure.
			if o.phase.CompareAndSwap(requestDispatched, requestComplete) || o.phase.Load() == requestComplete {
				return result
			}
		}
	}
	// A later hook may replace the error or its status; it cannot turn an
	// already unknown provider outcome into proof of rejection.
	o.phase.Store(requestUnknown)
	if result.Error == nil {
		result.Error = &schemas.ErrorField{}
	}
	cause := result.Error.Error
	if cause == nil {
		cause = errors.New("provider request outcome is unknown")
	}
	result.Error.Error = &requestOutcomeUnknownError{cause: cause}
	return preserveRequestOutcome(result, result.Error.Error)
}

type requestOutcomeUnknownError struct{ cause error }

func (e *requestOutcomeUnknownError) Error() string { return e.cause.Error() }
func (e *requestOutcomeUnknownError) Unwrap() error { return e.cause }

// RequestOutcomeUnknown identifies a transport failure after a non-idempotent
// request wrote bytes. A missing reply cannot prove the provider did no work.
func RequestOutcomeUnknown(err error) bool {
	var unknown *requestOutcomeUnknownError
	return errors.As(err, &unknown)
}

func requestMethodIsIdempotent(method []byte) bool {
	// RFC 9110 section 9.2.2. An idempotency header alone proves nothing about
	// an arbitrary provider's deduplication contract.
	switch string(method) {
	case "GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE":
		return true
	}
	return false
}

type requestWriteConn struct {
	net.Conn
	written      bool
	outcomeState *RequestOutcome
}

func (c *requestWriteConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.written = true
		c.outcomeState.dispatch()
	}
	return n, err
}

func (c *requestWriteConn) outcome(err error) error {
	if err != nil && c.written && !RequestOutcomeUnknown(err) {
		return &requestOutcomeUnknownError{cause: err}
	}
	return err
}

func preserveRequestOutcome(result *schemas.BifrostError, cause error) *schemas.BifrostError {
	if RequestOutcomeUnknown(cause) {
		result.AllowFallbacks = new(false)
		result.Error.Code = new(RequestOutcomeUnknownErrorCode)
	}
	return result
}

// trackHTTPRequest preserves net/http's distinction between failure to obtain a
// connection and a request whose transmission may have started. GotConn runs
// before writes; using it conservatively also covers partial header writes.
// Transports without trace support cannot prove a pre-send failure.
func trackHTTPRequest(req *http.Request) (*http.Request, func(error) error) {
	if requestMethodIsIdempotent([]byte(req.Method)) {
		return req, func(err error) error { return err }
	}
	var phase atomic.Int32 // 0: unobserved, 1: obtaining connection, 2: may have written
	outcome := requestOutcomeFromContext(req.Context())
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { phase.CompareAndSwap(0, 1) },
		GotConn: func(httptrace.GotConnInfo) {
			phase.Store(2)
			outcome.dispatch()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	// A caller's idempotency header does not establish a provider deduplication
	// contract. Prevent net/http from silently replaying this body's bytes.
	req.GetBody = nil
	return req, func(err error) error {
		if err != nil && phase.Load() != 1 && !RequestOutcomeUnknown(err) {
			return &requestOutcomeUnknownError{cause: err}
		}
		return err
	}
}

// Keep delivery provenance inside the timing wrapper: stream readers may
// remove timing, but must not remove the unknown-outcome evidence.
type requestOutcomeBody struct {
	io.ReadCloser
	outcome   *RequestOutcome
	status    int
	remaining int64
}

func (b *requestOutcomeBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.remaining >= 0 {
		b.remaining -= int64(n)
	}
	if errors.Is(err, io.EOF) || (err == nil && b.remaining == 0) {
		b.outcome.acknowledge(b.status)
	}
	if err != nil && !errors.Is(err, io.EOF) && !RequestOutcomeUnknown(err) {
		err = &requestOutcomeUnknownError{cause: err}
	}
	return n, err
}
