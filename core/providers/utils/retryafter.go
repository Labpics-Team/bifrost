package utils

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

// Preserve long provider hints: request cancellation bounds the actual wait.
// Saturate only at the largest whole millisecond representable as time.Duration,
// so native consumers can safely convert the field back to a duration.
const (
	minRetryAfter = time.Second
	maxRetryAfter = time.Duration(1<<63-1) / time.Millisecond * time.Millisecond
)

// ApplyRetryAfter stamps the retry hint from the response headers onto the error's
// ExtraFields.RetryAfter, in milliseconds. It leaves the error alone when the headers carry
// no hint, and when a provider's error parser already set one from the error body, which
// is the provider's own answer for that error. Only explicit hints count, in this order:
//
//   - retry-after-ms: milliseconds (Azure OpenAI);
//   - Retry-After: seconds or an HTTP date (OpenAI, Azure OpenAI, Anthropic, Groq, OpenRouter).
//
// Rate-limit reset headers (x-ratelimit-reset-*, anthropic-ratelimit-*-reset) are not read:
// they say when each limit refills, not how long this request must wait, and a request
// that needs more than a limit has left is refused while that limit still reads non-zero.
func ApplyRetryAfter(bifrostErr *schemas.BifrostError, headers *fasthttp.ResponseHeader) {
	if bifrostErr == nil || headers == nil || bifrostErr.ExtraFields.RetryAfter != 0 {
		return
	}
	header := func(name string) string {
		return strings.TrimSpace(string(headers.Peek(name)))
	}
	if hint, ok := retryAfterHint(header, time.Now()); ok {
		SetRetryAfter(bifrostErr, hint)
	}
}

// ApplyRetryAfterHTTP is ApplyRetryAfter for a net/http response, used by the token exchanges
// that run on an *http.Client rather than fasthttp.
func ApplyRetryAfterHTTP(bifrostErr *schemas.BifrostError, headers http.Header) {
	if bifrostErr == nil || headers == nil || bifrostErr.ExtraFields.RetryAfter != 0 {
		return
	}
	header := func(name string) string {
		return strings.TrimSpace(headers.Get(name))
	}
	if hint, ok := retryAfterHint(header, time.Now()); ok {
		SetRetryAfter(bifrostErr, hint)
	}
}

// SetRetryAfter records a provider's retry hint in milliseconds, with a minimum
// of minRetryAfter. Round up so sub-millisecond precision never shortens the hint.
func SetRetryAfter(bifrostErr *schemas.BifrostError, hint time.Duration) {
	if bifrostErr == nil {
		return
	}
	hint = min(max(hint, minRetryAfter), maxRetryAfter)
	bifrostErr.ExtraFields.RetryAfter = int64((hint-1)/time.Millisecond) + 1
}

// retryAfterHint reads a retry hint from the response headers. now anchors an HTTP-date
// Retry-After.
func retryAfterHint(header func(string) string, now time.Time) (time.Duration, bool) {
	if v := header("retry-after-ms"); v != "" {
		if ms, err := strconv.ParseFloat(v, 64); (err == nil || errors.Is(err, strconv.ErrRange)) && !math.IsNaN(ms) {
			return scaledDuration(ms, time.Millisecond), true
		}
	}
	if v := header("Retry-After"); v != "" {
		if seconds, err := strconv.ParseFloat(v, 64); (err == nil || errors.Is(err, strconv.ErrRange)) && !math.IsNaN(seconds) {
			return scaledDuration(seconds, time.Second), true
		}
		if at, err := http.ParseTime(v); err == nil {
			return at.Sub(now), true
		}
	}
	return 0, false
}

// scaledDuration saturates before conversion and rounds up to preserve the
// lower bound through both nanosecond and millisecond representations.
func scaledDuration(count float64, unit time.Duration) time.Duration {
	scaled := count * float64(unit)
	if scaled >= float64(maxRetryAfter) {
		return maxRetryAfter
	}
	return time.Duration(math.Ceil(max(scaled, 0)))
}
