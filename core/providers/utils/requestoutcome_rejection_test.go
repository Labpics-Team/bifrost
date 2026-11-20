package utils

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
)

func TestRequestOutcomeRetainsKnownRejection(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, status := range []int{400, 401, 403, 404, 429} {
			t.Run(fmt.Sprintf("stream=%v/status=%d", streaming, status), func(t *testing.T) {
				state := &RequestOutcome{streaming: streaming}
				state.dispatch()
				state.Preserve(&schemas.BifrostError{StatusCode: &status})
				// The key selector exhausted the pool without sending a new request.
				result := state.Preserve(&schemas.BifrostError{StatusCode: schemas.Ptr(502), Error: &schemas.ErrorField{Type: schemas.Ptr("upstream_credentials_exhausted")}})
				if RequestOutcomeUnknown(result.Error.Error) || result.Error.Code != nil || result.AllowFallbacks != nil {
					t.Fatalf("known refusal became unknown without another send: %v", result)
				}
				// A later send has its own uncertainty, even on the same tracker.
				state.dispatch()
				result = state.Preserve(NewBifrostUpstreamConnectionError("reply lost", nil))
				if !RequestOutcomeUnknown(result.Error.Error) || result.AllowFallbacks == nil || *result.AllowFallbacks {
					t.Fatalf("new send inherited an earlier rejection: %v", result)
				}
			})
		}
	}
	for _, status := range []int{408, 424, 500, 502, 503, 504} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			state := &RequestOutcome{}
			state.dispatch()
			result := state.Preserve(&schemas.BifrostError{StatusCode: &status})
			if !RequestOutcomeUnknown(result.Error.Error) {
				t.Fatalf("ambiguous status %d became a proven rejection", status)
			}
		})
	}
}

func TestRequestOutcomeConcurrentUnknownCannotBeRetracted(t *testing.T) {
	for range 256 {
		state := &RequestOutcome{}
		state.dispatch()
		start := make(chan struct{})
		var done sync.WaitGroup
		done.Add(2)
		go func() {
			defer done.Done()
			<-start
			state.Preserve(&schemas.BifrostError{StatusCode: schemas.Ptr(401)})
		}()
		go func() {
			defer done.Done()
			<-start
			state.Preserve(&schemas.BifrostError{Error: &schemas.ErrorField{Error: &requestOutcomeUnknownError{cause: io.ErrUnexpectedEOF}}})
		}()
		close(start)
		done.Wait()
		state.dispatch()
		state.acknowledge(200)
		result := state.Preserve(&schemas.BifrostError{StatusCode: schemas.Ptr(429)})
		if state.phase.Load() != requestUnknown || !RequestOutcomeUnknown(result.Error.Error) {
			t.Fatal("concurrent rejection retracted evidence of an unknown outcome")
		}
	}
}
