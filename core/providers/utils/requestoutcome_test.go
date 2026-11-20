package utils

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/valyala/fasthttp"
)

func BenchmarkRequestOutcomeScope(b *testing.B) {
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Hour))
	b.ReportAllocs()
	for b.Loop() {
		BeginRequestOutcome(ctx, false)
		scoped := providerRequestContext(ctx)
		providerRequestOutcome(scoped).dispatch()
	}
}

func TestRequestOutcomePreservesTransportEvidence(t *testing.T) {
	// Providers may construct BifrostError directly instead of using a shared
	// constructor. The public error still needs the same unknown-outcome code.
	result := &schemas.BifrostError{Error: &schemas.ErrorField{
		Error: &requestOutcomeUnknownError{cause: io.ErrUnexpectedEOF},
	}}
	var state *RequestOutcome
	result = state.Preserve(result)
	if result.AllowFallbacks == nil || *result.AllowFallbacks || result.Error.Code == nil || *result.Error.Code != RequestOutcomeUnknownErrorCode {
		t.Fatalf("transport evidence lost in public error: %v", result)
	}
	state = &RequestOutcome{}
	state.Preserve(result)
	// Late transport callbacks and a replacement error cannot retract evidence.
	state.dispatch()
	state.acknowledge(http.StatusOK)
	replacement := state.Preserve(&schemas.BifrostError{StatusCode: schemas.Ptr(429), AllowFallbacks: schemas.Ptr(true)})
	if replacement.AllowFallbacks == nil || *replacement.AllowFallbacks || !RequestOutcomeUnknown(replacement.Error.Error) {
		t.Fatalf("unknown outcome was reclassified: %v", replacement)
	}
}

func TestRequestOutcomeAuxiliaryIsolation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(500)
	}))
	defer upstream.Close()
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Second))
	state := BeginRequestOutcome(ctx, false)
	client := ConfigureDialer(&fasthttp.Client{ReadTimeout: time.Second}, true)
	defer client.CloseIdleConnections()
	authClient := &http.Client{Transport: &fasthttpRoundTripper{client: client}}
	authReq, _ := http.NewRequestWithContext(context.WithoutCancel(ctx), "POST", upstream.URL, strings.NewReader("client_credentials"))
	response, err := authClient.Do(authReq)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if got := state.Preserve(NewBifrostOperationError("token exchange failed", nil)); got.AllowFallbacks != nil || RequestOutcomeUnknown(got.Error.Error) {
		t.Fatal("auxiliary token exchange was counted as inference")
	}
	req, resp := newSilentStreamingRequest(upstream.URL)
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	_, bifrostErr, wait := MakeRequestWithContext(ctx, client, req, resp)
	wait()
	if bifrostErr != nil {
		t.Fatal(bifrostErr)
	}
	got := state.Preserve(NewBifrostUpstreamConnectionError("model response lost", nil))
	if !RequestOutcomeUnknown(got.Error.Error) || got.AllowFallbacks == nil || *got.AllowFallbacks {
		t.Fatal("inference through the same client was not tracked")
	}
}

func TestHTTPRequestStreamEOFDoesNotAcknowledgeInference(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
	}))
	defer upstream.Close()
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("streaming=", streaming), func(t *testing.T) {
			ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Second))
			state := BeginRequestOutcome(ctx, streaming)
			req, _ := http.NewRequestWithContext(ctx, "POST", upstream.URL, strings.NewReader("{}"))
			resp, err := DoHTTPRequest(upstream.Client(), req)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			failure := state.Preserve(NewBifrostOperationError("response contract failed", nil))
			if RequestOutcomeUnknown(failure.Error.Error) != streaming {
				t.Fatalf("clean HTTP EOF: streaming=%v, error=%v", streaming, failure)
			}
		})
	}
}

func TestHTTPRequestOutcomePreservesAcceptedAndUnsent(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint("partial=", partial), func(t *testing.T) {
			var accepted atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if accepted.Add(1) == 1 {
					fmt.Fprint(w, "warm")
					return
				}
				if partial {
					w.Header().Set("Content-Length", "1000")
					fmt.Fprint(w, "partial")
					w.(http.Flusher).Flush()
				}
				conn, _, _ := w.(http.Hijacker).Hijack()
				conn.Close()
			}))
			defer upstream.Close()
			warm, warmErr := upstream.Client().Get(upstream.URL)
			if warmErr != nil {
				t.Fatal(warmErr)
			}
			io.Copy(io.Discard, warm.Body)
			warm.Body.Close()
			req, _ := http.NewRequest("POST", upstream.URL, strings.NewReader(`{"input":"synthetic"}`))
			req.Header.Set("Idempotency-Key", "caller-cannot-assume-provider-support")
			resp, err := DoHTTPRequest(upstream.Client(), req)
			if resp != nil {
				_, err = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			if !RequestOutcomeUnknown(err) {
				t.Fatalf("lost reply needs unknown outcome: %v", err)
			}
			if got := accepted.Load(); got != 2 {
				t.Fatalf("warm request + accepted inference=%d, want 2", got)
			}
		})
	}
	t.Run("dial failure", func(t *testing.T) {
		transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("synthetic unavailable")}
		}}
		defer transport.CloseIdleConnections()
		req, _ := http.NewRequest("POST", "http://synthetic.invalid", strings.NewReader("{}"))
		_, err := DoHTTPRequest(&http.Client{Transport: transport}, req)
		if err == nil || RequestOutcomeUnknown(err) {
			t.Fatalf("proven dial failure became dispatched: %v", err)
		}
	})
}

func TestContextTransportAcceptedPooledRequestIsNotRepeated(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					req, err := http.ReadRequest(reader)
					if err != nil {
						return
					}
					_, err = io.Copy(io.Discard, req.Body)
					req.Body.Close()
					if err != nil {
						return
					}
					if accepted.Add(1) != 1 {
						return
					}
					io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
				}
			}()
		}
	}()
	client := ConfigureDialer(&fasthttp.Client{ReadTimeout: time.Second, WriteTimeout: time.Second}, true)
	defer client.CloseIdleConnections()
	for i := 0; i < 2; i++ {
		req, resp := newSilentStreamingRequest("http://" + ln.Addr().String())
		err := client.Do(req, resp)
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		if i == 0 && err != nil {
			t.Fatal(err)
		}
		if i == 1 && err == nil {
			t.Fatal("accepted request without reply became success")
		}
	}
	if got := accepted.Load(); got != 2 {
		t.Fatalf("warm request + accepted inference produced %d bodies; want 2", got)
	}
}
