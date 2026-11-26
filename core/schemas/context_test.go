package schemas

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// TestRealtimeTurnValues verifies opt-in snapshots, scoping, inheritance, and write guards.
func TestRealtimeTurnValues(t *testing.T) {
	type metadataKey uint8
	const userKey metadataKey = 0
	const requestKey metadataKey = 1
	ctx := NewBifrostContext(context.Background(), NoDeadline)
	if values := ctx.GetRealtimeTurnValues(); values != nil {
		t.Fatalf("unused context allocated a snapshot: %v", values)
	}
	name := "test-plugin"
	scoped := ctx.WithPluginScope(&name)
	scoped.SetValueForRealtimeTurns(userKey, "alice")
	scoped.SetValue(requestKey, "request-only")
	if scoped.Value(userKey) != "alice" || ctx.Value(userKey) != "alice" {
		t.Fatal("opt-in setter must also set the ordinary context value")
	}
	values := scoped.GetRealtimeTurnValues()
	if len(values) != 1 || values[userKey] != "alice" {
		t.Fatalf("snapshot = %v", values)
	}
	values[userKey] = "tampered"
	ctx.SetValue(userKey, "updated")
	if ctx.GetRealtimeTurnValues()[userKey] != "updated" {
		t.Fatal("snapshot must use current values and own its map")
	}
	child := NewBifrostContext(ctx, NoDeadline)
	if child.GetRealtimeTurnValues()[userKey] != "updated" {
		t.Fatal("derived context lost opted-in parent metadata")
	}
	child.SetValue(userKey, "child")
	child.ClearValue(userKey)
	if child.GetRealtimeTurnValues()[userKey] != nil {
		t.Fatal("child clear must shadow opted-in parent metadata")
	}
	ctx.BlockRestrictedWrites()
	scoped.SetValueForRealtimeTurns(BifrostContextKeyVirtualKey, "forbidden")
	if ctx.Value(BifrostContextKeyVirtualKey) != nil {
		t.Fatal("opt-in setter bypassed restricted writes")
	}
	if _, exists := ctx.GetRealtimeTurnValues()[BifrostContextKeyVirtualKey]; exists {
		t.Fatal("rejected key must not be marked")
	}
	scoped.ReleasePluginScope()
	ctx.DrainPluginLogs()
}

// TestRealtimeTurnValuesWrappedParent preserves opt-in inheritance through standard context wrappers.
func TestRealtimeTurnValuesWrappedParent(t *testing.T) {
	type metadataKey uint8
	const userKey metadataKey = 0
	parent := NewBifrostContext(context.Background(), NoDeadline)
	parent.SetValueForRealtimeTurns(userKey, "parent")
	wrapped, cancel := context.WithCancel(parent)
	defer cancel()
	child, childCancel := NewBifrostContextWithCancel(wrapped)
	defer childCancel()
	if child.Value(userKey) != "parent" || child.GetRealtimeTurnValues()[userKey] != "parent" {
		t.Fatal("wrapped parent must provide both ordinary and opted-in values")
	}
	child.SetValue(userKey, "child")
	if child.GetRealtimeTurnValues()[userKey] != "child" {
		t.Fatal("child write must override the wrapped parent snapshot")
	}
	child.ClearValue(userKey)
	if value, ok := child.GetRealtimeTurnValues()[userKey]; !ok || value != nil {
		t.Fatal("explicit clear must shadow the wrapped parent snapshot")
	}
	if parent.GetRealtimeTurnValues()[userKey] != "parent" {
		t.Fatal("child must not mutate the parent snapshot")
	}
	fresh, freshCancel := NewBifrostContextWithCancel(parent)
	defer freshCancel()
	fresh.ClearValue(userKey)
	if value, ok := fresh.GetRealtimeTurnValues()[userKey]; !ok || value != nil {
		t.Fatal("a fresh child's clear must shadow the parent value")
	}
	withValue, valueCancel := NewBifrostContextWithCancel(context.WithValue(wrapped, userKey, "wrapper"))
	defer valueCancel()
	if withValue.GetRealtimeTurnValues()[userKey] != "wrapper" {
		t.Fatal("wrapper value must override the inherited snapshot")
	}
}

// TestRealtimeTurnValuesConcurrent exercises writes and snapshots through plugin scopes.
func TestRealtimeTurnValuesConcurrent(t *testing.T) {
	type metadataKey int
	ctx := NewBifrostContext(context.Background(), NoDeadline)
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Go(func() {
			name := "test-plugin"
			scoped := ctx.WithPluginScope(&name)
			defer scoped.ReleasePluginScope()
			for j := range 100 {
				scoped.SetValueForRealtimeTurns(metadataKey(i), j)
				_ = scoped.GetRealtimeTurnValues()
			}
		})
	}
	workers.Wait()
	if got := len(ctx.GetRealtimeTurnValues()); got != 16 {
		t.Fatalf("marked keys = %d, want 16", got)
	}
	ctx.DrainPluginLogs()
}

// BenchmarkRealtimeTurnValues measures unused contexts and a one-value setup snapshot.
func BenchmarkRealtimeTurnValues(b *testing.B) {
	type metadataKey uint8
	for _, marked := range []bool{false, true} {
		b.Run(fmt.Sprint(marked), func(b *testing.B) {
			ctx := NewBifrostContext(context.Background(), NoDeadline)
			if marked {
				ctx.SetValueForRealtimeTurns(metadataKey(0), "alice")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = ctx.GetRealtimeTurnValues()
			}
		})
	}
}

type traceAttributeTestTracer struct {
	NoOpTracer
	gotTraceID string
	gotSpanID  *string
	gotHandle  SpanHandle
	gotKey     string
	gotValue   any
}

func (t *traceAttributeTestTracer) GetSpanHandleByID(traceID string, spanID *string) SpanHandle {
	t.gotTraceID = traceID
	t.gotSpanID = spanID
	t.gotHandle = struct{}{}
	return t.gotHandle
}

func (t *traceAttributeTestTracer) SetAttribute(handle SpanHandle, key string, value any) {
	if handle != t.gotHandle {
		return
	}
	t.gotKey = key
	t.gotValue = value
}

func TestBifrostContext_SetTraceAttribute_UsesRootSpanHandle(t *testing.T) {
	tracer := &traceAttributeTestTracer{}
	ctx := NewBifrostContext(context.Background(), NoDeadline)
	ctx.SetValue(BifrostContextKeyTracer, tracer)
	ctx.SetValue(BifrostContextKeyTraceID, "trace-123")

	ctx.SetTraceAttribute("custom.root_attr", "root-value")

	if tracer.gotTraceID != "trace-123" {
		t.Fatalf("trace ID = %q, want trace-123", tracer.gotTraceID)
	}
	if tracer.gotSpanID != nil {
		t.Fatalf("span ID = %v, want nil for root span lookup", tracer.gotSpanID)
	}
	if tracer.gotKey != "custom.root_attr" {
		t.Fatalf("attribute key = %q, want custom.root_attr", tracer.gotKey)
	}
	if tracer.gotValue != "root-value" {
		t.Fatalf("attribute value = %v, want root-value", tracer.gotValue)
	}
}

func TestNewBifrostContext_NoGoroutineLeakWithBackgroundAndNoDeadline(t *testing.T) {
	// Get baseline goroutine count
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	// Create multiple contexts with context.Background() and no deadline
	// Previously this would leak goroutines
	contexts := make([]*BifrostContext, 100)
	for i := 0; i < 100; i++ {
		contexts[i] = NewBifrostContext(context.Background(), NoDeadline)
	}

	// Give time for any goroutines to start
	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)

	// Check goroutine count - should not have increased significantly
	// (allow some slack for runtime/test goroutines)
	afterCreate := runtime.NumGoroutine()

	// With the fix, no goroutines should be spawned since there's nothing to watch
	// Allow a small margin for test framework goroutines
	if afterCreate > baseline+10 {
		t.Errorf("Goroutine leak detected: baseline=%d, after creating 100 contexts=%d", baseline, afterCreate)
	}

	// Verify the contexts still work correctly
	for i, ctx := range contexts {
		// Should not be cancelled
		select {
		case <-ctx.Done():
			t.Errorf("Context %d should not be done", i)
		default:
			// Expected
		}

		// Should return nil error
		if ctx.Err() != nil {
			t.Errorf("Context %d Err() should be nil, got %v", i, ctx.Err())
		}

		// Should have no deadline
		if _, ok := ctx.Deadline(); ok {
			t.Errorf("Context %d should not have deadline", i)
		}
	}

	// Explicitly cancel all contexts
	for _, ctx := range contexts {
		ctx.Cancel()
	}

	// Verify all are cancelled
	for i, ctx := range contexts {
		select {
		case <-ctx.Done():
			// Expected
		default:
			t.Errorf("Context %d should be done after Cancel()", i)
		}

		if ctx.Err() != context.Canceled {
			t.Errorf("Context %d Err() should be context.Canceled, got %v", i, ctx.Err())
		}
	}
}

func TestNewBifrostContext_DeadlineCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deadline := time.Now().Add(time.Second)
		ctx := NewBifrostContext(context.Background(), deadline)
		defer ctx.Cancel()
		if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) || ctx.Err() != nil {
			t.Fatal("deadline context did not preserve its live deadline")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if ctx.Err() != context.DeadlineExceeded || context.Cause(ctx) != context.DeadlineExceeded {
			t.Fatal("deadline did not cancel the context with its own cause")
		}
	})
}

func TestNewBifrostContext_ParentCancellation(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	ctx := NewBifrostContext(parent, NoDeadline)
	defer ctx.Cancel()
	parentCancel()

	select {
	case <-ctx.Done():
		// Expected
	default:
		t.Error("Context should be cancelled when parent is cancelled")
	}

	if ctx.Err() != context.Canceled {
		t.Errorf("Context Err() should be context.Canceled, got %v", ctx.Err())
	}
}

// Err must report a parent cancellation on the very next call, as context.WithCancel
// children do, not only once the watcher goroutine has run. A loop that cancels the
// request and then checks ctx.Err() (the injected-tool turn loop) otherwise opens
// one more upstream turn after the caller is gone.
func TestNewBifrostContext_ErrSeesParentCancelSynchronously(t *testing.T) {
	for i := 0; i < 1000; i++ {
		parent, parentCancel := context.WithCancel(context.Background())
		ctx := NewBifrostContext(parent, NoDeadline)
		parentCancel()
		if err := ctx.Err(); err != context.Canceled {
			t.Fatalf("iteration %d: Err() right after the parent cancel = %v, want context.Canceled", i, err)
		}
		select {
		case <-ctx.Done():
		default:
			t.Fatalf("iteration %d: Done() still open after Err() reported the cancel", i)
		}
	}
}

func BenchmarkBifrostContextErr(b *testing.B) {
	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	ctx := NewBifrostContext(parent, NoDeadline)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ctx.Err()
	}
}

func TestNewBifrostContext_DeadlineExpires(t *testing.T) {
	// Create context with short deadline
	deadline := time.Now().Add(50 * time.Millisecond)
	ctx := NewBifrostContext(context.Background(), deadline)

	// Should not be done yet
	select {
	case <-ctx.Done():
		t.Error("Context should not be done before deadline")
	default:
		// Expected
	}

	// Wait for deadline
	time.Sleep(100 * time.Millisecond)

	// Should be done now
	select {
	case <-ctx.Done():
		// Expected
	default:
		t.Error("Context should be done after deadline")
	}

	if ctx.Err() != context.DeadlineExceeded {
		t.Errorf("Context Err() should be context.DeadlineExceeded, got %v", ctx.Err())
	}
}

func TestNewBifrostContext_SetAndGetValue(t *testing.T) {
	ctx := NewBifrostContext(context.Background(), NoDeadline)

	// Set a value
	ctx.SetValue("key1", "value1")

	// Get the value
	if v := ctx.Value("key1"); v != "value1" {
		t.Errorf("Expected value1, got %v", v)
	}

	// Get non-existent key
	if v := ctx.Value("nonexistent"); v != nil {
		t.Errorf("Expected nil for non-existent key, got %v", v)
	}

	// Clean up
	ctx.Cancel()
}

func TestNewBifrostContext_NilParent(t *testing.T) {
	// Should not panic with nil parent
	// Note: passing nil is allowed by NewBifrostContext which converts it to context.Background()
	var nilCtx context.Context //nolint:staticcheck // testing nil parent handling
	ctx := NewBifrostContext(nilCtx, NoDeadline)

	// Should work normally
	if ctx.Err() != nil {
		t.Errorf("New context should have nil error, got %v", ctx.Err())
	}

	ctx.Cancel()

	if ctx.Err() != context.Canceled {
		t.Errorf("Cancelled context should have Canceled error, got %v", ctx.Err())
	}
}

// Plugin logging tests

func TestPluginLog_NoScopeIsNoop(t *testing.T) {
	ctx := NewBifrostContext(context.Background(), NoDeadline)
	ctx.Log(LogLevelInfo, "should be ignored")
	logs := ctx.GetPluginLogs()
	if logs != nil {
		t.Errorf("expected nil logs without plugin scope, got %v", logs)
	}
}

func TestPluginLog_SinglePlugin(t *testing.T) {
	ctx := NewBifrostContext(context.Background(), NoDeadline)
	name := "test-plugin"
	scoped := ctx.WithPluginScope(&name)
	scoped.Log(LogLevelInfo, "hello")
	scoped.Log(LogLevelError, "oops")
	scoped.ReleasePluginScope()

	logs := ctx.GetPluginLogs()
	if len(logs) != 2 {
		t.Fatalf("expected 2 logs, got %d", len(logs))
	}
	if logs[0].PluginName != "test-plugin" || logs[0].Level != LogLevelInfo || logs[0].Message != "hello" {
		t.Errorf("unexpected first log: %+v", logs[0])
	}
	if logs[1].Level != LogLevelError || logs[1].Message != "oops" {
		t.Errorf("unexpected second log: %+v", logs[1])
	}
}

func TestPluginLog_MultiplePlugins(t *testing.T) {
	ctx := NewBifrostContext(context.Background(), NoDeadline)

	name1 := "plugin-a"
	s1 := ctx.WithPluginScope(&name1)
	s1.Log(LogLevelDebug, "a-msg")
	s1.ReleasePluginScope()

	name2 := "plugin-b"
	s2 := ctx.WithPluginScope(&name2)
	s2.Log(LogLevelWarn, "b-msg")
	s2.ReleasePluginScope()

	logs := ctx.GetPluginLogs()
	if len(logs) != 2 {
		t.Fatalf("expected 2 logs, got %d", len(logs))
	}
	if logs[0].PluginName != "plugin-a" {
		t.Errorf("expected plugin-a, got %s", logs[0].PluginName)
	}
	if logs[1].PluginName != "plugin-b" {
		t.Errorf("expected plugin-b, got %s", logs[1].PluginName)
	}
}

func TestPluginLog_DrainTransfersOwnership(t *testing.T) {
	ctx := NewBifrostContext(context.Background(), NoDeadline)
	name := "drain-test"
	scoped := ctx.WithPluginScope(&name)
	scoped.Log(LogLevelInfo, "msg1")
	scoped.ReleasePluginScope()

	drained := ctx.DrainPluginLogs()
	if len(drained) != 1 {
		t.Fatalf("expected 1 drained log, got %d", len(drained))
	}

	// After drain, GetPluginLogs should return nil
	after := ctx.GetPluginLogs()
	if after != nil {
		t.Errorf("expected nil after drain, got %v", after)
	}

	// Second drain should return nil
	second := ctx.DrainPluginLogs()
	if second != nil {
		t.Errorf("expected nil on second drain, got %v", second)
	}
}

func TestPluginLog_ScopedContextValueDelegation(t *testing.T) {
	ctx := NewBifrostContext(context.Background(), NoDeadline)
	ctx.SetValue(BifrostContextKeyTraceID, "trace-123")

	name := "delegate-test"
	scoped := ctx.WithPluginScope(&name)

	// Scoped should read from root
	val := scoped.Value(BifrostContextKeyTraceID)
	if val != "trace-123" {
		t.Errorf("expected trace-123, got %v", val)
	}

	// Scoped should write to root
	type testContextKey string
	const customKey testContextKey = "custom-key"
	scoped.SetValue(customKey, "custom-val")
	if ctx.Value(customKey) != "custom-val" {
		t.Errorf("SetValue on scoped did not delegate to root")
	}

	scoped.ReleasePluginScope()
}

func TestPluginLog_PoolReuse(t *testing.T) {
	ctx := NewBifrostContext(context.Background(), NoDeadline)

	// Create and release multiple scoped contexts to exercise the pool
	for i := 0; i < 100; i++ {
		name := "pool-test"
		scoped := ctx.WithPluginScope(&name)
		scoped.Log(LogLevelInfo, "pooled")
		scoped.ReleasePluginScope()
	}

	logs := ctx.DrainPluginLogs()
	if len(logs) != 100 {
		t.Errorf("expected 100 logs from pool reuse, got %d", len(logs))
	}
}

// Root walks the entire plugin delegate chain and returns the request context.
func TestRoot_UnwrapsChainedValueDelegates(t *testing.T) {
	root := NewBifrostContext(context.Background(), NoDeadline)

	a := "outer"
	b := "inner"
	outer := root.WithPluginScope(&a)
	// Manually build a second scoped context whose delegate is the first
	// scoped context — simulates a plugin that derives its own scope from
	// an already-scoped ctx.
	inner := &BifrostContext{
		parent:        outer.parent,
		done:          outer.done,
		pluginScope:   &b,
		valueDelegate: outer,
	}

	got := inner.Root()
	if got != root {
		t.Fatalf("Root() did not walk the chain to the request root: got %p, want %p", got, root)
	}
	if got.valueDelegate != nil {
		t.Fatalf("Root() returned a context with a non-nil valueDelegate: %+v", got)
	}

	// Sanity: Root() on a non-scoped context returns itself.
	if root.Root() != root {
		t.Fatal("Root() on a non-scoped context should return the receiver")
	}
}

// Releasing hook metadata must not invalidate a derived request context.
func TestNewBifrostContext_DerivedFromReleasedScope_NoPanic(t *testing.T) {
	root := NewBifrostContext(context.Background(), NoDeadline)
	pluginName := "release-race"
	scope := root.WithPluginScope(&pluginName)

	derived := NewBifrostContext(scope, NoDeadline)

	// Release the scope while a derived child is still alive — same shape as
	// core/bifrost.go's plugin pipeline releasing pluginCtx after PreLLMHook
	// returns, while plugins/semanticcache holds the embeddingCtx child.
	scope.ReleasePluginScope()

	// All three accessors used to crash via the parent chain. After the unwrap
	// fix, derived.parent is the (still-live) root.
	_, _ = derived.Deadline()
	_ = derived.Err()
	_ = derived.Done()
}

// Cancellation observers may outlive release of hook metadata.
func TestNewBifrostContext_WatchdogRaceWithReleasedScope(t *testing.T) {
	root := NewBifrostContext(context.Background(), NoDeadline)
	pluginName := "watchdog-race"

	const iterations = 200
	derivedCtxs := make([]*BifrostContext, 0, iterations)
	for i := 0; i < iterations; i++ {
		scope := root.WithPluginScope(&pluginName)
		// Derive before release, keeping cancellation attached to the request.
		derivedCtxs = append(derivedCtxs, NewBifrostContext(scope, NoDeadline))
		scope.ReleasePluginScope()
	}

	// Yield so any late-scheduled watchdog goroutines run their Deadline()
	// observation after the surrounding scope was released. Under -race, any
	// remaining racy read of a pool-reset field would be flagged here.
	runtime.Gosched()
	time.Sleep(50 * time.Millisecond)

	// Touch each derived context to keep it live across the sleep — without
	// this, the compiler/GC could optimize them away before the race window
	// has a chance to manifest.
	for _, c := range derivedCtxs {
		_, _ = c.Deadline()
	}
}

// TestReservedKey_SkipProviderCheckIsProtected pins BifrostContextKeySkipProviderCheck
// as reserved. Governance uses it to bypass virtual-key provider allowlists, so a
// restricted (plugin) write must not be able to flip it. It regressed out of
// reservedKeys during the slice->map refactor.
func TestReservedKey_SkipProviderCheckIsProtected(t *testing.T) {
	if !isReservedKey(BifrostContextKeySkipProviderCheck) {
		t.Fatal("BifrostContextKeySkipProviderCheck must be a reserved key")
	}

	ctx := NewBifrostContext(context.Background(), NoDeadline)
	defer ctx.Cancel()

	// Trusted write before the guard is armed.
	ctx.SetValue(BifrostContextKeySkipProviderCheck, true)
	ctx.BlockRestrictedWrites()

	// A restricted write must be silently dropped, leaving the flag untouched.
	ctx.SetValue(BifrostContextKeySkipProviderCheck, false)
	if v := ctx.Value(BifrostContextKeySkipProviderCheck); v != true {
		t.Fatalf("reserved key overwritten under BlockRestrictedWrites: got %v, want true", v)
	}
}

// TestReservedKey_AuthCredentialIsProtected pins BifrostContextKeyAuthCredential as reserved.
// Delegated token exchange trusts it to say which credential authenticated the request, so a
// restricted (plugin) write must not be able to replace it.
func TestReservedKey_AuthCredentialIsProtected(t *testing.T) {
	if !isReservedKey(BifrostContextKeyAuthCredential) {
		t.Fatal("BifrostContextKeyAuthCredential must be a reserved key")
	}

	ctx := NewBifrostContext(context.Background(), NoDeadline)
	defer ctx.Cancel()

	recorded := Credential{Kind: "edge_session", Value: "user-1"}
	ctx.SetValue(BifrostContextKeyAuthCredential, recorded)
	ctx.BlockRestrictedWrites()

	ctx.SetValue(BifrostContextKeyAuthCredential, Credential{Kind: "edge_session", Value: "user-2"})
	if v := ctx.Value(BifrostContextKeyAuthCredential); v != recorded {
		t.Fatalf("reserved key overwritten under BlockRestrictedWrites: got %v, want %v", v, recorded)
	}
}

// TestReservedKey_NonComparableKeyDoesNotPanic guards the reserved-write path against
// non-comparable keys. isReservedKey indexes reservedKeys, which panics on a slice/map/
// func key unless the type is asserted first, and ClearValue reaches that guard even
// when userValues is nil.
func TestReservedKey_NonComparableKeyDoesNotPanic(t *testing.T) {
	if isReservedKey([]string{"not", "comparable"}) {
		t.Fatal("a non-comparable key must never be reported reserved")
	}

	ctx := NewBifrostContext(context.Background(), NoDeadline)
	defer ctx.Cancel()
	ctx.BlockRestrictedWrites()

	// userValues is still nil here; the guard must not panic before that check.
	ctx.ClearValue([]string{"slice", "key"})
}

type trustedTestKey struct{}

// TestTrustedValue_HiddenFromUserValues pins the trusted store: nothing a plugin can
// enumerate or copy exposes it, yet children and plugin scopes of the context see it.
func TestTrustedValue_HiddenFromUserValues(t *testing.T) {
	root := NewBifrostContext(context.Background(), NoDeadline)
	defer root.Cancel()
	root.SetTrustedValue(trustedTestKey{}, "secret")

	if v := root.Value(trustedTestKey{}); v != nil {
		t.Fatalf("Value exposed a trusted value: %v", v)
	}
	for k := range root.GetUserValues() {
		if _, ok := k.(trustedTestKey); ok {
			t.Fatal("GetUserValues exposed a trusted key")
		}
	}
	if v := root.GetParentCtxWithUserValues().Value(trustedTestKey{}); v != nil {
		t.Fatalf("GetParentCtxWithUserValues copied a trusted value: %v", v)
	}

	child := NewBifrostContext(root, NoDeadline)
	defer child.Cancel()
	if v := child.TrustedValue(trustedTestKey{}); v != "secret" {
		t.Fatalf("child context lost the trusted value: %v", v)
	}

	scoped := root.WithPluginScope(new("plugin"))
	defer scoped.ReleasePluginScope()
	if v := scoped.TrustedValue(trustedTestKey{}); v != "secret" {
		t.Fatalf("plugin scope lost the trusted value: %v", v)
	}
	scoped.SetTrustedValue(trustedTestKey{}, "rewritten")
	if v := root.TrustedValue(trustedTestKey{}); v != "rewritten" {
		t.Fatalf("scoped write did not reach the root: %v", v)
	}
}

// TestBifrostContextFromContext covers the recovery helper used by code that
// receives a request context through a foreign wrapper (mcp-go's in-process
// transport wraps every dispatched call in its own WithValue, and the tool
// manager adds a timeout on top).
func TestBifrostContextFromContext(t *testing.T) {
	root := NewBifrostContext(context.Background(), NoDeadline)
	type foreignKey struct{}

	t.Run("direct", func(t *testing.T) {
		got, ok := BifrostContextFromContext(root)
		if !ok || got != root {
			t.Fatalf("direct *BifrostContext must be returned as-is (ok=%v)", ok)
		}
	})
	t.Run("through foreign wrappers", func(t *testing.T) {
		timed, cancel := context.WithTimeout(root, time.Minute)
		defer cancel()
		wrapped := context.WithValue(timed, foreignKey{}, "x")
		got, ok := BifrostContextFromContext(wrapped)
		if !ok || got != root {
			t.Fatalf("wrapped BifrostContext must be recovered through Value (ok=%v)", ok)
		}
	})
	t.Run("scoped resolves to root", func(t *testing.T) {
		scoped := root.WithPluginScope(nil)
		defer scoped.ReleasePluginScope()
		got, ok := BifrostContextFromContext(context.WithValue(scoped, foreignKey{}, "x"))
		if !ok || got.Root() != root {
			t.Fatalf("scoped context must resolve to a context rooted at the original (ok=%v)", ok)
		}
	})
	t.Run("absent", func(t *testing.T) {
		if _, ok := BifrostContextFromContext(context.Background()); ok {
			t.Fatal("plain context must not report a BifrostContext")
		}
		if _, ok := BifrostContextFromContext(nil); ok {
			t.Fatal("nil context must not report a BifrostContext")
		}
	})
}

func TestScopedCancelPropagatesToStandardChild(t *testing.T) {
	root := NewBifrostContext(context.Background(), NoDeadline)
	name := "synthetic-plugin"
	scope := root.WithPluginScope(&name)
	scope.Cancel()

	// Creating a child after cancellation makes the closed Done / nil Err
	// defect deterministic: the standard library observes both synchronously.
	child, cancel := context.WithTimeout(root, time.Second)
	defer cancel()
	if root.Err() != context.Canceled || scope.Err() != context.Canceled || child.Err() != context.Canceled {
		t.Fatal("scope cancellation did not publish the same error to request and child")
	}
	if root.Done() != scope.Done() {
		t.Fatal("scope detached cancellation from the request")
	}
	root.Cancel()
	scope.Cancel()
	if root.Err() != context.Canceled {
		t.Fatal("repeated cancellation changed the published error")
	}
}

func TestScopedCancelWithActiveObserver(t *testing.T) {
	for _, parentKind := range []string{"root", "scope", "nested scope"} {
		t.Run(parentKind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root := NewBifrostContext(context.Background(), NoDeadline)
				name := "synthetic-plugin"
				scope := root.WithPluginScope(&name)
				nested := scope.WithPluginScope(&name)
				var parent context.Context = root
				if parentKind == "scope" {
					parent = scope
				} else if parentKind == "nested scope" {
					parent = nested
				}
				child, cancel := context.WithTimeout(parent, time.Second)
				defer cancel()
				scope.ReleasePluginScope()
				nested.ReleasePluginScope()
				nested.Cancel()
				synctest.Wait()
				if root.Err() != context.Canceled || scope.Err() != context.Canceled || nested.Err() != context.Canceled || child.Err() != context.Canceled {
					t.Fatal("released or nested scope lost request cancellation")
				}
				root.Cancel()
				scope.Cancel()
				nested.Cancel()
			})
		})
	}
}

func TestChildTimeoutPreservesRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root := NewBifrostContext(context.Background(), NoDeadline)
		name := "synthetic-plugin"
		scope := root.WithPluginScope(&name)
		child, cancel := context.WithTimeout(scope.Root(), 250*time.Millisecond)
		defer cancel()
		time.Sleep(250 * time.Millisecond)
		synctest.Wait()
		if child.Err() != context.DeadlineExceeded || root.Err() != nil || scope.Err() != nil {
			t.Fatal("child deadline canceled the request")
		}
		select {
		case <-root.Done():
			t.Fatal("request Done closed after child timeout")
		default:
		}
		root.Cancel()
		if scope.Err() != context.Canceled || child.Err() != context.DeadlineExceeded {
			t.Fatal("request cancellation changed an already published child error")
		}
	})
}

func TestCancellationKeepsFirstError(t *testing.T) {
	for _, first := range []string{"local", "parent deadline"} {
		t.Run(first, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancelParent := context.WithTimeout(context.Background(), time.Second)
				defer cancelParent()
				root := NewBifrostContext(parent, NoDeadline)
				var want error = context.DeadlineExceeded
				if first == "local" {
					want = context.Canceled
					root.Cancel()
				}
				time.Sleep(time.Second)
				synctest.Wait()
				root.Cancel()
				if root.Err() != want || context.Cause(root) != want {
					t.Fatalf("first cancellation changed: Err=%v Cause=%v want=%v", root.Err(), context.Cause(root), want)
				}
			})
		})
	}
}

func TestCancellationCausePreservesFirstCancellation(t *testing.T) {
	for _, first := range []string{"local", "parent"} {
		t.Run(first, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				type metadataKey struct{}
				metadata := &struct{ value string }{"synthetic-request-metadata"}
				parent, cancelParent := context.WithCancelCause(context.WithValue(context.Background(), metadataKey{}, metadata))
				defer cancelParent(nil)
				deadline := time.Now().Add(time.Minute)
				root := NewBifrostContext(parent, deadline)
				defer root.Cancel()
				name := "synthetic-plugin"
				scope := root.WithPluginScope(&name)
				child, cancelChild := context.WithCancel(scope)
				defer cancelChild()
				parentCause := errors.New("synthetic-parent-shutdown")
				var wantCause error = context.Canceled
				if first == "local" {
					root.Cancel()
					synctest.Wait()
					if context.Cause(root) != context.Canceled || context.Cause(child) != context.Canceled {
						t.Fatal("local cancellation did not publish its own cause")
					}
					cancelParent(parentCause)
				} else {
					wantCause = parentCause
					cancelParent(parentCause)
					synctest.Wait()
					root.Cancel()
				}
				synctest.Wait()
				lateChild, cancelLateChild := context.WithCancel(root)
				defer cancelLateChild()
				for _, ctx := range []context.Context{root, scope, child, lateChild} {
					if ctx.Err() != context.Canceled || context.Cause(ctx) != wantCause {
						t.Fatalf("first cancellation changed: Err=%v Cause=%v wantCause=%v", ctx.Err(), context.Cause(ctx), wantCause)
					}
					if ctx.Value(metadataKey{}) != metadata {
						t.Fatal("cancellation metadata isolation lost ordinary parent values")
					}
					if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
						t.Fatal("cancellation metadata isolation lost the request deadline")
					}
				}
			})
		})
	}
}

func TestCancellationPreservesParentTimeoutCause(t *testing.T) {
	for _, deadlineKind := range []string{"inherited", "equal", "later", "earlier"} {
		t.Run(deadlineKind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parentCause := errors.New("synthetic-parent-timeout")
				parent, cancelParent := context.WithTimeoutCause(context.Background(), time.Second, parentCause)
				defer cancelParent()
				inherited, _ := parent.Deadline()
				deadline := NoDeadline
				wantDeadline := inherited
				var wantCause error = parentCause
				switch deadlineKind {
				case "equal":
					deadline = inherited
				case "later":
					deadline = inherited.Add(time.Second)
				case "earlier":
					deadline = inherited.Add(-500 * time.Millisecond)
					wantDeadline = deadline
					wantCause = context.DeadlineExceeded
				}
				root := NewBifrostContext(parent, deadline)
				defer root.Cancel()
				name := "synthetic-plugin"
				scope := root.WithPluginScope(&name)
				child, cancelChild := context.WithCancel(scope)
				defer cancelChild()
				time.Sleep(time.Second)
				synctest.Wait()
				lateChild, cancelLateChild := context.WithCancel(root)
				defer cancelLateChild()
				for _, ctx := range []context.Context{root, scope, child, lateChild} {
					if ctx.Err() != context.DeadlineExceeded || context.Cause(ctx) != wantCause {
						t.Fatalf("timeout cause changed: Err=%v Cause=%v wantCause=%v", ctx.Err(), context.Cause(ctx), wantCause)
					}
					if got, ok := ctx.Deadline(); !ok || !got.Equal(wantDeadline) {
						t.Fatal("timeout inheritance changed the effective deadline")
					}
				}
			})
		})
	}
}

// TestGetOrStoreOpencodeSession pins the load-or-store helper behind the
// reserved BifrostContextKeyOpencodeSession: the supplier result is cached,
// later calls skip the supplier, the write lands under BlockRestrictedWrites
// (where SetValue drops it), and a plugin-scoped context stores on the root.
func TestGetOrStoreOpencodeSession(t *testing.T) {
	t.Run("caches the first supplier result", func(t *testing.T) {
		ctx := NewBifrostContext(context.Background(), NoDeadline)
		defer ctx.Cancel()
		calls := 0
		supplier := func() string { calls++; return fmt.Sprintf("sess-%d", calls) }
		if got := ctx.GetOrStoreOpencodeSession(supplier); got != "sess-1" {
			t.Fatalf("first call = %q, want sess-1", got)
		}
		if got := ctx.GetOrStoreOpencodeSession(supplier); got != "sess-1" {
			t.Fatalf("second call = %q, want cached sess-1", got)
		}
		if calls != 1 {
			t.Fatalf("supplier ran %d times, want 1", calls)
		}
	})

	t.Run("stores under BlockRestrictedWrites where SetValue is dropped", func(t *testing.T) {
		ctx := NewBifrostContext(context.Background(), NoDeadline)
		defer ctx.Cancel()
		ctx.BlockRestrictedWrites()
		ctx.SetValue(BifrostContextKeyOpencodeSession, "forged")
		if got := ctx.Value(BifrostContextKeyOpencodeSession); got != nil {
			t.Fatalf("SetValue on the reserved key landed under BlockRestrictedWrites: %v", got)
		}
		if got := ctx.GetOrStoreOpencodeSession(func() string { return "resolved" }); got != "resolved" {
			t.Fatalf("GetOrStoreOpencodeSession() = %q, want resolved", got)
		}
		if got := ctx.Value(BifrostContextKeyOpencodeSession); got != "resolved" {
			t.Fatalf("stored value = %v, want resolved", got)
		}
	})

	t.Run("plugin-scoped context stores on the root", func(t *testing.T) {
		root := NewBifrostContext(context.Background(), NoDeadline)
		defer root.Cancel()
		name := "plugin"
		scoped := root.WithPluginScope(&name)
		if got := scoped.GetOrStoreOpencodeSession(func() string { return "scoped" }); got != "scoped" {
			t.Fatalf("scoped call = %q, want scoped", got)
		}
		if got := root.GetOrStoreOpencodeSession(func() string { return "other" }); got != "scoped" {
			t.Fatalf("root call = %q, want the value stored through the scoped context", got)
		}
	})

	t.Run("concurrent first callers all return one stored value", func(t *testing.T) {
		ctx := NewBifrostContext(context.Background(), NoDeadline)
		defer ctx.Cancel()
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			counter int
		)
		results := make([]string, 64)
		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i] = ctx.GetOrStoreOpencodeSession(func() string {
					mu.Lock()
					defer mu.Unlock()
					counter++
					return fmt.Sprintf("sess-%d", counter)
				})
			}(i)
		}
		wg.Wait()
		for _, r := range results {
			if r != results[0] {
				t.Fatalf("concurrent callers saw %q and %q, want one value", results[0], r)
			}
		}
	})
}
