package semanticcache

import (
	"context"
	"fmt"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/vectorstore"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type cacheRetentionStore struct {
	*observableStore
	deleteFilter func(context.Context, string, []vectorstore.Query) error
}

func (s *cacheRetentionStore) DeleteByFilter(ctx context.Context, namespace string, queries []vectorstore.Query) error {
	return s.deleteFilter(ctx, namespace, queries)
}

type cacheRetentionLogger struct {
	schemas.Logger
	mu       sync.Mutex
	warnings []string
}

func (l *cacheRetentionLogger) Warn(message string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warnings = append(l.warnings, fmt.Sprintf(message, args...))
}

func newCacheRetentionPlugin(t *testing.T, ctx context.Context, store vectorstore.VectorStore, logger schemas.Logger) *Plugin {
	t.Helper()
	llmPlugin, err := Init(ctx, &Config{Dimension: 1, VectorStoreNamespace: "retention-test"}, logger, store)
	if err != nil {
		t.Fatal(err)
	}
	return llmPlugin.(*Plugin)
}

func TestCacheRetentionStartupAndPeriodicSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type call struct {
			namespace string
			queries   []vectorstore.Query
			deadline  time.Time
		}
		calls := make(chan call, 4)
		store := &cacheRetentionStore{observableStore: newObservableStore()}
		store.deleteFilter = func(ctx context.Context, namespace string, queries []vectorstore.Query) error {
			deadline, _ := ctx.Deadline()
			calls <- call{namespace, append([]vectorstore.Query(nil), queries...), deadline}
			return nil
		}
		start := time.Now()
		initCtx, cancelInit := context.WithCancel(context.Background())
		defer cancelInit()
		config := &Config{
			Provider: "cloudflare", EmbeddingModel: "@cf/baai/bge-m3", Dimension: 1024,
			TTL: 5 * time.Minute, VectorStoreNamespace: "retention-test",
			CacheByModel: new(false), CacheByProvider: new(false),
		}
		llmPlugin, err := Init(initCtx, config, bifrost.NewDefaultLogger(schemas.LogLevelError), store)
		if err != nil {
			t.Fatal(err)
		}
		plugin := llmPlugin.(*Plugin)
		defer plugin.Cleanup()
		cancelInit() // The completed Init request must not own the worker lifetime.
		synctest.Wait()

		check := func(cutoff time.Time) {
			t.Helper()
			select {
			case got := <-calls:
				want := []vectorstore.Query{
					{Field: "from_bifrost_semantic_cache_plugin", Operator: vectorstore.QueryOperatorEqual, Value: true},
					{Field: "expires_at", Operator: vectorstore.QueryOperatorGreaterThan, Value: int64(0)},
					{Field: "expires_at", Operator: vectorstore.QueryOperatorLessThan, Value: cutoff.Unix()},
				}
				if got.namespace != "retention-test" || !reflect.DeepEqual(got.queries, want) {
					t.Fatalf("unsafe retention scope: namespace=%q queries=%+v", got.namespace, got.queries)
				}
				if !got.deadline.Equal(cutoff.Add(30 * time.Second)) {
					t.Fatalf("deadline=%v, want %v", got.deadline, cutoff.Add(30*time.Second))
				}
			default:
				t.Fatal("expected a retention pass")
			}
		}
		check(start)
		if len(calls) != 0 {
			t.Fatal("more than one startup pass")
		}
		time.Sleep(59 * time.Second)
		synctest.Wait()
		if len(calls) != 0 {
			t.Fatal("retention ran before its interval")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		check(start.Add(time.Minute))
		if !VectorStoreProperties["expires_at"].Filterable || VectorStoreProperties["response"].Filterable || VectorStoreProperties["stream_chunks"].Filterable {
			t.Fatal("retention must index expiration without indexing response bodies")
		}
		if config.Provider != "cloudflare" || config.EmbeddingModel != "@cf/baai/bge-m3" || config.Dimension != 1024 || config.TTL != 5*time.Minute || *config.CacheByModel || *config.CacheByProvider {
			t.Fatal("retention changed existing cache policy or embedding configuration")
		}
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if len(calls) != 0 {
			t.Fatal("retention continued after Cleanup returned")
		}
	})
}

func TestCacheRetentionDeadlineAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 4)
		finished := make(chan error, 4)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		store := &cacheRetentionStore{observableStore: newObservableStore()}
		callNumber := 0 // Accessed only by the serial worker.
		store.deleteFilter = func(ctx context.Context, _ string, _ []vectorstore.Query) error {
			callNumber++
			started <- struct{}{}
			<-ctx.Done()
			finished <- ctx.Err()
			if callNumber == 2 {
				<-release // Cleanup must join the canceled operation before returning.
			}
			return ctx.Err()
		}
		logger := &cacheRetentionLogger{Logger: bifrost.NewDefaultLogger(schemas.LogLevelError)}
		plugin := newCacheRetentionPlugin(t, context.Background(), store, logger)
		defer func() { unblock(); _ = plugin.Cleanup() }()
		synctest.Wait()
		if len(started) != 1 {
			t.Fatal("startup cleanup did not start")
		}
		<-started
		time.Sleep(30 * time.Second)
		synctest.Wait()
		select {
		case err := <-finished:
			if err != context.DeadlineExceeded {
				t.Fatalf("pass ended with %v, want deadline exceeded", err)
			}
		default:
			t.Fatal("blocked store was not canceled after 30 seconds")
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if len(started) != 1 {
			t.Fatal("a failed pass prevented the next periodic pass")
		}
		<-started
		cleanupDone := make(chan error, 1)
		go func() { cleanupDone <- plugin.Cleanup() }()
		synctest.Wait()
		select {
		case err := <-finished:
			if err != context.Canceled {
				t.Fatalf("shutdown ended the store call with %v, want canceled", err)
			}
		default:
			t.Fatal("Cleanup did not cancel the current store call")
		}
		select {
		case <-cleanupDone:
			t.Fatal("Cleanup returned before the store call completed")
		default:
		}
		unblock()
		synctest.Wait()
		select {
		case err := <-cleanupDone:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("Cleanup did not join the completed worker")
		}
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		logger.mu.Lock()
		defer logger.mu.Unlock()
		if len(logger.warnings) != 1 || !strings.Contains(logger.warnings[0], "deadline exceeded") {
			t.Fatalf("want one timeout warning and no shutdown warning, got %v", logger.warnings)
		}
	})
}

func TestCacheRetentionNoOverlap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 4)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		store := &cacheRetentionStore{observableStore: newObservableStore()}
		store.deleteFilter = func(ctx context.Context, _ string, _ []vectorstore.Query) error {
			started <- struct{}{}
			<-release // Deliberately ignore the deadline to expose spawned-per-tick work.
			return ctx.Err()
		}
		plugin := newCacheRetentionPlugin(t, context.Background(), store, bifrost.NewDefaultLogger(schemas.LogLevelError))
		defer func() { unblock(); _ = plugin.Cleanup() }()
		synctest.Wait()
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if len(started) != 1 {
			t.Fatalf("overlapping cleanup calls: got %d, want 1", len(started))
		}
		done := make(chan struct{})
		go func() { _ = plugin.Cleanup(); close(done) }()
		synctest.Wait()
		unblock()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("worker did not finish after its store call returned")
		}
		if len(started) != 1 {
			t.Fatal("a queued tick started another operation after cancellation")
		}
	})
}

func TestCacheRetentionLegacyStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newObservableStore()
		logger := &cacheRetentionLogger{Logger: bifrost.NewDefaultLogger(schemas.LogLevelError)}
		plugin := newCacheRetentionPlugin(t, context.Background(), store, logger)
		defer plugin.Cleanup()
		time.Sleep(3 * time.Minute)
		synctest.Wait()
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		logger.mu.Lock()
		defer logger.mu.Unlock()
		if len(logger.warnings) != 1 || !strings.Contains(logger.warnings[0], "does not support conditional deletion") {
			t.Fatalf("unsupported cleanup must be visible once at Init, got %v", logger.warnings)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		if plugin.retentionCancel != nil || len(store.deleteIDs) != 0 || len(store.deleteAllQueries) != 0 || store.namespaceDeletes != 0 {
			t.Fatal("a store without conditional deletion received a destructive fallback")
		}
	})
}

func TestCacheRetentionLookupDoesNotDeleteRefreshedID(t *testing.T) {
	store := &cacheRetentionStore{observableStore: newObservableStore()}
	plugin := newTestPlugin(t, store)
	defer plugin.Cleanup()
	id := "same-deterministic-cache-id"
	fresh := vectorstore.SearchResult{ID: id, Properties: map[string]interface{}{"expires_at": time.Now().Add(time.Hour).Unix()}}
	store.chunks[id] = fresh
	staleSnapshot := vectorstore.SearchResult{ID: id, Properties: map[string]interface{}{"expires_at": time.Now().Add(-time.Hour).Unix()}}
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	result, err := plugin.buildResponseFromResult(ctx, nil, nil, staleSnapshot, CacheTypeDirect, nil, nil)
	if err != nil || result != nil {
		t.Fatalf("expired observation must be a miss, got result=%v err=%v", result, err)
	}
	plugin.WaitForPendingOperations()
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.deleteIDs) != 0 || !reflect.DeepEqual(store.chunks[id], fresh) {
		t.Fatal("stale observation deleted the refreshed cache ID")
	}
}

func TestCacheRetentionWriterShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		plugin := newTestPlugin(t, newObservableStore())
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var writes atomic.Int32
		plugin.cacheWriter("admitted", func(context.Context) { close(started); <-release; writes.Add(1) })
		<-started
		go func() { _ = plugin.Cleanup(); close(done) }()
		<-plugin.stopCh
		plugin.cacheWriter("during-shutdown", func(context.Context) { writes.Add(100) })
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("Cleanup returned before its admitted write completed")
		default:
		}
		close(release)
		<-done
		plugin.cacheWriter("after-shutdown", func(context.Context) { writes.Add(100) })
		plugin.WaitForPendingOperations()
		if got := writes.Load(); got != 1 {
			t.Fatalf("retired instance accepted extra writes: got %d want 1", got)
		}
	})
}

func TestCacheRetentionLateDelete(t *testing.T) {
	store := newObservableStore()
	plugin := newTestPlugin(t, store)
	if err := plugin.Cleanup(); err != nil {
		t.Fatal(err)
	}
	ctx := schemas.NewBifrostContext(context.Background(), time.Now().Add(time.Second))
	defer ctx.Cancel()
	_, err := plugin.buildResponseFromResult(ctx, nil, nil, vectorstore.SearchResult{ID: "expired", Properties: map[string]interface{}{"expires_at": time.Now().Add(-time.Hour).Unix()}}, CacheTypeDirect, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	plugin.WaitForPendingOperations()
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.deleteIDs) != 0 {
		t.Fatal("retired instance accepted a stale-entry delete")
	}
}

func TestCacheRetentionLatePostHook(t *testing.T) {
	for _, test := range []struct{ stream, failed bool }{{false, false}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("stream=%v/error=%v", test.stream, test.failed), func(t *testing.T) {
			store := newObservableStore()
			plugin := newTestPlugin(t, store)
			ctx := CreateContextWithCacheKeyAndType(t, "retired-cache", CacheTypeDirect)
			request := identityTestChatRequest("late response", nil)
			if test.stream {
				request.RequestType = schemas.ChatCompletionStreamRequest
			}
			mustPreLLMHookMiss(t, plugin, ctx, request)
			if test.stream {
				if _, _, err := plugin.PostLLMHook(ctx, newChatStreamChunk(0, "prefix", nil), nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := plugin.Cleanup(); err != nil {
				t.Fatal(err)
			}
			response := identityTestChatResponse("complete response")
			if test.stream {
				ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
				response = newChatStreamChunk(1, "suffix", nil)
			}
			var upstreamErr *schemas.BifrostError
			if test.failed {
				upstreamErr = &schemas.BifrostError{Error: &schemas.ErrorField{Message: "stream ended with provider error"}}
			}
			got, bifrostErr, err := plugin.PostLLMHook(ctx, response, upstreamErr)
			if got != response || bifrostErr != upstreamErr || err != nil {
				t.Fatal("retired cache changed the delivered response")
			}
			plugin.WaitForPendingOperations()
			requestID, _ := ctx.Value(schemas.BifrostContextKeyRequestID).(string)
			if _, ok := plugin.streamAccumulators.Load(requestID); ok {
				t.Fatal("retired cache retained a late stream accumulator")
			}
			if plugin.getCacheState(requestID) != nil {
				t.Fatal("retired cache retained late request state")
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.addIDs) != 0 {
				t.Fatal("retired cache accepted a late response write")
			}
		})
	}
}

func BenchmarkCacheRetentionWriter(b *testing.B) {
	plugin := &Plugin{stopCh: make(chan struct{}), config: &Config{}, logger: bifrost.NewDefaultLogger(schemas.LogLevelError)}
	write := func(context.Context) {}
	b.ReportAllocs()
	for b.Loop() {
		plugin.cacheWriter("benchmark", write)
		plugin.WaitForPendingOperations()
	}
}
