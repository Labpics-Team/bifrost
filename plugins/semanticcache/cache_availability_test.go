package semanticcache

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/vectorstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cacheAvailabilityStore struct {
	*observableStore
	create  func(context.Context) error
	get     func(context.Context, string) (vectorstore.SearchResult, error)
	nearest func(context.Context) ([]vectorstore.SearchResult, error)
	add     func(context.Context) error
}

func (s *cacheAvailabilityStore) CreateNamespace(ctx context.Context, _ string, _ int, _ map[string]vectorstore.VectorStoreProperties) error {
	return s.create(ctx)
}
func (s *cacheAvailabilityStore) GetChunk(ctx context.Context, ns, id string) (vectorstore.SearchResult, error) {
	if s.get != nil {
		return s.get(ctx, id)
	}
	return s.observableStore.GetChunk(ctx, ns, id)
}
func (s *cacheAvailabilityStore) GetNearest(ctx context.Context, ns string, vector []float32, queries []vectorstore.Query, selectFields []string, threshold float64, limit int64) ([]vectorstore.SearchResult, error) {
	if s.nearest != nil {
		return s.nearest(ctx)
	}
	return s.observableStore.GetNearest(ctx, ns, vector, queries, selectFields, threshold, limit)
}
func (s *cacheAvailabilityStore) Add(ctx context.Context, ns, id string, embedding []float32, metadata map[string]interface{}) error {
	if s.add != nil {
		if err := s.add(ctx); err != nil {
			return err
		}
	}
	return s.observableStore.Add(ctx, ns, id, embedding, metadata)
}
func cacheAvailabilityRequest() *schemas.BifrostRequest {
	return newCrossProviderChatRequest(schemas.OpenAI, "synthetic-model", schemas.ChatCompletionRequest, "synthetic-input")
}
func cacheAvailabilityContext() *schemas.BifrostContext {
	return withTestRequestID(schemas.NewBifrostContextWithValue(context.Background(), schemas.NoDeadline, CacheKey, "synthetic-cache-key"))
}
func cacheAvailabilityInit(t *testing.T, store vectorstore.VectorStore, config *Config) *Plugin {
	t.Helper()
	initialized, err := Init(context.Background(), config, bifrost.NewDefaultLogger(schemas.LogLevelError), store)
	if err != nil {
		t.Fatal(err)
	}
	return initialized.(*Plugin)
}

func TestCacheAvailabilityStartupAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var available atomic.Bool
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(context.Context) error {
			namespaceCalls.Add(1)
			if !available.Load() {
				return context.DeadlineExceeded
			}
			return nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		defer plugin.Cleanup()
		ctx := cacheAvailabilityContext()
		defer ctx.Cancel()
		req := cacheAvailabilityRequest()
		got, hit, err := plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || !plugin.namespaceUnavailable.Load() {
			t.Fatalf("unavailable namespace changed provider path: got=%p hit=%v error=%v", got, hit, err)
		}
		requestID := ctx.Value(schemas.BifrostContextKeyRequestID).(string)
		if plugin.getCacheState(requestID) != nil {
			t.Fatal("unavailable cache retained pending writes")
		}
		available.Store(true)
		time.Sleep(time.Second)
		synctest.Wait()
		if plugin.namespaceUnavailable.Load() || namespaceCalls.Load() != 2 {
			t.Fatal("namespace did not recover once without restart")
		}
		got, hit, err = plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || plugin.getCacheState(requestID) == nil {
			t.Fatal("recovered namespace did not resume cache lookup")
		}
		response := &schemas.BifrostResponse{ChatResponse: &schemas.BifrostChatResponse{
			ID: "synthetic-response", Model: "synthetic-model", Object: "chat.completion",
			ExtraFields: schemas.BifrostResponseExtraFields{RequestType: schemas.ChatCompletionRequest, Provider: schemas.OpenAI, OriginalModelRequested: "synthetic-model"},
		}}
		if _, _, err := plugin.PostLLMHook(ctx, response, nil); err != nil {
			t.Fatal(err)
		}
		plugin.WaitForPendingOperations()
		replayCtx := cacheAvailabilityContext()
		defer replayCtx.Cancel()
		_, replay, err := plugin.PreLLMHook(replayCtx, req)
		if err != nil || replay == nil {
			t.Fatalf("recovered cache did not replay saved provider response: %v", err)
		}
		metadata, ok := schemas.CacheLookupFromContext(replayCtx)
		if !ok || !metadata.CacheHit {
			t.Fatal("cache hit did not reach the original request context")
		}
	})
}

func TestCacheAvailabilityRejectsKnownSchemaConflict(t *testing.T) {
	store := &cacheAvailabilityStore{observableStore: newObservableStore(), create: func(context.Context) error { return errors.New("namespace has an incompatible dimension") }}
	plugin, err := Init(context.Background(), &Config{Dimension: 1}, bifrost.NewDefaultLogger(schemas.LogLevelError), store)
	if err == nil || plugin != nil {
		t.Fatal("known reachable schema conflict was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	store.create = func(ctx context.Context) error { return ctx.Err() }
	plugin, err = Init(canceled, &Config{Dimension: 1}, bifrost.NewDefaultLogger(schemas.LogLevelError), store)
	if err == nil || plugin != nil {
		t.Fatal("canceled initialization started maintenance")
	}
}

func TestCacheAvailabilityHangingStartupAndCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int32
		var active atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(ctx context.Context) error {
			attempts.Add(1)
			if active.Add(1) != 1 {
				t.Error("namespace recovery overlapped")
			}
			defer active.Add(-1)
			<-ctx.Done()
			return ctx.Err()
		}
		start := time.Now()
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		if elapsed := time.Since(start); elapsed != CreateNamespaceTimeout {
			t.Fatalf("namespace initialization deadline=%v", elapsed)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if attempts.Load() != 2 || active.Load() != 1 {
			t.Fatal("serial recovery did not start")
		}
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		if err := plugin.Cleanup(); err != nil {
			t.Fatal(err)
		}
		if active.Load() != 0 {
			t.Fatal("cleanup left an active namespace RPC")
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if attempts.Load() != 2 {
			t.Fatal("namespace recovery survived Cleanup")
		}
	})
}

func TestCacheAvailabilityCallerCancellationPreservesNamespace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var embeddings atomic.Int32
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore(), create: func(context.Context) error { namespaceCalls.Add(1); return nil }}
		store.get = func(ctx context.Context, _ string) (vectorstore.SearchResult, error) {
			<-ctx.Done()
			return vectorstore.SearchResult{}, ctx.Err()
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
		defer plugin.Cleanup()
		plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
			embeddings.Add(1)
			return nil, nil
		})
		parent, cancelParent := context.WithCancel(context.Background())
		defer cancelParent()
		ctx := withTestRequestID(schemas.NewBifrostContextWithValue(parent, schemas.NoDeadline, CacheKey, "synthetic-cache-key"))
		defer ctx.Cancel()
		go func() { time.Sleep(10 * time.Millisecond); cancelParent() }()
		req := cacheAvailabilityRequest()
		start := time.Now()
		got, hit, err := plugin.PreLLMHook(ctx, req)
		if elapsed := time.Since(start); elapsed != 10*time.Millisecond {
			t.Fatalf("caller cancellation took %v", elapsed)
		}
		if err != nil || got != req || hit != nil || ctx.Err() != context.Canceled {
			t.Fatal("cache lookup did not preserve caller cancellation")
		}
		if embeddings.Load() != 0 {
			t.Fatal("canceled direct lookup still dispatched embedding")
		}
		if plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string)) != nil {
			t.Fatal("canceled lookup left a cache writer")
		}
		synctest.Wait()
		if namespaceCalls.Load() != 1 || plugin.namespaceUnavailable.Load() {
			t.Fatal("caller cancellation invalidated the ready namespace")
		}
	})
}

func TestCacheAvailabilityEmbeddingKeepsCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deadline := time.Now().Add(time.Second)
		store := &cacheAvailabilityStore{observableStore: newObservableStore(), create: func(context.Context) error { return nil }}
		nearestCalled := false
		store.nearest = func(ctx context.Context) ([]vectorstore.SearchResult, error) {
			nearestCalled = true
			if got, ok := ctx.Deadline(); !ok || !got.Equal(time.Now().Add(cacheStoreLookupTimeout)) || ctx.Err() != nil {
				t.Error("semantic store lookup did not receive a fresh availability budget")
			}
			time.Sleep(100 * time.Millisecond)
			if ctx.Err() != nil {
				t.Error("embedding time exhausted the following store lookup")
			}
			return nil, nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
		defer plugin.Cleanup()
		plugin.SetEmbeddingRequestExecutor(func(ctx *schemas.BifrostContext, _ *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
			if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
				t.Error("embedding received a shorter deadline than its caller")
			}
			timer := time.NewTimer(350 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, &schemas.BifrostError{Error: &schemas.ErrorField{Message: ctx.Err().Error()}}
			case <-timer.C:
				return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{0.1, 0.2, 0.3, 0.4}}}}}, nil
			}
		})
		ctx := withTestRequestID(schemas.NewBifrostContextWithValue(context.Background(), deadline, CacheKey, "synthetic-cache-key"))
		defer ctx.Cancel()
		req := cacheAvailabilityRequest()
		start := time.Now()
		got, hit, err := plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || ctx.Err() != nil || !nearestCalled {
			t.Fatal("valid embedding did not reach semantic search within the caller deadline")
		}
		if elapsed := time.Since(start); elapsed != 450*time.Millisecond {
			t.Fatalf("semantic search ended after %v", elapsed)
		}
		state := plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string))
		if state == nil || state.PlaceholderEmbeddings || len(state.Embeddings) != 4 {
			t.Fatal("completed embedding was not retained for cache storage")
		}
	})
}

func TestCacheAvailabilityStoreReadDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name           string
		semantic       bool
		callerDeadline bool
	}{
		{"direct store timeout", false, false},
		{"semantic store timeout after embedding", true, false},
		{"direct earlier caller deadline", false, true},
		{"semantic earlier caller deadline", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var namespaceCalls, embeddings atomic.Int32
				store := &cacheAvailabilityStore{observableStore: newObservableStore()}
				store.create = func(ctx context.Context) error {
					if namespaceCalls.Add(1) > 1 {
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				}
				store.get = func(ctx context.Context, _ string) (vectorstore.SearchResult, error) {
					<-ctx.Done()
					return vectorstore.SearchResult{}, ctx.Err()
				}
				store.nearest = func(ctx context.Context) ([]vectorstore.SearchResult, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
				defer plugin.Cleanup()
				plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
					embeddings.Add(1)
					time.Sleep(350 * time.Millisecond)
					return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{1, 0, 0, 0}}}}}, nil
				})
				elapsed := cacheStoreLookupTimeout
				if tc.callerDeadline {
					elapsed = 50 * time.Millisecond
				}
				if tc.semantic {
					elapsed += 350 * time.Millisecond
				}
				deadline := time.Now().Add(2 * time.Second)
				if tc.callerDeadline {
					deadline = time.Now().Add(elapsed)
				}
				ctx := withTestRequestID(schemas.NewBifrostContextWithValue(context.Background(), deadline, CacheKey, "synthetic-cache-key"))
				defer ctx.Cancel()
				if tc.semantic {
					ctx.SetValue(CacheTypeKey, CacheTypeSemantic)
				}
				req := cacheAvailabilityRequest()
				start := time.Now()
				got, hit, err := plugin.PreLLMHook(ctx, req)
				if got != req || hit != nil || err != nil || time.Since(start) != elapsed {
					t.Fatalf("blocked store did not return the provider request after %v: %v", elapsed, err)
				}
				if tc.callerDeadline {
					if ctx.Err() != context.DeadlineExceeded {
						t.Fatal("store lookup lost the earlier caller deadline")
					}
				} else if ctx.Err() != nil {
					t.Fatal("store timeout canceled the provider request")
				}
				if (embeddings.Load() == 1) != tc.semantic {
					t.Fatal("failed direct lookup dispatched an embedding, or semantic lookup skipped it")
				}
				if plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string)) != nil {
					t.Fatal("failed store read left a cache writer")
				}
				synctest.Wait()
				if tc.callerDeadline {
					if namespaceCalls.Load() != 1 || plugin.namespaceUnavailable.Load() {
						t.Fatal("caller deadline invalidated a ready namespace")
					}
				} else if namespaceCalls.Load() != 2 || !plugin.namespaceUnavailable.Load() {
					t.Fatal("unresponsive store did not start namespace recovery")
				}
			})
		})
	}
}

func TestCacheAvailabilityNamespaceLossAfterHitAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var lost atomic.Bool
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(context.Context) error {
			namespaceCalls.Add(1)
			lost.Store(false)
			return nil
		}
		store.get = func(ctx context.Context, id string) (vectorstore.SearchResult, error) {
			if lost.Load() {
				return vectorstore.SearchResult{}, vectorstore.ErrNamespaceNotFound
			}
			return store.observableStore.GetChunk(ctx, "", id)
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		defer plugin.Cleanup()
		req := cacheAvailabilityRequest()
		writeCtx := cacheAvailabilityContext()
		defer writeCtx.Cancel()
		writeThroughCache(t, plugin, writeCtx, req)
		hitCtx := cacheAvailabilityContext()
		defer hitCtx.Cancel()
		if !cacheHits(t, plugin, hitCtx, req) {
			t.Fatal("initial saved response did not hit")
		}
		store.mu.Lock()
		clear(store.chunks)
		store.mu.Unlock()
		lost.Store(true)
		failedCtx := cacheAvailabilityContext()
		defer failedCtx.Cancel()
		got, hit, err := plugin.PreLLMHook(failedCtx, req)
		if err != nil || got != req || hit != nil || failedCtx.Err() != nil {
			t.Fatal("namespace loss changed the provider request")
		}
		if plugin.getCacheState(failedCtx.Value(schemas.BifrostContextKeyRequestID).(string)) != nil {
			t.Fatal("namespace loss was treated as a normal point miss")
		}
		synctest.Wait()
		if plugin.namespaceUnavailable.Load() || namespaceCalls.Load() != 2 {
			t.Fatal("lost namespace was not recreated without restart")
		}
		recoveredCtx := cacheAvailabilityContext()
		defer recoveredCtx.Cancel()
		writeThroughCache(t, plugin, recoveredCtx, req)
		replayCtx := cacheAvailabilityContext()
		defer replayCtx.Cancel()
		if !cacheHits(t, plugin, replayCtx, req) {
			t.Fatal("fresh namespace did not resume writes and hits")
		}
	})
}

func TestCacheAvailabilitySemanticOnlyNamespaceRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var lost atomic.Bool
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(context.Context) error {
			namespaceCalls.Add(1)
			lost.Store(false)
			return nil
		}
		store.nearest = func(context.Context) ([]vectorstore.SearchResult, error) {
			if lost.Load() {
				return nil, vectorstore.ErrNamespaceNotFound
			}
			return nil, nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
		defer plugin.Cleanup()
		plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
			return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{1, 0, 0, 0}}}}}, nil
		})
		lost.Store(true)
		ctx := cacheAvailabilityContext()
		defer ctx.Cancel()
		ctx.SetValue(CacheTypeKey, CacheTypeSemantic)
		req := cacheAvailabilityRequest()
		got, hit, err := plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || ctx.Err() != nil {
			t.Fatal("semantic store failure damaged provider continuation")
		}
		if plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string)) != nil {
			t.Fatal("failed semantic lookup retained an async write")
		}
		synctest.Wait()
		if namespaceCalls.Load() != 2 || plugin.namespaceUnavailable.Load() {
			t.Fatal("semantic-only lookup did not repair its namespace")
		}
		got, hit, err = plugin.PreLLMHook(ctx, req)
		if err != nil || got != req || hit != nil || plugin.getCacheState(ctx.Value(schemas.BifrostContextKeyRequestID).(string)) == nil {
			t.Fatal("semantic-only cache did not resume after recovery")
		}
	})
}

func TestCacheAvailabilityWriteNamespaceLossRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var lost atomic.Bool
		var namespaceCalls atomic.Int32
		store := &cacheAvailabilityStore{observableStore: newObservableStore()}
		store.create = func(context.Context) error {
			namespaceCalls.Add(1)
			lost.Store(false)
			return nil
		}
		store.add = func(context.Context) error {
			if lost.Load() {
				return vectorstore.ErrNamespaceNotFound
			}
			return nil
		}
		plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
		defer plugin.Cleanup()
		ctx := cacheAvailabilityContext()
		defer ctx.Cancel()
		req := cacheAvailabilityRequest()
		if got, hit, err := plugin.PreLLMHook(ctx, req); err != nil || got != req || hit != nil {
			t.Fatal("initial lookup failed")
		}
		lost.Store(true)
		response := identityTestChatResponse("provider response")
		if got, providerErr, err := plugin.PostLLMHook(ctx, response, nil); got != response || providerErr != nil || err != nil {
			t.Fatal("failed cache write changed the provider response")
		}
		plugin.WaitForPendingOperations()
		synctest.Wait()
		if namespaceCalls.Load() != 2 || plugin.namespaceUnavailable.Load() {
			t.Fatal("namespace loss between lookup and write did not recover")
		}
		recoveredCtx := cacheAvailabilityContext()
		defer recoveredCtx.Cancel()
		writeThroughCache(t, plugin, recoveredCtx, req)
		replayCtx := cacheAvailabilityContext()
		defer replayCtx.Cancel()
		if !cacheHits(t, plugin, replayCtx, req) {
			t.Fatal("cache writes did not resume after async failure")
		}
	})
}

func TestCacheAvailabilityAccessFailureIsNotQuietPointMiss(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument} {
		t.Run(code.String(), func(t *testing.T) {
			store := &cacheAvailabilityStore{
				observableStore: newObservableStore(),
				create:          func(context.Context) error { return nil },
				get: func(context.Context, string) (vectorstore.SearchResult, error) {
					return vectorstore.SearchResult{}, status.Error(code, "subject not found")
				},
			}
			plugin := cacheAvailabilityInit(t, store, &Config{Dimension: 1})
			defer plugin.Cleanup()
			ctx := scopedTestContext(t, "")
			defer ctx.Cancel()
			ctx.SetValue(CacheNoStoreKey, true)
			req := cacheAvailabilityRequest()
			got, hit, err := plugin.PreLLMHook(ctx, req)
			if err != nil || got != req || hit != nil || ctx.Err() != nil {
				t.Fatal("cache access failure damaged the provider request")
			}
			if !pluginLogContains(ctx.GetPluginLogs(), schemas.LogLevelWarn, "subject not found") {
				t.Fatal("known service failure disappeared as a quiet point miss")
			}
			if plugin.namespaceUnavailable.Load() {
				t.Fatal("known access/schema error started transport recovery")
			}
			response := identityTestChatResponse("provider response")
			if got, providerErr, err := plugin.PostLLMHook(ctx, response, nil); got != response || providerErr != nil || err != nil {
				t.Fatal("cache access failure changed the provider response")
			}
			plugin.WaitForPendingOperations()
			store.mu.Lock()
			writes := len(store.addIDs)
			store.mu.Unlock()
			if writes != 0 {
				t.Fatal("no-store request wrote a cache entry")
			}
		})
	}
}

func TestCacheAvailabilityLookupFailureKeepsUsableWrite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cacheType CacheType
		direct    bool
		corrupt   bool
	}{
		{"direct unsupported", CacheTypeDirect, true, false},
		{"direct corrupt response", CacheTypeDirect, true, true},
		{"semantic unsupported", "", false, false},
		{"semantic-only unsupported", CacheTypeSemantic, false, false},
		{"semantic-only corrupt response", CacheTypeSemantic, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var failLookup atomic.Bool
			failLookup.Store(true)
			var namespaceCalls atomic.Int32
			store := &cacheAvailabilityStore{observableStore: newObservableStore()}
			store.create = func(context.Context) error {
				namespaceCalls.Add(1)
				return nil
			}
			failedResult := func(id string) (vectorstore.SearchResult, error) {
				if tc.corrupt {
					return vectorstore.SearchResult{ID: id, Properties: map[string]interface{}{"response": "{"}}, nil
				}
				return vectorstore.SearchResult{}, vectorstore.ErrNotSupported
			}
			store.get = func(ctx context.Context, id string) (vectorstore.SearchResult, error) {
				if tc.direct && failLookup.Load() {
					return failedResult(id)
				}
				return store.observableStore.GetChunk(ctx, "", id)
			}
			store.nearest = func(context.Context) ([]vectorstore.SearchResult, error) {
				if !tc.direct && failLookup.Load() {
					result, err := failedResult("synthetic-corrupt-entry")
					return []vectorstore.SearchResult{result}, err
				}
				return nil, nil
			}
			plugin := cacheAvailabilityInit(t, store, &Config{Provider: schemas.OpenAI, EmbeddingModel: "synthetic-embedding", Dimension: 4})
			defer plugin.Cleanup()
			plugin.SetEmbeddingRequestExecutor(func(*schemas.BifrostContext, *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
				return &schemas.BifrostEmbeddingResponse{Data: []schemas.EmbeddingData{{Embedding: schemas.EmbeddingStruct{EmbeddingArray: []float64{1, 0, 0, 0}}}}}, nil
			})
			ctx := scopedTestContext(t, "")
			defer ctx.Cancel()
			if tc.cacheType != "" {
				ctx.SetValue(CacheTypeKey, tc.cacheType)
			}
			req := cacheAvailabilityRequest()
			got, hit, err := plugin.PreLLMHook(ctx, req)
			if err != nil || got != req || hit != nil || ctx.Err() != nil {
				t.Fatal("failed lookup changed the provider request")
			}
			if !pluginLogContains(ctx.GetPluginLogs(), schemas.LogLevelWarn, "failed") && !pluginLogContains(ctx.GetPluginLogs(), schemas.LogLevelWarn, "semantic search skipped") {
				t.Fatal("lookup failure was not visible as a warning")
			}
			response := identityTestChatResponse("synthetic-provider-answer")
			if got, providerErr, err := plugin.PostLLMHook(ctx, response, nil); got != response || providerErr != nil || err != nil {
				t.Fatal("cache write changed the provider response")
			}
			plugin.WaitForPendingOperations()
			store.mu.Lock()
			writes := len(store.addIDs)
			var written vectorstore.SearchResult
			var embedding []float32
			if writes == 1 {
				written = store.chunks[store.addIDs[0]]
				embedding = store.addEmbeddings[0]
			}
			store.mu.Unlock()
			if writes != 1 {
				t.Fatalf("usable response was not cached after lookup failure: %d writes", writes)
			}
			if !tc.direct && !slices.Equal(embedding, []float32{1, 0, 0, 0}) {
				t.Fatalf("successful embedding was lost before write: %v", embedding)
			}
			payload, ok := written.Properties["response"].(string)
			if !ok || !strings.Contains(payload, "synthetic-provider-answer") {
				t.Fatal("write did not preserve the provider response")
			}
			if namespaceCalls.Load() != 1 || plugin.namespaceUnavailable.Load() {
				t.Fatal("individual lookup failure started namespace recovery")
			}
			// Direct entries, including an unreadable former value, are replaced
			// under the same deterministic ID and can serve the next request.
			if tc.cacheType != CacheTypeSemantic {
				failLookup.Store(false)
				replayCtx := scopedTestContext(t, "")
				defer replayCtx.Cancel()
				if tc.cacheType != "" {
					replayCtx.SetValue(CacheTypeKey, tc.cacheType)
				}
				if !cacheHits(t, plugin, replayCtx, req) {
					t.Fatal("provider response did not repair the direct cache entry")
				}
			}
		})
	}
}
