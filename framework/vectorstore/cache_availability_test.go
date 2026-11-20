package vectorstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// These tests use the real Qdrant client and protobuf transport. No external
// service or environment variables are needed, including in short test runs.
type cacheAvailabilityQdrantStub struct {
	qdrant.UnimplementedCollectionsServer
	mu                  sync.Mutex
	exists              bool
	omitExistenceResult bool
	existenceErr        error
	info                *qdrant.CollectionInfo
	infoErr             error
	healthErr           error
	indexErr            error
	pointErr            error
	loseBeforeInfo      bool
	loseBeforeIndex     bool
	methods             []string
	indexes             []*qdrant.CreateFieldIndexCollection
	points              []*qdrant.RetrievedPoint
}

func (s *cacheAvailabilityQdrantStub) CollectionExists(context.Context, *qdrant.CollectionExistsRequest) (*qdrant.CollectionExistsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.existenceErr != nil {
		return nil, s.existenceErr
	}
	if s.omitExistenceResult {
		return &qdrant.CollectionExistsResponse{}, nil
	}
	return &qdrant.CollectionExistsResponse{Result: &qdrant.CollectionExists{Exists: s.exists}}, nil
}

func (s *cacheAvailabilityQdrantStub) Get(context.Context, *qdrant.GetCollectionInfoRequest) (*qdrant.GetCollectionInfoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loseBeforeInfo {
		s.loseBeforeInfo = false
		s.exists, s.info, s.points = false, nil, nil
		return nil, status.Error(codes.NotFound, "collection disappeared before inspection")
	}
	if s.infoErr != nil {
		return nil, s.infoErr
	}
	if s.info == nil {
		return &qdrant.GetCollectionInfoResponse{}, nil
	}
	return &qdrant.GetCollectionInfoResponse{Result: proto.Clone(s.info).(*qdrant.CollectionInfo)}, nil
}

func (s *cacheAvailabilityQdrantStub) Create(_ context.Context, request *qdrant.CreateCollection) (*qdrant.CollectionOperationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exists = true
	s.info = &qdrant.CollectionInfo{
		Config:        &qdrant.CollectionConfig{Params: &qdrant.CollectionParams{VectorsConfig: proto.Clone(request.VectorsConfig).(*qdrant.VectorsConfig)}},
		PayloadSchema: make(map[string]*qdrant.PayloadSchemaInfo),
	}
	return &qdrant.CollectionOperationResponse{Result: true}, nil
}

type cacheAvailabilityQdrantHealthStub struct {
	qdrant.UnimplementedQdrantServer
	stub *cacheAvailabilityQdrantStub
}

func (s *cacheAvailabilityQdrantHealthStub) HealthCheck(context.Context, *qdrant.HealthCheckRequest) (*qdrant.HealthCheckReply, error) {
	if s.stub.healthErr != nil {
		return nil, s.stub.healthErr
	}
	return &qdrant.HealthCheckReply{Version: "1.16.2"}, nil
}

type cacheAvailabilityQdrantPointsStub struct {
	qdrant.UnimplementedPointsServer
	stub *cacheAvailabilityQdrantStub
}

func (s *cacheAvailabilityQdrantPointsStub) CreateFieldIndex(_ context.Context, request *qdrant.CreateFieldIndexCollection) (*qdrant.PointsOperationResponse, error) {
	s.stub.mu.Lock()
	defer s.stub.mu.Unlock()
	s.stub.indexes = append(s.stub.indexes, proto.Clone(request).(*qdrant.CreateFieldIndexCollection))
	if s.stub.loseBeforeIndex {
		s.stub.loseBeforeIndex = false
		s.stub.exists, s.stub.info, s.stub.points = false, nil, nil
		return nil, status.Error(codes.NotFound, "collection disappeared before index creation")
	}
	if s.stub.indexErr != nil {
		return nil, s.stub.indexErr
	}
	if !s.stub.exists {
		return nil, status.Error(codes.NotFound, "collection does not exist")
	}
	// Model the schema effect of creating an index, so repeated initialization
	// observes the new index and cannot silently recreate the old ones.
	if s.stub.info.PayloadSchema == nil {
		s.stub.info.PayloadSchema = make(map[string]*qdrant.PayloadSchemaInfo)
	}
	var dataType qdrant.PayloadSchemaType
	switch request.GetFieldType() {
	case qdrant.FieldType_FieldTypeInteger:
		dataType = qdrant.PayloadSchemaType_Integer
	case qdrant.FieldType_FieldTypeBool:
		dataType = qdrant.PayloadSchemaType_Bool
	case qdrant.FieldType_FieldTypeKeyword:
		dataType = qdrant.PayloadSchemaType_Keyword
	default:
		return nil, status.Error(codes.InvalidArgument, "unexpected index type")
	}
	s.stub.info.PayloadSchema[request.FieldName] = &qdrant.PayloadSchemaInfo{DataType: dataType}
	return &qdrant.PointsOperationResponse{Result: &qdrant.UpdateResult{Status: qdrant.UpdateStatus_Completed}}, nil
}

func (s *cacheAvailabilityQdrantPointsStub) operationError() error {
	if s.stub.pointErr != nil {
		return s.stub.pointErr
	}
	if !s.stub.exists {
		return status.Error(codes.NotFound, "collection does not exist")
	}
	return nil
}

func (s *cacheAvailabilityQdrantPointsStub) Get(_ context.Context, request *qdrant.GetPoints) (*qdrant.GetResponse, error) {
	s.stub.mu.Lock()
	defer s.stub.mu.Unlock()
	if err := s.operationError(); err != nil {
		return nil, err
	}
	var result []*qdrant.RetrievedPoint
	for _, point := range s.stub.points {
		for _, id := range request.Ids {
			if proto.Equal(point.Id, id) {
				result = append(result, proto.Clone(point).(*qdrant.RetrievedPoint))
				break
			}
		}
	}
	return &qdrant.GetResponse{Result: result}, nil
}

func (s *cacheAvailabilityQdrantPointsStub) Query(context.Context, *qdrant.QueryPoints) (*qdrant.QueryResponse, error) {
	s.stub.mu.Lock()
	defer s.stub.mu.Unlock()
	if err := s.operationError(); err != nil {
		return nil, err
	}
	result := make([]*qdrant.ScoredPoint, 0, len(s.stub.points))
	for _, point := range s.stub.points {
		result = append(result, &qdrant.ScoredPoint{Id: point.Id, Payload: point.Payload, Score: 1})
	}
	return &qdrant.QueryResponse{Result: result}, nil
}

func (s *cacheAvailabilityQdrantPointsStub) Upsert(_ context.Context, request *qdrant.UpsertPoints) (*qdrant.PointsOperationResponse, error) {
	s.stub.mu.Lock()
	defer s.stub.mu.Unlock()
	if err := s.operationError(); err != nil {
		return nil, err
	}
	for _, point := range request.Points {
		s.stub.points = append(s.stub.points, &qdrant.RetrievedPoint{Id: point.Id, Payload: point.Payload})
	}
	return &qdrant.PointsOperationResponse{Result: &qdrant.UpdateResult{Status: qdrant.UpdateStatus_Completed}}, nil
}

func (s *cacheAvailabilityQdrantStub) methodCalls(method string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	for _, called := range s.methods {
		if called == method {
			count++
		}
	}
	return count
}

func (s *cacheAvailabilityQdrantStub) snapshot() (*qdrant.CollectionInfo, []*qdrant.CreateFieldIndexCollection, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var info *qdrant.CollectionInfo
	if s.info != nil {
		info = proto.Clone(s.info).(*qdrant.CollectionInfo)
	}
	return info, append([]*qdrant.CreateFieldIndexCollection(nil), s.indexes...), append([]string(nil), s.methods...)
}

func (s *cacheAvailabilityQdrantStub) assertNoMutations(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, method := range s.methods {
		switch method {
		case qdrant.Collections_CollectionExists_FullMethodName, qdrant.Collections_Get_FullMethodName:
		default:
			t.Errorf("namespace inspection unexpectedly called %s", method)
		}
	}
}

func newCacheAvailabilityQdrant(t *testing.T, stub *cacheAvailabilityQdrantStub) (*QdrantStore, context.Context) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		stub.mu.Lock()
		stub.methods = append(stub.methods, info.FullMethod)
		stub.mu.Unlock()
		return handler(ctx, req)
	}))
	qdrant.RegisterCollectionsServer(server, stub)
	qdrant.RegisterPointsServer(server, &cacheAvailabilityQdrantPointsStub{stub: stub})
	qdrant.RegisterQdrantServer(server, &cacheAvailabilityQdrantHealthStub{stub: stub})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if serveErr := server.Serve(listener); serveErr != nil {
			t.Errorf("Qdrant test server: %v", serveErr)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		<-done
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	config := &QdrantConfig{
		Host: *schemas.NewSecretVar("127.0.0.1"),
		Port: *schemas.NewSecretVar(strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)),
	}
	store, err := newQdrantStore(ctx, config, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close(context.Background(), "")) })
	return store, ctx
}

func cacheAvailabilityQdrantInfo(vectors *qdrant.VectorsConfig) *qdrant.CollectionInfo {
	return &qdrant.CollectionInfo{
		Config: &qdrant.CollectionConfig{Params: &qdrant.CollectionParams{VectorsConfig: vectors}},
		PayloadSchema: map[string]*qdrant.PayloadSchemaInfo{
			"cache_key": {
				DataType: qdrant.PayloadSchemaType_Keyword,
				Params: &qdrant.PayloadIndexParams{IndexParams: &qdrant.PayloadIndexParams_KeywordIndexParams{
					KeywordIndexParams: &qdrant.KeywordIndexParams{OnDisk: qdrant.PtrOf(true), IsTenant: qdrant.PtrOf(true)},
				}},
			},
			"expires_at": {DataType: qdrant.PayloadSchemaType_Integer},
			"owned":      {DataType: qdrant.PayloadSchemaType_Bool},
			"response":   {DataType: qdrant.PayloadSchemaType_Keyword}, // Legacy operator-managed index.
		},
	}
}

func cacheAvailabilityQdrantProperties() map[string]VectorStoreProperties {
	return map[string]VectorStoreProperties{
		"cache_key":  {DataType: VectorStorePropertyTypeString, Filterable: true},
		"expires_at": {DataType: VectorStorePropertyTypeInteger, Filterable: true},
		"owned":      {DataType: VectorStorePropertyTypeBoolean, Filterable: true},
		"response":   {DataType: VectorStorePropertyTypeString},
	}
}

func TestCacheAvailabilityQdrantConstructorDoesNotRequireReachableServer(t *testing.T) {
	stub := &cacheAvailabilityQdrantStub{healthErr: status.Error(codes.Unavailable, "Qdrant is unavailable")}
	store, ctx := newCacheAvailabilityQdrant(t, stub)
	assert.Zero(t, stub.methodCalls(qdrant.Qdrant_HealthCheck_FullMethodName), "constructing an optional store must perform no health RPC")
	require.ErrorContains(t, store.Ping(ctx), "Qdrant is unavailable")
	assert.Equal(t, 1, stub.methodCalls(qdrant.Qdrant_HealthCheck_FullMethodName), "explicit Ping must still probe the service")

	// Even a canceled startup context and an endpoint without a running server
	// must allow local client setup. No timing threshold or DNS is involved.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	unavailable, err := newQdrantStore(canceled, &QdrantConfig{
		Host: *schemas.NewSecretVar("127.0.0.1"), Port: *schemas.NewSecretVar(strconv.Itoa(port)),
	}, nil)
	require.NoError(t, err)
	require.NoError(t, unavailable.Close(context.Background(), ""))
}

func TestCacheAvailabilityQdrantConstructorValidatesConfig(t *testing.T) {
	for _, config := range []*QdrantConfig{nil, {}, {Host: *schemas.NewSecretVar(" \t ")}} {
		store, err := newQdrantStore(context.Background(), config, nil)
		require.Error(t, err)
		assert.Nil(t, store)
	}
}

func TestCacheAvailabilityQdrantNamespaceRejectsUnavailableLookup(t *testing.T) {
	tests := []struct {
		name        string
		stub        *cacheAvailabilityQdrantStub
		unavailable bool
	}{
		{"existence unavailable", &cacheAvailabilityQdrantStub{existenceErr: status.Error(codes.Unavailable, "lookup unavailable")}, true},
		{"existence unknown", &cacheAvailabilityQdrantStub{omitExistenceResult: true}, false},
		{"schema unavailable", &cacheAvailabilityQdrantStub{exists: true, infoErr: status.Error(codes.Unavailable, "schema unavailable")}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, ctx := newCacheAvailabilityQdrant(t, tt.stub)
			err := store.CreateNamespace(ctx, "cache", 2, cacheAvailabilityQdrantProperties())
			require.Error(t, err)
			assert.Equal(t, tt.unavailable, IsUnavailable(err), "classification must survive the real SDK and adapter wrappers")
			tt.stub.assertNoMutations(t)
		})
	}
}

func TestCacheAvailabilityQdrantNamespaceRejectsNonPositiveDimension(t *testing.T) {
	store := &QdrantStore{} // Invalid dimensions must fail before using a client.
	for _, dimension := range []int{0, -1} {
		require.ErrorContains(t, store.CreateNamespace(context.Background(), "cache", dimension, nil), "dimension must be positive")
	}
}

func TestCacheAvailabilityQdrantNamespaceRejectsIncompatibleVectors(t *testing.T) {
	tests := []struct {
		name string
		info *qdrant.CollectionInfo
	}{
		{"dimension mismatch", cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 3, Distance: qdrant.Distance_Cosine}))},
		{"named vectors", cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{"embedding": {Size: 2, Distance: qdrant.Distance_Cosine}}))},
		{"missing info", nil},
		{"missing config", &qdrant.CollectionInfo{}},
		{"missing collection params", &qdrant.CollectionInfo{Config: &qdrant.CollectionConfig{}}},
		{"missing vectors", cacheAvailabilityQdrantInfo(nil)},
		{"unknown vectors", cacheAvailabilityQdrantInfo(&qdrant.VectorsConfig{})},
		{"unknown dimension", cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{}))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &cacheAvailabilityQdrantStub{exists: true, info: tt.info}
			store, ctx := newCacheAvailabilityQdrant(t, stub)
			err := store.CreateNamespace(ctx, "cache", 2, cacheAvailabilityQdrantProperties())
			require.Error(t, err)
			assert.False(t, IsUnavailable(err), "a reachable incompatible namespace must fail initialization")
			stub.assertNoMutations(t)
		})
	}
}

func TestCacheAvailabilityQdrantNamespaceRejectsIncompatibleIndexesWithoutMutation(t *testing.T) {
	for _, dataType := range []qdrant.PayloadSchemaType{qdrant.PayloadSchemaType_Text, qdrant.PayloadSchemaType_UnknownType} {
		t.Run(dataType.String(), func(t *testing.T) {
			info := cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: qdrant.Distance_Cosine}))
			info.PayloadSchema["cache_key"].DataType = dataType
			before := proto.Clone(info)
			stub := &cacheAvailabilityQdrantStub{exists: true, info: info}
			store, ctx := newCacheAvailabilityQdrant(t, stub)
			properties := cacheAvailabilityQdrantProperties()
			for i := range 8 {
				properties[fmt.Sprintf("missing_%d", i)] = VectorStoreProperties{DataType: VectorStorePropertyTypeBoolean, Filterable: true}
			}
			err := store.CreateNamespace(ctx, "cache", 2, properties)
			require.ErrorContains(t, err, "expected Keyword")
			assert.False(t, IsUnavailable(err))
			stub.assertNoMutations(t)
			after, _, _ := stub.snapshot()
			assert.True(t, proto.Equal(before, after), "incompatible schema must survive untouched")
		})
	}
}

func TestCacheAvailabilityQdrantNamespaceRejectsNonCosineDistance(t *testing.T) {
	for _, distance := range []qdrant.Distance{qdrant.Distance_Dot, qdrant.Distance_Euclid, qdrant.Distance_Manhattan} {
		t.Run(distance.String(), func(t *testing.T) {
			info := cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: distance}))
			before := proto.Clone(info)
			stub := &cacheAvailabilityQdrantStub{exists: true, info: info}
			store, ctx := newCacheAvailabilityQdrant(t, stub)
			properties := cacheAvailabilityQdrantProperties()
			properties["missing_filter"] = VectorStoreProperties{DataType: VectorStorePropertyTypeBoolean, Filterable: true}
			err := store.CreateNamespace(ctx, "cache", 2, properties)
			require.ErrorContains(t, err, "expected Cosine")
			assert.False(t, IsUnavailable(err))
			stub.assertNoMutations(t)
			after, _, _ := stub.snapshot()
			assert.True(t, proto.Equal(before, after))
		})
	}
}

func TestCacheAvailabilityQdrantNamespacePreservesCompatibleCollectionAndData(t *testing.T) {
	info := cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: qdrant.Distance_Cosine}))
	before := proto.Clone(info)
	const id = "550e8400-e29b-41d4-a716-446655440000"
	stub := &cacheAvailabilityQdrantStub{exists: true, info: info, points: []*qdrant.RetrievedPoint{{
		Id: qdrant.NewID(id), Payload: qdrant.NewValueMap(map[string]interface{}{"response": "preserved response"}),
	}}}
	store, ctx := newCacheAvailabilityQdrant(t, stub)
	for range 2 {
		require.NoError(t, store.CreateNamespace(ctx, "cache", 2, cacheAvailabilityQdrantProperties()))
	}
	stub.assertNoMutations(t)
	after, _, _ := stub.snapshot()
	assert.True(t, proto.Equal(before, after), "custom index settings and legacy indexes must be preserved")
	require.NoError(t, store.Ping(ctx))
	point, err := store.GetChunk(ctx, "cache", id)
	require.NoError(t, err)
	assert.Equal(t, "preserved response", point.Properties["response"])
}

func TestCacheAvailabilityQdrantNamespaceCreatesOnlyMissingIndexes(t *testing.T) {
	info := cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: qdrant.Distance_Cosine}))
	customIndex := proto.Clone(info.PayloadSchema["cache_key"])
	stub := &cacheAvailabilityQdrantStub{exists: true, info: info}
	store, ctx := newCacheAvailabilityQdrant(t, stub)
	properties := cacheAvailabilityQdrantProperties()
	properties["new_filter"] = VectorStoreProperties{DataType: VectorStorePropertyTypeStringArray, Filterable: true}
	for range 2 {
		require.NoError(t, store.CreateNamespace(ctx, "cache", 2, properties))
	}
	after, indexes, methods := stub.snapshot()
	require.Len(t, indexes, 1)
	assert.Equal(t, "new_filter", indexes[0].FieldName)
	assert.Equal(t, qdrant.FieldType_FieldTypeKeyword, indexes[0].GetFieldType())
	assert.True(t, indexes[0].GetWait())
	assert.True(t, proto.Equal(customIndex, after.PayloadSchema["cache_key"]))
	for _, method := range methods {
		assert.False(t, strings.Contains(method, "Delete"), "initialization must not drop any collection, index, or data")
	}
	assert.Zero(t, stub.methodCalls(qdrant.Collections_Create_FullMethodName))
}

func TestCacheAvailabilityQdrantNamespaceRejectsUnavailableIndexCreation(t *testing.T) {
	info := cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: qdrant.Distance_Cosine}))
	before := proto.Clone(info)
	stub := &cacheAvailabilityQdrantStub{exists: true, info: info, indexErr: status.Error(codes.Unavailable, "index unavailable")}
	store, ctx := newCacheAvailabilityQdrant(t, stub)
	properties := cacheAvailabilityQdrantProperties()
	properties["new_filter"] = VectorStoreProperties{DataType: VectorStorePropertyTypeBoolean, Filterable: true}
	err := store.CreateNamespace(ctx, "cache", 2, properties)
	require.ErrorContains(t, err, "index unavailable")
	assert.True(t, IsUnavailable(err))
	after, _, _ := stub.snapshot()
	assert.True(t, proto.Equal(before, after))
	assert.Zero(t, stub.methodCalls(qdrant.Collections_Create_FullMethodName))
}

func TestCacheAvailabilityQdrantNamespaceRecoversAfterCollectionDisappearsDuringRPC(t *testing.T) {
	for _, stage := range []string{"inspection", "index creation"} {
		t.Run(stage, func(t *testing.T) {
			stub := &cacheAvailabilityQdrantStub{
				exists: true,
				info:   cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: qdrant.Distance_Cosine})),
			}
			if stage == "inspection" {
				stub.loseBeforeInfo = true
			} else {
				stub.loseBeforeIndex = true
			}
			store, ctx := newCacheAvailabilityQdrant(t, stub)
			properties := cacheAvailabilityQdrantProperties()
			properties["new_filter"] = VectorStoreProperties{DataType: VectorStorePropertyTypeBoolean, Filterable: true}
			err := store.CreateNamespace(ctx, "cache", 2, properties)
			require.ErrorIs(t, err, ErrNamespaceNotFound)
			assert.NotErrorIs(t, err, ErrNotFound)
			assert.Equal(t, codes.NotFound, status.Code(err), "the real SDK status must survive namespace wrapping")
			assert.True(t, IsUnavailable(err), "namespace loss during initialization must permit recovery")
			assert.Zero(t, stub.methodCalls(qdrant.Collections_Create_FullMethodName))
			if stage == "inspection" {
				stub.assertNoMutations(t)
			}

			// The next attempt sees the loss, recreates the collection, and builds
			// every required index rather than remaining stuck on the first error.
			require.NoError(t, store.CreateNamespace(ctx, "cache", 2, properties))
			after, _, methods := stub.snapshot()
			require.NotNil(t, after)
			assert.Equal(t, uint64(2), after.GetConfig().GetParams().GetVectorsConfig().GetParams().GetSize())
			assert.Equal(t, qdrant.Distance_Cosine, after.GetConfig().GetParams().GetVectorsConfig().GetParams().GetDistance())
			require.Len(t, after.PayloadSchema, 4)
			assert.Equal(t, qdrant.PayloadSchemaType_Keyword, after.PayloadSchema["cache_key"].GetDataType())
			assert.Equal(t, qdrant.PayloadSchemaType_Integer, after.PayloadSchema["expires_at"].GetDataType())
			assert.Equal(t, qdrant.PayloadSchemaType_Bool, after.PayloadSchema["owned"].GetDataType())
			assert.Equal(t, qdrant.PayloadSchemaType_Bool, after.PayloadSchema["new_filter"].GetDataType())
			for _, method := range methods {
				assert.False(t, strings.Contains(method, "Delete"), "recovery must not issue its own destructive operation")
			}
			assert.Equal(t, 1, stub.methodCalls(qdrant.Collections_Create_FullMethodName))
			indexCalls := stub.methodCalls(qdrant.Points_CreateFieldIndex_FullMethodName)
			require.NoError(t, store.CreateNamespace(ctx, "cache", 2, properties))
			assert.Equal(t, 1, stub.methodCalls(qdrant.Collections_Create_FullMethodName))
			assert.Equal(t, indexCalls, stub.methodCalls(qdrant.Points_CreateFieldIndex_FullMethodName), "once recovered, compatible indexes are preserved")
		})
	}
}

func TestCacheAvailabilityQdrantNamespaceRPCPreservesSchemaAndAccessErrors(t *testing.T) {
	for _, stage := range []string{"inspection", "index creation"} {
		t.Run(stage, func(t *testing.T) {
			for _, code := range []codes.Code{codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated} {
				t.Run(code.String(), func(t *testing.T) {
					info := cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: qdrant.Distance_Cosine}))
					before := proto.Clone(info)
					stub := &cacheAvailabilityQdrantStub{exists: true, info: info}
					if stage == "inspection" {
						stub.infoErr = status.Error(code, "inspection refused")
					} else {
						stub.indexErr = status.Error(code, "index creation refused")
					}
					store, ctx := newCacheAvailabilityQdrant(t, stub)
					properties := cacheAvailabilityQdrantProperties()
					properties["new_filter"] = VectorStoreProperties{DataType: VectorStorePropertyTypeBoolean, Filterable: true}
					err := store.CreateNamespace(ctx, "cache", 2, properties)
					require.Error(t, err)
					assert.NotErrorIs(t, err, ErrNamespaceNotFound)
					assert.NotErrorIs(t, err, ErrNotFound)
					assert.Equal(t, code, status.Code(err))
					assert.False(t, IsUnavailable(err), "a reachable service's refusal must remain an initialization error")
					after, _, _ := stub.snapshot()
					assert.True(t, proto.Equal(before, after))
					assert.Zero(t, stub.methodCalls(qdrant.Collections_Create_FullMethodName))
				})
			}
		})
	}
}

func cacheAvailabilityQdrantOperations(store *QdrantStore, ctx context.Context, id string) map[string]func() error {
	return map[string]func() error{
		"GetChunk": func() error {
			_, err := store.GetChunk(ctx, "cache", id)
			return err
		},
		"GetNearest": func() error {
			_, err := store.GetNearest(ctx, "cache", []float32{1, 0}, nil, nil, 0.8, 1)
			return err
		},
		"Add": func() error {
			return store.Add(ctx, "cache", id, []float32{1, 0}, map[string]interface{}{"response": "recovered response"})
		},
	}
}

func TestCacheAvailabilityQdrantLostCollectionAllowsRecoveryAndPointMissDoesNot(t *testing.T) {
	const id = "550e8400-e29b-41d4-a716-446655440000"
	const missingID = "550e8400-e29b-41d4-a716-446655440001"
	stub := &cacheAvailabilityQdrantStub{
		exists: true,
		info:   cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: qdrant.Distance_Cosine})),
		points: []*qdrant.RetrievedPoint{{Id: qdrant.NewID(id), Payload: qdrant.NewValueMap(map[string]interface{}{"response": "previous hit"})}},
	}
	store, ctx := newCacheAvailabilityQdrant(t, stub)
	point, err := store.GetChunk(ctx, "cache", id)
	require.NoError(t, err)
	require.Equal(t, "previous hit", point.Properties["response"])
	_, err = store.GetChunk(ctx, "cache", missingID)
	require.ErrorIs(t, err, ErrNotFound)
	assert.NotErrorIs(t, err, ErrNamespaceNotFound)
	assert.False(t, IsUnavailable(err), "an absent point in a healthy collection is an ordinary cache miss")

	// A replacement service or a deleted collection has none of the previous
	// namespace or point state. Reads and writes must request namespace recovery.
	stub.mu.Lock()
	stub.exists, stub.info, stub.points = false, nil, nil
	stub.mu.Unlock()
	for name, operation := range cacheAvailabilityQdrantOperations(store, ctx, id) {
		t.Run(name, func(t *testing.T) {
			err := operation()
			require.ErrorIs(t, err, ErrNamespaceNotFound)
			assert.NotErrorIs(t, err, ErrNotFound)
			assert.Equal(t, codes.NotFound, status.Code(err), "the original SDK status must survive wrapping")
			assert.True(t, IsUnavailable(err), "a lost collection must allow initialization recovery")
		})
	}

	require.NoError(t, store.CreateNamespace(ctx, "cache", 2, cacheAvailabilityQdrantProperties()))
	_, err = store.GetChunk(ctx, "cache", id)
	require.ErrorIs(t, err, ErrNotFound, "a recreated empty collection is already available")
	assert.False(t, IsUnavailable(err))
	require.NoError(t, store.Add(ctx, "cache", id, []float32{1, 0}, map[string]interface{}{"response": "recovered response"}))
	point, err = store.GetChunk(ctx, "cache", id)
	require.NoError(t, err)
	assert.Equal(t, "recovered response", point.Properties["response"])
	nearest, err := store.GetNearest(ctx, "cache", []float32{1, 0}, nil, nil, 0.8, 1)
	require.NoError(t, err)
	require.Len(t, nearest, 1)
	assert.Equal(t, id, nearest[0].ID)
	assert.Equal(t, 1, stub.methodCalls(qdrant.Collections_Create_FullMethodName))
}

func TestCacheAvailabilityQdrantPointOperationsPreserveSchemaAndAccessErrors(t *testing.T) {
	const id = "550e8400-e29b-41d4-a716-446655440000"
	for _, code := range []codes.Code{codes.InvalidArgument, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			stub := &cacheAvailabilityQdrantStub{exists: true, pointErr: status.Error(code, "request refused")}
			store, ctx := newCacheAvailabilityQdrant(t, stub)
			for name, operation := range cacheAvailabilityQdrantOperations(store, ctx, id) {
				t.Run(name, func(t *testing.T) {
					err := operation()
					require.Error(t, err)
					assert.NotErrorIs(t, err, ErrNamespaceNotFound)
					assert.NotErrorIs(t, err, ErrNotFound)
					assert.Equal(t, code, status.Code(err))
					assert.False(t, IsUnavailable(err))
				})
			}
		})
	}
}

func TestCacheAvailabilityIsUnavailable(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		unavailable bool
	}{
		{"nil", nil, false},
		{"lost namespace", ErrNamespaceNotFound, true},
		{"point miss", ErrNotFound, false},
		{"unclassified gRPC not found", status.Error(codes.NotFound, "unspecified resource"), false},
		{"context deadline", context.DeadlineExceeded, true},
		{"context cancellation", context.Canceled, true},
		{"connection refusal", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, true},
		{"DNS failure", &net.DNSError{Err: "no such host", Name: "qdrant.invalid", IsNotFound: true}, true},
		{"gRPC unavailable", status.Error(codes.Unavailable, "backend unavailable"), true},
		{"gRPC deadline", status.Error(codes.DeadlineExceeded, "backend timed out"), true},
		{"gRPC cancellation", status.Error(codes.Canceled, "request canceled"), true},
		{"dimension mismatch", dimensionMismatchError("cache", 3, 2, "collection"), false},
		{"schema mismatch", errors.New("index has type Text, expected Keyword"), false},
		{"gRPC invalid argument", status.Error(codes.InvalidArgument, "incompatible schema"), false},
		{"gRPC permission denied", status.Error(codes.PermissionDenied, "access unavailable"), false},
		{"gRPC unauthenticated", status.Error(codes.Unauthenticated, "invalid key"), false},
		{"gRPC failed precondition", status.Error(codes.FailedPrecondition, "collection incompatible"), false},
		{"network wrapper around permission response", &url.Error{Op: "POST", URL: "https://qdrant.invalid", Err: status.Error(codes.PermissionDenied, "invalid access")}, false},
		{"network wrapper around schema response", &url.Error{Op: "POST", URL: "https://qdrant.invalid", Err: status.Error(codes.InvalidArgument, "invalid schema")}, false},
		{"message alone", errors.New("dial tcp: connection refused; unavailable; deadline exceeded"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.unavailable, IsUnavailable(tt.err))
			if tt.err != nil {
				assert.Equal(t, tt.unavailable, IsUnavailable(fmt.Errorf("initializing namespace: %w", tt.err)), "wrapping must preserve the classification")
			}
		})
	}
}
