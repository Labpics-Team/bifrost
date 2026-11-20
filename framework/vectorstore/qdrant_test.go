package vectorstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	QdrantTestTimeout     = 30 * time.Second
	QdrantTestCollection  = "bifrost-test-collection"
	QdrantTestDefaultHost = "localhost"
	QdrantTestDefaultPort = "6334"
	QdrantTestDimension   = 384
)

type QdrantTestSetup struct {
	Store  *QdrantStore
	Logger schemas.Logger
	Config QdrantConfig
	ctx    context.Context
	cancel context.CancelFunc
}

func NewQdrantTestSetup(t *testing.T) *QdrantTestSetup {
	host := schemas.NewSecretVar(getEnvWithDefault("QDRANT_HOST", QdrantTestDefaultHost))
	port := schemas.NewSecretVar(getEnvWithDefault("QDRANT_PORT", QdrantTestDefaultPort))
	apiKey := schemas.NewSecretVar(os.Getenv("QDRANT_API_KEY"))
	useTLS := schemas.NewSecretVar(os.Getenv("QDRANT_USE_TLS"))

	config := QdrantConfig{
		Host:   *host,
		Port:   *port,
		APIKey: *apiKey,
		UseTLS: *useTLS,
	}

	logger := bifrost.NewDefaultLogger(schemas.LogLevelInfo)
	ctx, cancel := context.WithTimeout(context.Background(), QdrantTestTimeout)

	store, err := newQdrantStore(ctx, &config, logger)
	if err != nil {
		cancel()
		t.Fatalf("Failed to create Qdrant store: %v", err)
	}

	setup := &QdrantTestSetup{
		Store:  store,
		Logger: logger,
		Config: config,
		ctx:    ctx,
		cancel: cancel,
	}

	setup.ensureCollectionExists(t)

	return setup
}

func (ts *QdrantTestSetup) Cleanup(t *testing.T) {
	defer ts.cancel()

	if !testing.Short() {
		ts.cleanupTestData(t)
	}

	if err := ts.Store.Close(ts.ctx, QdrantTestCollection); err != nil {
		t.Logf("Warning: Failed to close store: %v", err)
	}
}

func (ts *QdrantTestSetup) ensureCollectionExists(t *testing.T) {
	properties := map[string]VectorStoreProperties{
		"key": {
			DataType: VectorStorePropertyTypeString,
		},
		"type": {
			DataType: VectorStorePropertyTypeString,
		},
		"test_type": {
			DataType: VectorStorePropertyTypeString,
		},
		"size": {
			DataType: VectorStorePropertyTypeInteger,
		},
		"public": {
			DataType: VectorStorePropertyTypeBoolean,
		},
		"author": {
			DataType: VectorStorePropertyTypeString,
		},
		"request_hash": {
			DataType: VectorStorePropertyTypeString,
		},
		"user": {
			DataType: VectorStorePropertyTypeString,
		},
		"lang": {
			DataType: VectorStorePropertyTypeString,
		},
		"category": {
			DataType: VectorStorePropertyTypeString,
		},
		"content": {
			DataType: VectorStorePropertyTypeString,
		},
		"response": {
			DataType: VectorStorePropertyTypeString,
		},
	}

	err := ts.Store.CreateNamespace(ts.ctx, QdrantTestCollection, QdrantTestDimension, properties)
	if err != nil {
		t.Fatalf("Failed to create collection %q: %v", QdrantTestCollection, err)
	}
	t.Logf("Created test collection: %s", QdrantTestCollection)
}

func (ts *QdrantTestSetup) cleanupTestData(t *testing.T) {
	allTestKeys, _, err := ts.Store.GetAll(ts.ctx, QdrantTestCollection, []Query{}, []string{}, nil, 1000)
	if err != nil {
		t.Logf("Warning: Failed to get all test keys: %v", err)
		return
	}

	for _, key := range allTestKeys {
		err := ts.Store.Delete(ts.ctx, QdrantTestCollection, key.ID)
		if err != nil {
			t.Logf("Warning: Failed to delete test key %s: %v", key.ID, err)
		}
	}

	t.Logf("Cleaned up test collection: %s", QdrantTestCollection)
}

func TestQdrantConfig_Validation(t *testing.T) {
	logger := bifrost.NewDefaultLogger(schemas.LogLevelInfo)
	ctx := context.Background()

	tests := []struct {
		name        string
		config      QdrantConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: QdrantConfig{
				Host: *schemas.NewSecretVar("localhost"),
				Port: *schemas.NewSecretVar("6334"),
			},
			expectError: false,
		},
		{
			name: "missing host",
			config: QdrantConfig{
				Port: *schemas.NewSecretVar("6334"),
			},
			expectError: true,
			errorMsg:    "qdrant host is required",
		},
		{
			name: "missing port uses default",
			config: QdrantConfig{
				Host: *schemas.NewSecretVar("localhost"),
			},
			expectError: false, // Port defaults to 6334 via CoerceInt fallback
		},
		{
			name: "with api key",
			config: QdrantConfig{
				Host:   *schemas.NewSecretVar("cluster.qdrant.io"),
				Port:   *schemas.NewSecretVar("6334"),
				APIKey: *schemas.NewSecretVar("test-key"),
			},
			expectError: false,
		},
		{
			name: "with tls",
			config: QdrantConfig{
				Host:   *schemas.NewSecretVar("localhost"),
				Port:   *schemas.NewSecretVar("6334"),
				UseTLS: *schemas.NewSecretVar("true"),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := newQdrantStore(ctx, &tt.config, logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, store)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					assert.Contains(t, err.Error(), "failed to connect")
				}
			}
		})
	}
}

func TestParsePointID(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		expectError bool
	}{
		{
			name:        "valid UUID",
			id:          "550e8400-e29b-41d4-a716-446655440000",
			expectError: false,
		},
		{
			name:        "invalid UUID",
			id:          "not-a-uuid",
			expectError: true,
		},
		{
			name:        "empty string",
			id:          "",
			expectError: true,
		},
		{
			name:        "numeric string",
			id:          "12345",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pointID, err := parsePointID(tt.id)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, pointID)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, pointID)
			}
		})
	}
}

func TestBuildQdrantFilter(t *testing.T) {
	tests := []struct {
		name     string
		queries  []Query
		expected bool
	}{
		{
			name:     "empty queries",
			queries:  []Query{},
			expected: false,
		},
		{
			name: "single string query",
			queries: []Query{
				{Field: "category", Operator: QueryOperatorEqual, Value: "tech"},
			},
			expected: true,
		},
		{
			name: "single numeric query",
			queries: []Query{
				{Field: "size", Operator: QueryOperatorGreaterThan, Value: 1000},
			},
			expected: true,
		},
		{
			name: "multiple queries (AND)",
			queries: []Query{
				{Field: "category", Operator: QueryOperatorEqual, Value: "tech"},
				{Field: "public", Operator: QueryOperatorEqual, Value: true},
			},
			expected: true,
		},
		{
			name: "null checks",
			queries: []Query{
				{Field: "author", Operator: QueryOperatorIsNull, Value: nil},
			},
			expected: true,
		},
		{
			name: "not null checks",
			queries: []Query{
				{Field: "author", Operator: QueryOperatorIsNotNull, Value: nil},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildQdrantFilter(tt.queries)

			if tt.expected {
				assert.NotNil(t, result)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestQdrantStore_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	setup := NewQdrantTestSetup(t)
	defer setup.Cleanup(t)

	err := setup.Store.Ping(setup.ctx)
	require.NoError(t, err)

	key := generateUUID()
	err = setup.Store.Add(setup.ctx, QdrantTestCollection, key, generateTestEmbedding(QdrantTestDimension), map[string]interface{}{"type": "document"})
	require.NoError(t, err)

	result, err := setup.Store.GetChunk(setup.ctx, QdrantTestCollection, key)
	require.NoError(t, err)
	assert.Equal(t, "document", result.Properties["type"])

	keys := []string{generateUUID(), generateUUID()}
	for i, k := range keys {
		err = setup.Store.Add(setup.ctx, QdrantTestCollection, k, generateTestEmbedding(QdrantTestDimension), map[string]interface{}{"type": i})
		require.NoError(t, err)
	}

	results, err := setup.Store.GetChunks(setup.ctx, QdrantTestCollection, keys)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestQdrantStore_Filtering(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	setup := NewQdrantTestSetup(t)
	defer setup.Cleanup(t)

	for i := 0; i < 3; i++ {
		metadata := map[string]interface{}{"type": "pdf", "public": true}
		if i == 1 {
			metadata["type"] = "docx"
			metadata["public"] = false
		}
		err := setup.Store.Add(setup.ctx, QdrantTestCollection, generateUUID(), generateTestEmbedding(QdrantTestDimension), metadata)
		require.NoError(t, err)
	}

	queries := []Query{{Field: "type", Operator: QueryOperatorEqual, Value: "pdf"}}
	results, _, err := setup.Store.GetAll(setup.ctx, QdrantTestCollection, queries, []string{"type"}, nil, 10)
	require.NoError(t, err)
	assert.Len(t, results, 2)

	multiQuery := []Query{
		{Field: "type", Operator: QueryOperatorEqual, Value: "pdf"},
		{Field: "public", Operator: QueryOperatorEqual, Value: true},
	}
	results, _, err = setup.Store.GetAll(setup.ctx, QdrantTestCollection, multiQuery, []string{"type"}, nil, 10)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestQdrantStore_VectorSearch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	setup := NewQdrantTestSetup(t)
	defer setup.Cleanup(t)

	emb := generateTestEmbedding(QdrantTestDimension)
	err := setup.Store.Add(setup.ctx, QdrantTestCollection, generateUUID(), emb, map[string]interface{}{"type": "tech"})
	require.NoError(t, err)

	err = setup.Store.Add(setup.ctx, QdrantTestCollection, generateUUID(), generateTestEmbedding(QdrantTestDimension), map[string]interface{}{"type": "sports"})
	require.NoError(t, err)

	results, err := setup.Store.GetNearest(setup.ctx, QdrantTestCollection, emb, nil, []string{"type"}, 0.1, 10)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
	require.NotNil(t, results[0].Score)

	queries := []Query{{Field: "type", Operator: QueryOperatorEqual, Value: "tech"}}
	results, err = setup.Store.GetNearest(setup.ctx, QdrantTestCollection, emb, queries, []string{"type"}, 0.1, 10)
	require.NoError(t, err)
	for _, result := range results {
		assert.Equal(t, "tech", result.Properties["type"])
	}
}

func TestQdrantStore_InterfaceCompliance(t *testing.T) {
	var _ VectorStore = (*QdrantStore)(nil)
}

func TestVectorStoreFactory_Qdrant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	logger := bifrost.NewDefaultLogger(schemas.LogLevelInfo)

	host := schemas.NewSecretVar(getEnvWithDefault("QDRANT_HOST", QdrantTestDefaultHost))
	port := schemas.NewSecretVar(getEnvWithDefault("QDRANT_PORT", QdrantTestDefaultPort))
	apiKey := schemas.NewSecretVar(os.Getenv("QDRANT_API_KEY"))

	config := &Config{
		Enabled: true,
		Type:    VectorStoreTypeQdrant,
		Config: QdrantConfig{
			Host:   *host,
			Port:   *port,
			APIKey: *apiKey,
		},
	}

	store, err := NewVectorStore(context.Background(), config, logger)
	if err != nil {
		t.Skipf("Could not create Qdrant store: %v", err)
	}
	defer store.Close(context.Background(), QdrantTestCollection)

	qdrantStore, ok := store.(*QdrantStore)
	assert.True(t, ok)
	assert.NotNil(t, qdrantStore)
}

func TestQdrantStore_DimensionHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	setup := NewQdrantTestSetup(t)
	defer setup.Cleanup(t)

	testCollection := "TestDim"
	props := map[string]VectorStoreProperties{"type": {DataType: VectorStorePropertyTypeString}}

	err := setup.Store.CreateNamespace(setup.ctx, testCollection, 512, props)
	require.NoError(t, err)

	err = setup.Store.Add(setup.ctx, testCollection, generateUUID(), generateTestEmbedding(512), map[string]interface{}{"type": "test"})
	require.NoError(t, err)

	err = setup.Store.DeleteNamespace(setup.ctx, testCollection)
	require.NoError(t, err)

	err = setup.Store.CreateNamespace(setup.ctx, testCollection, QdrantTestDimension, props)
	require.NoError(t, err)

	emb := generateTestEmbedding(QdrantTestDimension)
	err = setup.Store.Add(setup.ctx, testCollection, generateUUID(), emb, map[string]interface{}{"type": "test"})
	require.NoError(t, err)

	results, err := setup.Store.GetNearest(setup.ctx, testCollection, emb, nil, []string{"type"}, 0.8, 10)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)

	setup.Store.DeleteNamespace(setup.ctx, testCollection)
}

func TestQdrantStore_ErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	setup := NewQdrantTestSetup(t)
	defer setup.Cleanup(t)

	_, err := setup.Store.GetChunk(setup.ctx, QdrantTestCollection, generateUUID())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	err = setup.Store.Add(setup.ctx, QdrantTestCollection, "", generateTestEmbedding(QdrantTestDimension), map[string]interface{}{"type": "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "id is required")

	err = setup.Store.Add(setup.ctx, QdrantTestCollection, "not-a-uuid", generateTestEmbedding(QdrantTestDimension), map[string]interface{}{"type": "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid id format")

	err = setup.Store.Delete(setup.ctx, QdrantTestCollection, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "id is required")

	err = setup.Store.Delete(setup.ctx, QdrantTestCollection, "not-a-uuid")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid id format")
}

// TestQdrantStore_CreateNamespaceRejectsDimensionChange pins the guard every store shares and the
// sentinel callers use to tell a configuration mistake apart from a store failure.
func TestQdrantStore_CreateNamespaceRejectsDimensionChange(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	setup := NewQdrantTestSetup(t)
	defer setup.Cleanup(t)

	collection := "TestDimGuard"
	props := map[string]VectorStoreProperties{"type": {DataType: VectorStorePropertyTypeString}}
	_ = setup.Store.DeleteNamespace(setup.ctx, collection)
	defer func() { _ = setup.Store.DeleteNamespace(setup.ctx, collection) }()

	require.NoError(t, setup.Store.CreateNamespace(setup.ctx, collection, 512, props))
	require.NoError(t, setup.Store.CreateNamespace(setup.ctx, collection, 512, props))

	err := setup.Store.CreateNamespace(setup.ctx, collection, 1024, props)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDimensionMismatch)
	assert.Contains(t, err.Error(), "already exists with dimension 512 but config requires 1024")
}

// TestQdrantStore_GetNearestScoresCosineSimilarity pins GetNearest's threshold and score to cosine
// similarity, the scale every store uses.
func TestQdrantStore_GetNearestScoresCosineSimilarity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	setup := NewQdrantTestSetup(t)
	defer setup.Cleanup(t)

	collection := "TestCosineScore"
	props := map[string]VectorStoreProperties{"label": {DataType: VectorStorePropertyTypeString}}
	_ = setup.Store.DeleteNamespace(setup.ctx, collection)
	defer func() { _ = setup.Store.DeleteNamespace(setup.ctx, collection) }()
	require.NoError(t, setup.Store.CreateNamespace(setup.ctx, collection, 2, props))

	require.NoError(t, setup.Store.Add(setup.ctx, collection, generateUUID(), unitVectorAtCosine(2, 0.9), map[string]interface{}{"label": "close"}))
	require.NoError(t, setup.Store.Add(setup.ctx, collection, generateUUID(), unitVectorAtCosine(2, 0.7), map[string]interface{}{"label": "far"}))

	results, err := setup.Store.GetNearest(setup.ctx, collection, unitVectorAtCosine(2, 1), nil, []string{"label"}, 0.8, 10)
	require.NoError(t, err)
	require.Len(t, results, 1, "only the vector at cosine 0.9 clears a 0.8 threshold")
	assert.Equal(t, "close", results[0].Properties["label"])
	require.NotNil(t, results[0].Score)
	assert.InDelta(t, 0.9, *results[0].Score, 1e-3)
}

// TestQdrantStore_DeleteMissingIDSucceeds pins deleting an id that was never written as a no-op,
// the contract every store shares so clearing an unknown cache entry is not a server error.
func TestQdrantStore_DeleteMissingIDSucceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	setup := NewQdrantTestSetup(t)
	defer setup.Cleanup(t)

	props := map[string]VectorStoreProperties{"type": {DataType: VectorStorePropertyTypeString}}
	require.NoError(t, setup.Store.CreateNamespace(setup.ctx, QdrantTestCollection, QdrantTestDimension, props))
	assert.NoError(t, setup.Store.Delete(setup.ctx, QdrantTestCollection, generateUUID()))
}

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

func TestCacheRetentionQdrantRequiresIntegerFilters(t *testing.T) {
	for _, tc := range []struct {
		name      string
		params    *qdrant.IntegerIndexParams
		wantError string
	}{
		{name: "default"},
		{name: "implicit range", params: &qdrant.IntegerIndexParams{Lookup: qdrant.PtrOf(true), OnDisk: qdrant.PtrOf(true)}},
		{name: "explicit range", params: &qdrant.IntegerIndexParams{Range: qdrant.PtrOf(true), OnDisk: qdrant.PtrOf(true)}},
		{name: "disabled range", params: &qdrant.IntegerIndexParams{Lookup: qdrant.PtrOf(true), Range: qdrant.PtrOf(false)}, wantError: "range"},
		{name: "disabled lookup", params: &qdrant.IntegerIndexParams{Lookup: qdrant.PtrOf(false), Range: qdrant.PtrOf(true)}, wantError: "lookup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := cacheAvailabilityQdrantInfo(qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 2, Distance: qdrant.Distance_Cosine}))
			info.PayloadSchema["rank"] = &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Integer}
			if tc.params != nil {
				info.PayloadSchema["rank"].Params = &qdrant.PayloadIndexParams{IndexParams: &qdrant.PayloadIndexParams_IntegerIndexParams{IntegerIndexParams: tc.params}}
			}
			before := proto.Clone(info)
			stub := &cacheAvailabilityQdrantStub{exists: true, info: info}
			store, ctx := newCacheAvailabilityQdrant(t, stub)
			err := store.CreateNamespace(ctx, "cache", 2, map[string]VectorStoreProperties{"rank": {DataType: VectorStorePropertyTypeInteger, Filterable: true}})
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
			stub.assertNoMutations(t)
			after, _, _ := stub.snapshot()
			require.True(t, proto.Equal(before, after), "operator-selected index parameters must survive")
		})
	}
}

func TestCacheRetentionQdrantUsesSingleFilteredDeleteRPC(t *testing.T) {
	var methods []string
	var deleted *qdrant.DeletePoints
	client, err := qdrant.NewClient(&qdrant.Config{
		Host: "127.0.0.1", Port: 1, PoolSize: 1, SkipCompatibilityCheck: true,
		GrpcOptions: []grpc.DialOption{grpc.WithUnaryInterceptor(func(_ context.Context, method string, req, reply any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
			methods = append(methods, method)
			if method != qdrant.Points_Delete_FullMethodName {
				return fmt.Errorf("unexpected retention RPC %s", method)
			}
			deleted = proto.Clone(req.(*qdrant.DeletePoints)).(*qdrant.DeletePoints)
			reply.(*qdrant.PointsOperationResponse).Result = &qdrant.UpdateResult{Status: qdrant.UpdateStatus_Completed}
			return nil
		})},
	})
	require.NoError(t, err)
	defer client.Close()
	queries := []Query{
		{Field: "from_bifrost_semantic_cache_plugin", Operator: QueryOperatorEqual, Value: true},
		{Field: "expires_at", Operator: QueryOperatorGreaterThan, Value: int64(0)},
		{Field: "expires_at", Operator: QueryOperatorLessThan, Value: int64(2_000_000_000)},
	}
	require.NoError(t, (&QdrantStore{client: client}).DeleteByFilter(context.Background(), "cache", queries))
	require.Equal(t, []string{qdrant.Points_Delete_FullMethodName}, methods)
	require.NotNil(t, deleted.GetPoints().GetFilter(), "retention must evaluate current payload, not an ID snapshot")
	require.Nil(t, deleted.GetPoints().GetPoints())
	require.Equal(t, "cache", deleted.CollectionName)
	require.True(t, deleted.GetWait())
	require.Len(t, deleted.GetPoints().GetFilter().GetMust(), 3)
}

func TestCacheRetentionQdrantRejectsUnsafeFilters(t *testing.T) {
	store := &QdrantStore{} // An invalid request must not reach even a client.
	guard := Query{Field: "owned", Operator: QueryOperatorEqual, Value: true}
	tests := []struct {
		name    string
		queries []Query
	}{
		{name: "nil"},
		{name: "empty", queries: []Query{}},
		{name: "empty field", queries: []Query{guard, {Operator: QueryOperatorEqual, Value: true}}},
		{name: "whitespace field", queries: []Query{guard, {Field: " ", Operator: QueryOperatorEqual, Value: true}}},
		{name: "unknown operator", queries: []Query{guard, {Field: "expiry", Operator: "misspelled", Value: 1}}},
		{name: "unsupported operator", queries: []Query{guard, {Field: "expiry", Operator: QueryOperatorLike, Value: "*"}}},
		{name: "nil equality", queries: []Query{guard, {Field: "expiry", Operator: QueryOperatorEqual}}},
		{name: "unsupported equality", queries: []Query{guard, {Field: "expiry", Operator: QueryOperatorEqual, Value: []string{"1"}}}},
		{name: "string range", queries: []Query{guard, {Field: "expiry", Operator: QueryOperatorLessThan, Value: "1"}}},
		{name: "nan range", queries: []Query{guard, {Field: "expiry", Operator: QueryOperatorLessThan, Value: math.NaN()}}},
		{name: "infinite range", queries: []Query{guard, {Field: "expiry", Operator: QueryOperatorLessThan, Value: math.Inf(1)}}},
		{name: "rounded integer", queries: []Query{guard, {Field: "expiry", Operator: QueryOperatorLessThan, Value: int64(1<<53 + 1)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, store.DeleteByFilter(context.Background(), "retention", tt.queries))
		})
	}
	require.Error(t, store.DeleteByFilter(context.Background(), "", []Query{guard}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, store.DeleteByFilter(ctx, "retention", []Query{guard}), context.Canceled)
}

// newCacheRetentionQdrant uses a unique collection and fails if the integration
// service is unavailable. CI must supply an isolated Qdrant, never production.
func newCacheRetentionQdrant(t *testing.T) (*QdrantStore, context.Context, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("Qdrant integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	config := &QdrantConfig{
		Host:   *schemas.NewSecretVar(getEnvWithDefault("QDRANT_HOST", QdrantTestDefaultHost)),
		Port:   *schemas.NewSecretVar(getEnvWithDefault("QDRANT_PORT", QdrantTestDefaultPort)),
		APIKey: *schemas.NewSecretVar(os.Getenv("QDRANT_API_KEY")),
		UseTLS: *schemas.NewSecretVar(os.Getenv("QDRANT_USE_TLS")),
	}
	store, err := newQdrantStore(ctx, config, bifrost.NewDefaultLogger(schemas.LogLevelError))
	require.NoError(t, err)
	namespace := "bifrost-retention-" + generateUUID()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		assert.NoError(t, store.DeleteNamespace(cleanupCtx, namespace))
		assert.NoError(t, store.Close(cleanupCtx, namespace))
	})
	return store, ctx, namespace
}

func TestCacheRetentionQdrantDeletesExpiredOwnedPoints(t *testing.T) {
	store, ctx, namespace := newCacheRetentionQdrant(t)
	props := map[string]VectorStoreProperties{
		"from_bifrost_semantic_cache_plugin": {DataType: VectorStorePropertyTypeBoolean, Filterable: true},
		"expires_at":                         {DataType: VectorStorePropertyTypeInteger, Filterable: true},
	}
	require.NoError(t, store.CreateNamespace(ctx, namespace, 2, props))
	const cutoff int64 = 2_000_000_000
	points := make([]*qdrant.PointStruct, 0, 160)
	allIDs := make([]*qdrant.PointId, 0, 160)
	addPoint := func(payload map[string]interface{}) string {
		id := generateUUID()
		pointID := qdrant.NewID(id)
		allIDs = append(allIDs, pointID)
		points = append(points, &qdrant.PointStruct{
			Id: pointID, Vectors: qdrant.NewVectors(1, 0), Payload: qdrant.NewValueMap(payload),
		})
		return id
	}
	// More than a scroll page: the operation must not truncate at an ID snapshot.
	for range 150 {
		addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": true, "expires_at": cutoff - 1})
	}
	survivors := []string{
		addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": true, "expires_at": cutoff + 3600}),
		addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": true, "expires_at": cutoff}),
		addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": true, "expires_at": int64(0)}),
		addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": true, "expires_at": int64(-1)}),
		addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": true}),
		addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": false, "expires_at": cutoff - 1}),
		addPoint(map[string]interface{}{"expires_at": cutoff - 1}),
		addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": true, "expires_at": "invalid"}),
	}
	refreshedID := addPoint(map[string]interface{}{"from_bifrost_semantic_cache_plugin": true, "expires_at": cutoff - 1})
	survivors = append(survivors, refreshedID)
	_, err := store.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: namespace, Points: points, Wait: qdrant.PtrOf(true),
	})
	require.NoError(t, err)
	seeded, err := store.client.Get(ctx, &qdrant.GetPoints{CollectionName: namespace, Ids: allIDs})
	require.NoError(t, err)
	require.Len(t, seeded, len(points), "positive control: all fixture writes must be visible")

	// The same deterministic cache ID was expired when observed, but is fresh
	// when the delete is submitted. Its current payload must win.
	_, err = store.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: namespace,
		Points: []*qdrant.PointStruct{{
			Id: qdrant.NewID(refreshedID), Vectors: qdrant.NewVectors(1, 0),
			Payload: qdrant.NewValueMap(map[string]interface{}{
				"from_bifrost_semantic_cache_plugin": true, "expires_at": cutoff + 3600,
			}),
		}},
		Wait: qdrant.PtrOf(true),
	})
	require.NoError(t, err)
	queries := []Query{
		{Field: "from_bifrost_semantic_cache_plugin", Operator: QueryOperatorEqual, Value: true},
		{Field: "expires_at", Operator: QueryOperatorGreaterThan, Value: int64(0)},
		{Field: "expires_at", Operator: QueryOperatorLessThan, Value: cutoff},
	}
	for range 2 { // Retry after an ambiguous timeout must be harmless.
		require.NoError(t, store.DeleteByFilter(ctx, namespace, queries))
		remaining, err := store.client.Get(ctx, &qdrant.GetPoints{
			CollectionName: namespace, Ids: allIDs, WithPayload: qdrant.NewWithPayload(true),
		})
		require.NoError(t, err)
		gotIDs := make([]string, 0, len(remaining))
		for _, point := range remaining {
			gotIDs = append(gotIDs, pointIDToString(point.Id))
			if pointIDToString(point.Id) == refreshedID {
				assert.Equal(t, cutoff+3600, point.Payload["expires_at"].GetIntegerValue())
			}
		}
		assert.ElementsMatch(t, survivors, gotIDs, "expired owned points disappear immediately; every positive control survives")
	}
}

func TestCacheRetentionQdrantPreservesIndexSchema(t *testing.T) {
	store, ctx, namespace := newCacheRetentionQdrant(t)
	props := map[string]VectorStoreProperties{
		"cache_key":                          {DataType: VectorStorePropertyTypeString, Filterable: true},
		"provider":                           {DataType: VectorStorePropertyTypeString, Filterable: true},
		"model":                              {DataType: VectorStorePropertyTypeString, Filterable: true},
		"params_hash":                        {DataType: VectorStorePropertyTypeString, Filterable: true},
		"expires_at":                         {DataType: VectorStorePropertyTypeInteger, Filterable: true},
		"from_bifrost_semantic_cache_plugin": {DataType: VectorStorePropertyTypeBoolean, Filterable: true},
		"response":                           {DataType: VectorStorePropertyTypeString},
		"stream_chunks":                      {DataType: VectorStorePropertyTypeStringArray},
	}
	require.NoError(t, store.CreateNamespace(ctx, namespace, 2, props))
	info, err := store.client.GetCollectionInfo(ctx, namespace)
	require.NoError(t, err)
	require.Len(t, info.GetPayloadSchema(), 6)
	require.NotContains(t, info.GetPayloadSchema(), "response")
	require.NotContains(t, info.GetPayloadSchema(), "stream_chunks")
	assert.Equal(t, qdrant.PayloadSchemaType_Integer, info.GetPayloadSchema()["expires_at"].GetDataType())

	keyword := qdrant.FieldType_FieldTypeKeyword
	_, err = store.client.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
		CollectionName: namespace, FieldName: "cache_key", FieldType: &keyword, Wait: qdrant.PtrOf(true),
		FieldIndexParams: &qdrant.PayloadIndexParams{
			IndexParams: &qdrant.PayloadIndexParams_KeywordIndexParams{
				KeywordIndexParams: &qdrant.KeywordIndexParams{OnDisk: qdrant.PtrOf(true), IsTenant: qdrant.PtrOf(true)},
			},
		},
	})
	require.NoError(t, err)
	before, err := store.client.GetCollectionInfo(ctx, namespace)
	require.NoError(t, err)
	customParams := before.GetPayloadSchema()["cache_key"].GetParams()
	require.True(t, customParams.GetKeywordIndexParams().GetIsTenant(), "positive control: custom schema was applied")
	require.True(t, customParams.GetKeywordIndexParams().GetOnDisk(), "positive control: disk placement was applied")

	// Repeated Init must preserve all custom parameters and still create missing
	// filterable fields. It must not blindly recreate an existing collection.
	props["new_filter"] = VectorStoreProperties{DataType: VectorStorePropertyTypeBoolean, Filterable: true}
	for range 2 {
		require.NoError(t, store.CreateNamespace(ctx, namespace, 2, props))
		after, err := store.client.GetCollectionInfo(ctx, namespace)
		require.NoError(t, err)
		assert.True(t, proto.Equal(customParams, after.GetPayloadSchema()["cache_key"].GetParams()))
		assert.Len(t, after.GetPayloadSchema(), 7)
		assert.NotContains(t, after.GetPayloadSchema(), "response")
		assert.NotContains(t, after.GetPayloadSchema(), "stream_chunks")
		assert.Equal(t, qdrant.PayloadSchemaType_Bool, after.GetPayloadSchema()["new_filter"].GetDataType())
	}

	// Legacy non-filterable indexes are an operator migration, not a destructive
	// Init side effect. Existing compatible indexes must remain untouched.
	_, err = store.client.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
		CollectionName: namespace, FieldName: "response", FieldType: &keyword, Wait: qdrant.PtrOf(true),
	})
	require.NoError(t, err)
	require.NoError(t, store.CreateNamespace(ctx, namespace, 2, props))
	after, err := store.client.GetCollectionInfo(ctx, namespace)
	require.NoError(t, err)
	assert.Contains(t, after.GetPayloadSchema(), "response")
	props["cache_key"] = VectorStoreProperties{DataType: VectorStorePropertyTypeInteger, Filterable: true}
	require.ErrorContains(t, store.CreateNamespace(ctx, namespace, 2, props), "expected Integer")
	afterMismatch, err := store.client.GetCollectionInfo(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, proto.Equal(customParams, afterMismatch.GetPayloadSchema()["cache_key"].GetParams()))
}
