package vectorstore

import (
	"context"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"math"
	"os"
	"testing"
	"time"
)

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
