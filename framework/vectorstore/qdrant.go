package vectorstore

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// qdrantMaxRecvMsgSize is the gRPC max receive message size for Qdrant.
// The default 4 MB is too small for payloads that embed large responses
// (e.g. base64-encoded images from image generation providers).
const qdrantMaxRecvMsgSize = 64 * 1024 * 1024 // 64 MB

// QdrantConfig represents the configuration for the Qdrant vector store.
type QdrantConfig struct {
	Host             schemas.SecretVar `json:"host"`                           // Qdrant server host - REQUIRED
	Port             schemas.SecretVar `json:"port"`                           // Qdrant server port  (fallback to 6334 for gRPC)
	APIKey           schemas.SecretVar `json:"api_key,omitempty"`              // API key for authentication - Optional
	UseTLS           schemas.SecretVar `json:"use_tls,omitempty"`              // Use TLS for connection - Optional
	MaxRecvMsgSizeMB schemas.SecretVar `json:"max_recv_msg_size_mb,omitempty"` // gRPC max receive message size in MB (default: 64). Increase when caching large payloads such as image generation responses.
}

// QdrantStore represents the Qdrant vector store.
type QdrantStore struct {
	client *qdrant.Client
	logger schemas.Logger
}

// Ping checks if the Qdrant server is reachable.
func (s *QdrantStore) Ping(ctx context.Context) error {
	_, err := s.client.HealthCheck(ctx)
	return err
}

// CreateNamespace creates a new collection in the Qdrant vector store.
func (s *QdrantStore) CreateNamespace(ctx context.Context, namespace string, dimension int, properties map[string]VectorStoreProperties) error {
	if dimension <= 0 {
		return fmt.Errorf("collection %q dimension must be positive", namespace)
	}
	existence, err := s.client.GetCollectionsClient().CollectionExists(ctx, &qdrant.CollectionExistsRequest{CollectionName: namespace})
	if err != nil {
		return fmt.Errorf("failed to check collection existence: %w", qdrantNamespaceError(namespace, err))
	}
	if existence.GetResult() == nil {
		return fmt.Errorf("failed to check collection %q existence: missing result", namespace)
	}
	exists := existence.GetResult().GetExists()

	var payloadSchema map[string]*qdrant.PayloadSchemaInfo
	if exists {
		info, infoErr := s.client.GetCollectionInfo(ctx, namespace)
		if infoErr != nil {
			return fmt.Errorf("failed to inspect existing collection %q before reconciling indexes: %w", namespace, qdrantNamespaceError(namespace, infoErr))
		}
		payloadSchema = info.GetPayloadSchema()
		vectors := info.GetConfig().GetParams().GetVectorsConfig()
		if vectors.GetParamsMap() != nil {
			return fmt.Errorf("collection %q uses named vectors, which Bifrost does not support", namespace)
		}
		params := vectors.GetParams()
		if params == nil {
			return fmt.Errorf("collection %q has no unnamed vector configuration; cannot verify dimension", namespace)
		}
		if params.GetSize() != uint64(dimension) {
			return dimensionMismatchError(namespace, int(params.GetSize()), dimension, "collection")
		}
		if params.GetDistance() != qdrant.Distance_Cosine {
			return fmt.Errorf("collection %q has distance %s, expected Cosine", namespace, params.GetDistance())
		}
	}

	// Validate every existing index before creating missing ones. A schema
	// mismatch must leave the collection and its operator-selected indexes intact.
	var missingIndexes []*qdrant.CreateFieldIndexCollection
	for fieldName, prop := range properties {
		if !prop.Filterable {
			continue
		}
		var fieldType qdrant.FieldType
		var schemaType qdrant.PayloadSchemaType
		switch prop.DataType {
		case VectorStorePropertyTypeInteger:
			fieldType, schemaType = qdrant.FieldType_FieldTypeInteger, qdrant.PayloadSchemaType_Integer
		case VectorStorePropertyTypeBoolean:
			fieldType, schemaType = qdrant.FieldType_FieldTypeBool, qdrant.PayloadSchemaType_Bool
		default:
			fieldType, schemaType = qdrant.FieldType_FieldTypeKeyword, qdrant.PayloadSchemaType_Keyword
		}
		if existing, ok := payloadSchema[fieldName]; ok {
			if existing.GetDataType() != schemaType {
				return fmt.Errorf("collection %q index %q has type %s, expected %s", namespace, fieldName, existing.GetDataType(), schemaType)
			}
			if schemaType == qdrant.PayloadSchemaType_Integer {
				// Filterable integers support both equality and range Query operators.
				// Omitted flags default to true; preserve every other index setting.
				if params := existing.GetParams().GetIntegerIndexParams(); params != nil {
					if params.Range != nil && !*params.Range {
						return fmt.Errorf("collection %q integer index %q disables range filtering", namespace, fieldName)
					}
					if params.Lookup != nil && !*params.Lookup {
						return fmt.Errorf("collection %q integer index %q disables lookup filtering", namespace, fieldName)
					}
				}
			}
			// Keep operator-selected memory placement and other index parameters.
			continue
		}

		missingIndexes = append(missingIndexes, &qdrant.CreateFieldIndexCollection{
			CollectionName: namespace,
			FieldName:      fieldName,
			FieldType:      &fieldType,
			Wait:           qdrant.PtrOf(true),
		})
	}

	if !exists {
		err = s.client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: namespace,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     uint64(dimension),
				Distance: qdrant.Distance_Cosine,
			}),
		})
		if err != nil {
			return fmt.Errorf("failed to create collection: %w", qdrantNamespaceError(namespace, err))
		}
	}

	for _, index := range missingIndexes {
		_, err = s.client.CreateFieldIndex(ctx, index)
		if err != nil {
			return fmt.Errorf("failed to create index %q in collection %q: %w", index.FieldName, namespace, qdrantNamespaceError(namespace, err))
		}
	}

	return nil
}

// ListNamespaces returns the Qdrant collections beginning with prefix. Qdrant's
// list API takes no filter, so the prefix is applied here.
func (s *QdrantStore) ListNamespaces(ctx context.Context, prefix string) ([]string, error) {
	collections, err := s.client.ListCollections(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	namespaces := make([]string, 0, len(collections))
	for _, collection := range collections {
		if strings.HasPrefix(collection, prefix) {
			namespaces = append(namespaces, collection)
		}
	}
	sort.Strings(namespaces)
	return namespaces, nil
}

// DeleteNamespace deletes a collection from the Qdrant vector store.
func (s *QdrantStore) DeleteNamespace(ctx context.Context, namespace string) error {
	exists, err := s.client.CollectionExists(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to check collection existence: %w", err)
	}
	if !exists {
		return nil
	}
	return s.client.DeleteCollection(ctx, namespace)
}

// GetChunk retrieves a single point from the Qdrant vector store.
func (s *QdrantStore) GetChunk(ctx context.Context, namespace string, id string) (SearchResult, error) {
	if strings.TrimSpace(id) == "" {
		return SearchResult{}, fmt.Errorf("id is required")
	}

	pointID, err := parsePointID(id)
	if err != nil {
		return SearchResult{}, fmt.Errorf("invalid id format: %w", err)
	}

	points, err := s.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: namespace,
		Ids:            []*qdrant.PointId{pointID},
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return SearchResult{}, fmt.Errorf("failed to get point: %w", qdrantNamespaceError(namespace, err))
	}

	if len(points) == 0 {
		return SearchResult{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}

	return SearchResult{
		ID:         id,
		Properties: payloadToMap(points[0].Payload),
	}, nil
}

// GetChunks retrieves multiple points from the Qdrant vector store.
func (s *QdrantStore) GetChunks(ctx context.Context, namespace string, ids []string) ([]SearchResult, error) {
	if len(ids) == 0 {
		return []SearchResult{}, nil
	}

	pointIDs := make([]*qdrant.PointId, 0, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(id) == "" {
			continue
		}
		pointID, err := parsePointID(id)
		if err != nil {
			s.logger.Debug(fmt.Sprintf("skipping invalid id %s: %v", id, err))
			continue
		}
		pointIDs = append(pointIDs, pointID)
	}

	if len(pointIDs) == 0 {
		return []SearchResult{}, nil
	}

	points, err := s.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: namespace,
		Ids:            pointIDs,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get points: %w", err)
	}

	results := make([]SearchResult, 0, len(points))
	for _, point := range points {
		results = append(results, SearchResult{
			ID:         pointIDToString(point.Id),
			Properties: payloadToMap(point.Payload),
		})
	}
	return results, nil
}

// GetAll retrieves all points with optional filtering and pagination.
func (s *QdrantStore) GetAll(ctx context.Context, namespace string, queries []Query, selectFields []string, cursor *string, limit int64) ([]SearchResult, *string, error) {
	filter := buildQdrantFilter(queries)

	var offset *qdrant.PointId
	if cursor != nil && *cursor != "" {
		var err error
		offset, err = parsePointID(*cursor)
		if err != nil {
			s.logger.Debug(fmt.Sprintf("invalid cursor format: %v", err))
		}
	}

	// Clamped rather than converted: a truncated-to-zero limit would scroll
	// nothing, and the len(scrollResult) >= scrollLimit check below would then
	// read 0 >= 0 as a full page and hand back a cursor pointing at the empty
	// lastID — which the next call treats as no cursor at all, so a caller
	// paging to completion would never advance.
	scrollLimit := boundedPageLimit(limit, 100)

	// Vectors are large, so they are read only for a caller that asked.
	includeVectors := IncludeVectorsRequested(ctx)
	request := &qdrant.ScrollPoints{
		CollectionName: namespace,
		Filter:         filter,
		Limit:          &scrollLimit,
		Offset:         offset,
		WithPayload:    qdrant.NewWithPayload(true),
	}
	if includeVectors {
		request.WithVectors = qdrant.NewWithVectors(true)
	}
	// The next page starts at the offset Qdrant hands back, which is the
	// first point it has not returned. Scroll's offset is inclusive, so
	// resuming from the last point returned instead would return that point
	// again at the head of every page after the first.
	scrollResult, nextOffset, err := s.client.ScrollAndOffset(ctx, request)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scroll points: %w", err)
	}

	results := make([]SearchResult, 0, len(scrollResult))
	for _, point := range scrollResult {
		result := SearchResult{
			ID:         pointIDToString(point.Id),
			Properties: filterProperties(payloadToMap(point.Payload), selectFields),
		}
		if includeVectors {
			result.Vector = qdrantDenseVector(point.GetVectors())
		}
		results = append(results, result)
	}

	if nextOffset != nil {
		next := pointIDToString(nextOffset)
		return results, &next, nil
	}
	return results, nil, nil
}

// FiltersVectorReadsOnServer reports that a scroll applies its filter on the
// server.
func (s *QdrantStore) FiltersVectorReadsOnServer() bool { return true }

// qdrantDenseVector is a point's unnamed dense vector, or nil when it has
// none. Both shapes are read: newer servers return it under Dense, older ones
// in the deprecated Data field.
func qdrantDenseVector(vectors *qdrant.VectorsOutput) []float32 {
	vector := vectors.GetVector()
	if vector == nil {
		return nil
	}
	if dense := vector.GetDense(); dense != nil && len(dense.GetData()) > 0 {
		return dense.GetData()
	}
	//nolint:staticcheck // read for servers that predate the Dense field
	if data := vector.GetData(); len(data) > 0 {
		return data
	}
	return nil
}

// GetNearest retrieves the nearest points to a vector.
func (s *QdrantStore) GetNearest(ctx context.Context, namespace string, vector []float32, queries []Query, selectFields []string, threshold float64, limit int64) ([]SearchResult, error) {
	filter := buildQdrantFilter(queries)

	searchLimit := uint64(limit)
	if limit <= 0 {
		searchLimit = 10
	}

	searchResult, err := s.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: namespace,
		Query:          qdrant.NewQuery(vector...),
		Filter:         filter,
		Limit:          &searchLimit,
		WithPayload:    qdrant.NewWithPayload(true),
		ScoreThreshold: qdrant.PtrOf(float32(threshold)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search points: %w", qdrantNamespaceError(namespace, err))
	}

	results := make([]SearchResult, 0, len(searchResult))
	for _, point := range searchResult {
		score := float64(point.Score)
		results = append(results, SearchResult{
			ID:         pointIDToString(point.Id),
			Score:      &score,
			Properties: filterProperties(payloadToMap(point.Payload), selectFields),
		})
	}

	return results, nil
}

// Add stores a new point in the Qdrant vector store.
func (s *QdrantStore) Add(ctx context.Context, namespace string, id string, embedding []float32, metadata map[string]interface{}) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("id is required")
	}

	pointID, err := parsePointID(id)
	if err != nil {
		return fmt.Errorf("invalid id format (must be UUID): %w", err)
	}

	point := &qdrant.PointStruct{
		Id:      pointID,
		Payload: mapToPayload(metadata),
	}
	if len(embedding) > 0 {
		point.Vectors = qdrant.NewVectors(embedding...)
	}

	_, err = s.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: namespace,
		Points:         []*qdrant.PointStruct{point},
		Wait:           qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to upsert point: %w", qdrantNamespaceError(namespace, err))
	}
	return nil
}

// Namespace-bound RPCs return NotFound when their collection is missing. A
// missing point is an empty successful Get response, handled separately above.
func qdrantNamespaceError(namespace string, err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %q: %w", ErrNamespaceNotFound, namespace, err)
	}
	return err
}

// Delete removes a point from the Qdrant vector store.
func (s *QdrantStore) Delete(ctx context.Context, namespace string, id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("id is required")
	}

	pointID, err := parsePointID(id)
	if err != nil {
		return fmt.Errorf("invalid id format: %w", err)
	}

	_, err = s.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: namespace,
		Points:         qdrant.NewPointsSelector(pointID),
	})
	return err
}

// DeleteAll removes multiple points matching the filter.
func (s *QdrantStore) DeleteAll(ctx context.Context, namespace string, queries []Query) ([]DeleteResult, error) {
	filter := buildQdrantFilter(queries)

	scrollResult, err := s.client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: namespace,
		Filter:         filter,
		WithPayload:    qdrant.NewWithPayload(false),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scroll points: %w", err)
	}

	if len(scrollResult) == 0 {
		return []DeleteResult{}, nil
	}

	results := make([]DeleteResult, len(scrollResult))
	for i, point := range scrollResult {
		results[i] = DeleteResult{
			ID:     pointIDToString(point.Id),
			Status: DeleteStatusSuccess,
		}
	}

	var deleteErr error
	if filter != nil {
		_, deleteErr = s.client.Delete(ctx, &qdrant.DeletePoints{
			CollectionName: namespace,
			Points:         qdrant.NewPointsSelectorFilter(filter),
		})
	} else {
		pointIDs := make([]*qdrant.PointId, len(scrollResult))
		for i, point := range scrollResult {
			pointIDs[i] = point.Id
		}
		_, deleteErr = s.client.Delete(ctx, &qdrant.DeletePoints{
			CollectionName: namespace,
			Points:         qdrant.NewPointsSelectorIDs(pointIDs),
		})
	}

	if deleteErr != nil {
		for i := range results {
			results[i].Status = DeleteStatusError
			results[i].Error = deleteErr.Error()
		}
	}

	return results, nil
}

// DeleteByFilter implements FilteredDeleter without a client-side ID snapshot.
// Equality accepts strings, booleans and signed integers; range comparisons
// accept finite numbers within the signed 64-bit domain, without integer precision loss.
// Other operators are rejected rather than silently weakening the filter.
func (s *QdrantStore) DeleteByFilter(ctx context.Context, namespace string, queries []Query) error {
	if strings.TrimSpace(namespace) == "" {
		return fmt.Errorf("namespace is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	filter, err := buildQdrantDeleteFilter(queries)
	if err != nil {
		return err
	}
	result, err := s.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: namespace,
		Points:         qdrant.NewPointsSelectorFilter(filter),
		Wait:           qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to delete filtered points in collection %q: %w", namespace, err)
	}
	if result.GetStatus() != qdrant.UpdateStatus_Completed {
		return fmt.Errorf("filtered deletion in collection %q was not confirmed complete: %s", namespace, result.GetStatus())
	}
	return nil
}

// buildQdrantDeleteFilter fails closed. The read-path filter builder tolerates
// unsupported values by dropping conditions, which is unsafe for deletion.
func buildQdrantDeleteFilter(queries []Query) (*qdrant.Filter, error) {
	if len(queries) == 0 {
		return nil, fmt.Errorf("conditional deletion requires a non-empty filter")
	}
	conditions := make([]*qdrant.Condition, 0, len(queries))
	for i, query := range queries {
		if strings.TrimSpace(query.Field) == "" {
			return nil, fmt.Errorf("delete filter %d: field is required", i)
		}
		switch query.Operator {
		case QueryOperatorEqual, QueryOperatorNotEqual:
			switch query.Value.(type) {
			case string, bool, int, int32, int64:
			default:
				return nil, fmt.Errorf("delete filter %d: unsupported equality value %T", i, query.Value)
			}
		case QueryOperatorGreaterThan, QueryOperatorGreaterThanOrEqual, QueryOperatorLessThan, QueryOperatorLessThanOrEqual:
			var number float64
			var integer int64
			switch value := query.Value.(type) {
			case int:
				integer, number = int64(value), float64(value)
			case int32:
				integer, number = int64(value), float64(value)
			case int64:
				integer, number = value, float64(value)
			case float32:
				number = float64(value)
			case float64:
				number = value
			default:
				return nil, fmt.Errorf("delete filter %d: unsupported range value %T", i, query.Value)
			}
			if integer > 1<<53 || integer < -(1<<53) || math.IsNaN(number) || math.IsInf(number, 0) {
				return nil, fmt.Errorf("delete filter %d: range value is not a finite, precisely representable number", i)
			}
			// An integer index clamps out-of-domain floating bounds. Reject them
			// before dispatch so that conversion cannot widen a destructive filter.
			if number >= float64(1<<63) || number < -float64(1<<63) {
				return nil, fmt.Errorf("delete filter %d: range value is outside the signed 64-bit integer domain", i)
			}
			query.Value = number
		default:
			return nil, fmt.Errorf("delete filter %d: unsupported operator %q", i, query.Operator)
		}
		condition := buildQdrantCondition(query)
		if condition == nil {
			return nil, fmt.Errorf("delete filter %d: could not build condition", i)
		}
		conditions = append(conditions, condition)
	}
	return &qdrant.Filter{Must: conditions}, nil
}

// Close closes the Qdrant client connection.
func (s *QdrantStore) Close(ctx context.Context, namespace string) error {
	return s.client.Close()
}

// RequiresVectors returns true because Qdrant is a dedicated vector database
// that requires vectors for all points/entries.
func (s *QdrantStore) RequiresVectors() bool {
	return true
}

// newQdrantStore creates a new Qdrant vector store.
func newQdrantStore(_ context.Context, config *QdrantConfig, logger schemas.Logger) (*QdrantStore, error) {
	if config == nil {
		return nil, fmt.Errorf("qdrant configuration is required")
	}
	if strings.TrimSpace(config.Host.GetValue()) == "" {
		return nil, fmt.Errorf("qdrant host is required")
	}
	maxRecvMsgSize := qdrantMaxRecvMsgSize
	if mb := config.MaxRecvMsgSizeMB.CoerceInt(0); mb > 0 {
		maxRecvMsgSize = mb * 1024 * 1024
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   config.Host.GetValue(),
		Port:                   config.Port.CoerceInt(6334),
		APIKey:                 config.APIKey.GetValue(),
		UseTLS:                 config.UseTLS.CoerceBool(false),
		SkipCompatibilityCheck: true,
		GrpcOptions: []grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgSize)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create qdrant client: %w", err)
	}

	// Client setup is local. Optional cache reachability is checked explicitly
	// through Ping or namespace operations, so an outage cannot block startup.
	return &QdrantStore{
		client: client,
		logger: logger,
	}, nil
}

func parsePointID(id string) (*qdrant.PointId, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, err
	}
	return qdrant.NewID(id), nil
}

func pointIDToString(id *qdrant.PointId) string {
	if id == nil {
		return ""
	}
	switch v := id.PointIdOptions.(type) {
	case *qdrant.PointId_Uuid:
		return v.Uuid
	case *qdrant.PointId_Num:
		return fmt.Sprintf("%d", v.Num)
	default:
		return ""
	}
}

func payloadToMap(payload map[string]*qdrant.Value) map[string]interface{} {
	if payload == nil {
		return make(map[string]interface{})
	}

	result := make(map[string]interface{}, len(payload))
	for k, v := range payload {
		result[k] = valueToInterface(v)
	}
	return result
}

func valueToInterface(v *qdrant.Value) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.Kind.(type) {
	case *qdrant.Value_StringValue:
		return val.StringValue
	case *qdrant.Value_IntegerValue:
		return val.IntegerValue
	case *qdrant.Value_DoubleValue:
		return val.DoubleValue
	case *qdrant.Value_BoolValue:
		return val.BoolValue
	case *qdrant.Value_ListValue:
		list := make([]interface{}, len(val.ListValue.Values))
		for i, item := range val.ListValue.Values {
			list[i] = valueToInterface(item)
		}
		return list
	case *qdrant.Value_StructValue:
		return payloadToMap(val.StructValue.Fields)
	default:
		return nil
	}
}

func mapToPayload(m map[string]interface{}) map[string]*qdrant.Value {
	if m == nil {
		return make(map[string]*qdrant.Value)
	}
	// Convert []string to []interface{} since Qdrant's NewValueMap doesn't handle []string directly
	converted := make(map[string]interface{}, len(m))
	for k, v := range m {
		switch val := v.(type) {
		case []string:
			// Convert []string to []interface{}
			interfaceSlice := make([]interface{}, len(val))
			for i, s := range val {
				interfaceSlice[i] = s
			}
			converted[k] = interfaceSlice
		default:
			converted[k] = v
		}
	}
	return qdrant.NewValueMap(converted)
}

func filterProperties(props map[string]interface{}, selectFields []string) map[string]interface{} {
	if len(selectFields) == 0 {
		return props
	}
	filtered := make(map[string]interface{}, len(selectFields))
	for _, field := range selectFields {
		if val, ok := props[field]; ok {
			filtered[field] = val
		}
	}
	return filtered
}

func buildQdrantFilter(queries []Query) *qdrant.Filter {
	if len(queries) == 0 {
		return nil
	}

	var conditions []*qdrant.Condition
	for _, q := range queries {
		condition := buildQdrantCondition(q)
		if condition != nil {
			conditions = append(conditions, condition)
		}
	}

	if len(conditions) == 0 {
		return nil
	}

	return &qdrant.Filter{
		Must: conditions,
	}
}

func buildQdrantCondition(q Query) *qdrant.Condition {
	field := q.Field

	switch q.Operator {
	case QueryOperatorEqual:
		return buildMatchCondition(field, q.Value)
	case QueryOperatorNotEqual:
		matchCond := buildMatchCondition(field, q.Value)
		if matchCond == nil {
			return nil
		}
		return qdrant.NewFilterAsCondition(&qdrant.Filter{
			MustNot: []*qdrant.Condition{matchCond},
		})
	case QueryOperatorGreaterThan:
		return buildRangeCondition(field, q.Value, "gt")
	case QueryOperatorGreaterThanOrEqual:
		return buildRangeCondition(field, q.Value, "gte")
	case QueryOperatorLessThan:
		return buildRangeCondition(field, q.Value, "lt")
	case QueryOperatorLessThanOrEqual:
		return buildRangeCondition(field, q.Value, "lte")
	case QueryOperatorIsNull:
		return qdrant.NewIsNull(field)
	case QueryOperatorIsNotNull:
		return qdrant.NewFilterAsCondition(&qdrant.Filter{
			MustNot: []*qdrant.Condition{qdrant.NewIsNull(field)},
		})
	case QueryOperatorContainsAny:
		switch v := q.Value.(type) {
		case []string:
			return qdrant.NewMatchKeywords(field, v...)
		case []int:
			int64s := make([]int64, len(v))
			for i, val := range v {
				int64s[i] = int64(val)
			}
			return qdrant.NewMatchInts(field, int64s...)
		case []int64:
			return qdrant.NewMatchInts(field, v...)
		}
		return buildMatchCondition(field, q.Value)
	case QueryOperatorContainsAll:
		if values, ok := q.Value.([]interface{}); ok {
			var mustConditions []*qdrant.Condition
			for _, v := range values {
				cond := buildMatchCondition(field, v)
				if cond != nil {
					mustConditions = append(mustConditions, cond)
				}
			}
			if len(mustConditions) > 0 {
				return qdrant.NewFilterAsCondition(&qdrant.Filter{
					Must: mustConditions,
				})
			}
		}
		return buildMatchCondition(field, q.Value)
	case QueryOperatorLike:
		if str, ok := q.Value.(string); ok {
			return qdrant.NewMatchText(field, str)
		}
		return nil
	default:
		return buildMatchCondition(field, q.Value)
	}
}

func buildMatchCondition(field string, value interface{}) *qdrant.Condition {
	switch v := value.(type) {
	case string:
		return qdrant.NewMatchKeyword(field, v)
	case int:
		return qdrant.NewMatchInt(field, int64(v))
	case int32:
		return qdrant.NewMatchInt(field, int64(v))
	case int64:
		return qdrant.NewMatchInt(field, v)
	case bool:
		return qdrant.NewMatchBool(field, v)
	default:
		return qdrant.NewMatchKeyword(field, fmt.Sprintf("%v", v))
	}
}

func buildRangeCondition(field string, value interface{}, op string) *qdrant.Condition {
	var floatVal float64
	switch v := value.(type) {
	case int:
		floatVal = float64(v)
	case int32:
		floatVal = float64(v)
	case int64:
		floatVal = float64(v)
	case float32:
		floatVal = float64(v)
	case float64:
		floatVal = v
	default:
		return nil
	}

	r := &qdrant.Range{}
	switch op {
	case "gt":
		r.Gt = qdrant.PtrOf(floatVal)
	case "gte":
		r.Gte = qdrant.PtrOf(floatVal)
	case "lt":
		r.Lt = qdrant.PtrOf(floatVal)
	case "lte":
		r.Lte = qdrant.PtrOf(floatVal)
	}
	return qdrant.NewRange(field, r)
}
