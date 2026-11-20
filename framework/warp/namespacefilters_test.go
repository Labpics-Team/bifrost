package warp

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/maximhq/bifrost/framework/vectorstore"
	"github.com/stretchr/testify/require"
)

// A strict store refuses query filters that namespace provisioning did not
// declare as filterable. Exercise the search path against that contract so a
// new filter cannot silently depend on an unindexed payload field.
func TestWarpSearchDeclaresItsQueryFilterIndexes(t *testing.T) {
	ctx := context.Background()
	vectors := &warpFilterSchemaStore{fakeWarpVectorStore: newFakeWarpVectorStore()}
	config := validWarpConfigRow()
	_, err := ensureWarpNamespace(ctx, vectors, config.LogVectorStoreNamespace, config.EmbeddingDimension)
	require.NoError(t, err)
	vectors.fail = func(queries []vectorstore.Query) error {
		for _, query := range queries {
			if !vectors.properties[query.Field].Filterable {
				return fmt.Errorf("filter property %q has no declared index", query.Field)
			}
		}
		return nil
	}
	searcher := NewSemanticSearcher(&recordingStore{row: config}, vectors, zeroEmbeddingExecutor, &semanticLogReader{})
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	minLatency, maxLatency, minCost, maxCost := 1.0, 10.0, 0.001, 0.01
	filters := &logstore.SearchFilters{
		StartTime: &start, EndTime: &end,
		Providers: []string{"openai"}, Models: []string{"test-model"}, Status: []string{"success"},
		VirtualKeyIDs: []string{"key"}, UserIDs: []string{"user"}, Apps: []string{"app"}, SessionID: "session",
		TeamIDs: []string{"team"}, CustomerIDs: []string{"customer"}, BusinessUnitIDs: []string{"unit"},
		MinLatency: &minLatency, MaxLatency: &maxLatency, MinCost: &minCost, MaxCost: &maxCost,
	}
	t.Run("search filters", func(t *testing.T) {
		_, err := searcher.Search(ctx, []string{"a request"}, filters, 1)
		require.NoError(t, err)
	})
	t.Run("visibility filters", func(t *testing.T) {
		visible := &LogVisibility{
			UserIDs: []string{"user"}, VirtualKeyIDs: []string{"key"}, TeamIDs: []string{"team"},
			BusinessUnitIDs: []string{"unit"}, CustomerIDs: []string{"customer"},
		}
		_, err := searcher.SearchVisible(ctx, []string{"a request"}, nil, visible, 1)
		require.NoError(t, err)
	})
}

type warpFilterSchemaStore struct {
	*fakeWarpVectorStore
	properties map[string]vectorstore.VectorStoreProperties
}

func (s *warpFilterSchemaStore) CreateNamespace(ctx context.Context, namespace string, dimension int, properties map[string]vectorstore.VectorStoreProperties) error {
	s.properties = maps.Clone(properties)
	return s.fakeWarpVectorStore.CreateNamespace(ctx, namespace, dimension, properties)
}
