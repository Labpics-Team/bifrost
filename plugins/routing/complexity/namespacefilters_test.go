package complexity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"sync"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/vectorstore"
	"github.com/stretchr/testify/require"
)

// Keep the real warmup/classification path, but enforce the query/index
// contract required by strict vector stores on the local backend.
func TestSemanticClassifierDeclaresItsQueryFilterIndexes(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_generation=%v", legacy), func(t *testing.T) {
			ctx := context.Background()
			logger := bifrost.NewDefaultLogger(schemas.LogLevelError)
			base, err := vectorstore.NewVectorStore(ctx, &vectorstore.Config{
				Enabled: true, Type: vectorstore.VectorStoreTypeChromem, Config: vectorstore.ChromemConfig{},
			}, logger)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close(ctx, SemanticVectorStoreNamespace)) })
			config := testSemanticClassifierConfig(configstore.ComplexitySemanticVectorStoreConfigured)
			var legacyNamespace string
			if legacy {
				// Freeze the v1 format: its completion marker must not cause the
				// classifier to reuse metadata written before TAG encoding changed.
				exemplars := semanticExemplars(&config)
				sort.Slice(exemplars, func(i, j int) bool {
					if exemplars[i].Tier != exemplars[j].Tier {
						return exemplars[i].Tier < exemplars[j].Tier
					}
					return exemplars[i].Phrase < exemplars[j].Phrase
				})
				hash := sha256.New()
				_, _ = fmt.Fprintf(hash, "semantic-router-v1\x00%s\x00%s\x00%d\x00", config.Semantic.Provider, config.Semantic.EmbeddingModel, testSemanticDimension)
				for _, exemplar := range exemplars {
					_, _ = fmt.Fprintf(hash, "%s\x00%s\x00", exemplar.Tier, exemplar.Phrase)
				}
				fingerprint := hex.EncodeToString(hash.Sum(nil))
				legacyNamespace = semanticGenerationNamespace(fingerprint)
				properties := maps.Clone(semanticVectorStoreProperties)
				kind := properties[semanticMetadataKind]
				kind.Filterable = false
				properties[semanticMetadataKind] = kind
				require.NoError(t, base.CreateNamespace(ctx, legacyNamespace, testSemanticDimension, properties))
				for _, exemplar := range exemplars {
					vector, err := testSemanticEmbedding(ctx, config.Semantic, exemplar.Phrase)
					require.NoError(t, err)
					require.NoError(t, base.Add(ctx, legacyNamespace, semanticExemplarID(fingerprint, exemplar), vector, map[string]interface{}{
						semanticMetadataTier: exemplar.Tier, semanticMetadataKind: semanticMetadataKindExample, semanticMetadataFingerprint: fingerprint,
					}))
				}
				require.NoError(t, base.Add(ctx, legacyNamespace, semanticMarkerID(fingerprint), []float32{1, 0}, map[string]interface{}{
					semanticMetadataKind: semanticMetadataKindMarker, semanticMetadataFingerprint: fingerprint,
				}))
			}
			store := &semanticFilterSchemaStore{VectorStore: base, properties: make(map[string]map[string]vectorstore.VectorStoreProperties)}
			classifier := NewSemanticClassifier(ctx, logger)
			t.Cleanup(func() { require.NoError(t, classifier.Close()) })
			classifier.SetConfiguredStore(store)
			classifier.SetEmbeddingFunc(testSemanticEmbedding)
			classifier.Configure(&config)
			require.Eventually(t, func() bool { return classifier.Status().State == SemanticStatusReady }, time.Second, 10*time.Millisecond)
			if legacy {
				require.NotEqual(t, legacyNamespace, classifier.Status().Namespace, "legacy completion marker must not bypass the new metadata format")
			}
			result, err := classifier.Classify(ctx, ComplexityInput{LastUserText: "simple request"})
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, TierSimple, result.Tier)
		})
	}
}

type semanticFilterSchemaStore struct {
	vectorstore.VectorStore
	mu         sync.RWMutex
	properties map[string]map[string]vectorstore.VectorStoreProperties
}

func (s *semanticFilterSchemaStore) CreateNamespace(ctx context.Context, namespace string, dimension int, properties map[string]vectorstore.VectorStoreProperties) error {
	if err := s.VectorStore.CreateNamespace(ctx, namespace, dimension, properties); err != nil {
		return err
	}
	s.mu.Lock()
	s.properties[namespace] = maps.Clone(properties)
	s.mu.Unlock()
	return nil
}

func (s *semanticFilterSchemaStore) GetNearest(ctx context.Context, namespace string, vector []float32, queries []vectorstore.Query, fields []string, threshold float64, limit int64) ([]vectorstore.SearchResult, error) {
	s.mu.RLock()
	properties := s.properties[namespace]
	s.mu.RUnlock()
	for _, query := range queries {
		if !properties[query.Field].Filterable {
			return nil, fmt.Errorf("filter property %q has no declared index", query.Field)
		}
	}
	return s.VectorStore.GetNearest(ctx, namespace, vector, queries, fields, threshold, limit)
}
