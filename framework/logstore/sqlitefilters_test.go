package logstore

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maximhq/bifrost/framework/queryscope"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestSQLiteFilterIndexesPreserveResultsAndCoverQueries(t *testing.T) {
	s := newScopedDBTestLogStore(t)
	ctx := context.Background()
	require.NoError(t, s.db.AutoMigrate(&Log{}))
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	now := time.Now().UTC()
	for i, row := range []struct {
		model, id, name, object string
		age                     int
	}{
		{"model-a", "key-a", "Old name", "responses", 1},
		{"model-a", "key-a", "New name", "responses", 0},
		{"model-a", "key-a", "New name", "responses", 0},
		{"model-b", "key-b", "Second", "responses", 0},
		{"expired", "key-expired", "Expired", "responses", 31},
		{"hidden", "key-hidden", "Hidden", "embedding", 0},
		{"", "", "", "responses", 0},
	} {
		id, name := row.id, row.name
		require.NoError(t, s.Create(ctx, &Log{ID: fmt.Sprint(i), Model: row.model, Timestamp: now.AddDate(0, 0, -row.age), Status: "success", Object: row.object,
			SelectedKeyID: id, SelectedKeyName: name, VirtualKeyID: &id, VirtualKeyName: &name}))
	}
	visible := context.WithValue(ctx, HiddenRequestTypesContextKey, []string{"embedding"})
	read := func() map[string]any {
		models, err := s.GetDistinctModels(visible, 1000, "")
		require.NoError(t, err)
		require.Equal(t, []string{"model-a", "model-b"}, models)
		out := map[string]any{"models": models}
		for _, prefix := range []string{"selected_key", "virtual_key"} {
			pairs, err := s.GetDistinctKeyPairs(visible, prefix+"_id", prefix+"_name", 1000, "")
			require.NoError(t, err)
			require.Equal(t, []KeyPairResult{{ID: "key-a", Name: "New name"}, {ID: "key-a", Name: "Old name"}, {ID: "key-b", Name: "Second"}}, pairs)
			out[prefix] = pairs
			pairs, err = s.GetDistinctKeyPairs(visible, prefix+"_id", prefix+"_name", 1, "NAME")
			require.NoError(t, err)
			require.Equal(t, []KeyPairResult{{ID: "key-a", Name: "New name"}}, pairs)
			scoped := queryscope.WithQueryScope(visible, func(db *gorm.DB) *gorm.DB { return db.Where("model = ?", "model-b") })
			pairs, err = s.GetDistinctKeyPairs(scoped, prefix+"_id", prefix+"_name", 1000, "")
			require.NoError(t, err)
			require.Equal(t, []KeyPairResult{{ID: "key-b", Name: "Second"}}, pairs)
		}
		return out
	}
	before := read()
	require.NoError(t, migrationAddSQLiteFilterIndexes(ctx, s.db, testLogger{}))
	require.Equal(t, before, read())
	require.NoError(t, migrationAddSQLiteFilterIndexes(ctx, s.db, testLogger{}), "migration can be retried")

	// Inspect the actual SQL the unchanged store methods execute, including
	// hidden-type filtering. Neither the payload table nor temporary sorting is
	// needed for the normal unscoped model/key dropdown.
	var sql string
	var args []any
	require.NoError(t, s.db.Callback().Query().After("gorm:query").Register("capture_filter_query", func(db *gorm.DB) {
		sql, args = db.Statement.SQL.String(), append([]any(nil), db.Statement.Vars...)
	}))
	for _, dimension := range []string{"models", "selected_key", "virtual_key"} {
		if dimension == "models" {
			_, err := s.GetDistinctModels(visible, 1000, "")
			require.NoError(t, err)
		} else {
			_, err := s.GetDistinctKeyPairs(visible, dimension+"_id", dimension+"_name", 1000, "")
			require.NoError(t, err)
		}
		var plan []struct{ Detail string }
		require.NoError(t, s.db.Raw("EXPLAIN QUERY PLAN "+sql, args...).Scan(&plan).Error)
		var details []string
		for _, row := range plan {
			details = append(details, row.Detail)
		}
		require.Contains(t, strings.Join(details, "\n"), "COVERING INDEX idx_logs_filter_", dimension)
		require.NotContains(t, strings.Join(details, "\n"), "TEMP B-TREE", dimension)
	}
}

// BenchmarkSQLiteFilterData measures the store calls the dashboard's lazy
// model/key dropdowns make. Payloads deliberately exceed a SQLite page: these
// queries need only scalar metadata, even when older logs retained content.
func BenchmarkSQLiteFilterData(b *testing.B) {
	ctx := context.Background()
	store := sqliteFilterBenchmarkStore(b)
	for _, dimension := range []string{"models", "selected_keys", "virtual_keys"} {
		b.Run(dimension, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if dimension == "models" {
					models, err := store.GetDistinctModels(ctx, 1000, "")
					if err != nil || len(models) != 20 {
						b.Fatal(fmt.Errorf("models=%d: %v", len(models), err))
					}
				} else {
					id, name, count := "selected_key_id", "selected_key_name", 30
					if dimension == "virtual_keys" {
						id, name, count = "virtual_key_id", "virtual_key_name", 40
					}
					pairs, err := store.GetDistinctKeyPairs(ctx, id, name, 1000, "")
					if err != nil || len(pairs) != count {
						b.Fatal(fmt.Errorf("pairs=%d: %v", len(pairs), err))
					}
				}
			}
		})
	}
}

func sqliteFilterBenchmarkStore(b *testing.B) *RDBLogStore {
	b.Helper()
	ctx := context.Background()
	store, err := newSqliteLogStore(ctx, &SQLiteConfig{Path: filepath.Join(b.TempDir(), "logs.db")}, testLogger{})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, store.Close(ctx)) })
	require.NoError(b, store.db.Exec(`WITH RECURSIVE rows(n) AS (
		SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n < 50000
	) INSERT INTO logs (id, timestamp, created_at, object_type, provider, model, status,
		selected_key_id, selected_key_name, virtual_key_id, virtual_key_name, output_message)
	SELECT printf('request-%d',n), ?, ?, 'chat_completion', 'openai', printf('model-%02d',n%20), 'success',
		printf('key-%02d',n%30), printf('Key %02d',n%30), printf('vk-%02d',n%40), printf('Virtual %02d',n%40),
		printf('%05000d',n) FROM rows`, time.Now().UTC(), time.Now().UTC()).Error)
	return store
}

// Compare the asynchronous log writer with and without the new B-trees. The
// database already contains 50k requests, so this includes index maintenance.
func BenchmarkSQLiteFilterIndexWrites(b *testing.B) {
	for _, indexes := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexes=%v", indexes), func(b *testing.B) {
			store := sqliteFilterBenchmarkStore(b)
			if !indexes {
				for _, index := range sqliteFilterIndexes {
					require.NoError(b, store.db.Exec("DROP INDEX "+index.name).Error)
				}
			}
			ctx := context.Background()
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				id, name := "vk-00", "Virtual 00"
				if err := store.Create(ctx, &Log{ID: fmt.Sprintf("new-%d", i), Timestamp: time.Now().UTC(), Object: "chat_completion", Provider: "openai", Model: "model-00", Status: "success", SelectedKeyID: "key-00", SelectedKeyName: "Key 00", VirtualKeyID: &id, VirtualKeyName: &name}); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}
