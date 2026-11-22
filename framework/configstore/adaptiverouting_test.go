package configstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func adaptiveTestStore(t *testing.T, path string) (*AdaptiveRoutingStore, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(path+"?_journal_mode=WAL&_synchronous=FULL&_foreign_keys=1&_txlock=immediate&_busy_timeout=1000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, migrationAddAdaptiveRoutingState(context.Background(), db, testMigrationLogger))
	base := &RDBConfigStore{}
	base.db.Store(db)
	s, err := NewAdaptiveRoutingStore(base)
	require.NoError(t, err)
	return s, db
}

func adaptiveOpening(id string, at time.Time) AdaptiveMutation {
	return AdaptiveMutation{ExpectedRevision: 0, Revision: 1,
		UpsertIncidents:  []Incident{{ID: id, Scope: "key", Provider: "openai", KeyID: "key-1", Status: "open", State: "held", OpenedAt: at, UpdatedAt: at, LastSeen: at, Summary: "rate limit", FirstRequestID: "request-1"}},
		UpsertConditions: []AdaptiveCondition{{ID: "condition-1", IncidentID: id, Provider: "openai", KeyID: "key-1", KeyRevision: "credential-v1", HoldUntil: at.Add(-time.Minute), HeldAt: at, Rung: 1}},
		Events:           []IncidentEvent{{ID: "event-1", IncidentID: id, Timestamp: at, Kind: "opened", RequestID: "request-1", Message: "Provider refused this key"}},
	}
}

func TestAdaptiveStoreAtomicLifecycleAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	s, db := adaptiveTestStore(t, filepath.Join(t.TempDir(), "config.db"))
	require.NoError(t, migrationAddAdaptiveRoutingState(ctx, db, testMigrationLogger))
	at := time.Now().UTC()
	m := adaptiveOpening("incident-1", at)
	require.NoError(t, s.Apply(ctx, m))
	require.NoError(t, s.Checkpoint(ctx, 1, []byte("sample1")))
	state, err := s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Revision)
	require.Len(t, state.Conditions, 1)
	// Probe eligibility in the past is not a reason to omit this unresolved hold.
	require.True(t, state.Conditions[0].HoldUntil.Before(time.Now()))
	resolved := at.Add(time.Minute)
	incident := m.UpsertIncidents[0]
	incident.Status = "resolved"
	incident.ResolvedAt = &resolved
	incident.ClosedAt = &resolved
	bad := AdaptiveMutation{ExpectedRevision: 1, Revision: 2, DeleteConditionIDs: []string{"condition-1"}, UpsertIncidents: []Incident{incident}, Events: []IncidentEvent{{ID: "event-1", IncidentID: incident.ID, Timestamp: resolved, Kind: "duplicate"}}}
	require.Error(t, s.Apply(ctx, bad)) // duplicate immutable event makes the whole transition roll back
	state, err = s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Revision)
	require.Len(t, state.Conditions, 1)
	require.Len(t, state.OpenIncidents, 1)
	bad.Events = []IncidentEvent{{ID: "event-2", IncidentID: incident.ID, Timestamp: resolved, Kind: "released", RequestID: "request-2"}}
	require.NoError(t, s.Apply(ctx, bad))
	require.ErrorIs(t, s.Checkpoint(ctx, 1, []byte("stale")), ErrAdaptiveRevisionConflict)
	require.ErrorIs(t, s.Apply(ctx, m), ErrAdaptiveRevisionConflict)
	state, err = s.Load(ctx)
	require.NoError(t, err)
	require.Empty(t, state.Conditions)
	require.Empty(t, state.OpenIncidents)
	require.Empty(t, state.Checkpoint) // an old revision's telemetry is not restored over a new lifecycle
	detail, err := s.GetIncident(ctx, incident.ID, "", 25)
	require.NoError(t, err)
	require.Len(t, detail.Events, 2)
	require.Equal(t, "request-2", detail.Events[1].RequestID)
	require.Equal(t, "resolved", detail.Incident.Status)
	require.Error(t, s.Checkpoint(ctx, 2, make([]byte, (1<<20)+1)))
}

func TestAdaptiveStoreConcurrentMutationHasOneWinner(t *testing.T) {
	s, _ := adaptiveTestStore(t, filepath.Join(t.TempDir(), "config.db"))
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan error, 2)
	start := make(chan struct{})
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			results <- s.Apply(ctx, adaptiveOpening(id, time.Now().UTC()))
		}(id)
	}
	close(start)
	wg.Wait()
	close(results)
	success, conflicts := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrAdaptiveRevisionConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	require.Equal(t, 1, success)
	require.Equal(t, 1, conflicts)
	state, err := s.Load(ctx)
	require.NoError(t, err)
	require.Len(t, state.Conditions, 1)
	require.Len(t, state.OpenIncidents, 1)
}

func TestAdaptiveIncidentIdentityMigrationPreservesLifecycle(t *testing.T) {
	ctx := context.Background()
	s, db := adaptiveTestStore(t, filepath.Join(t.TempDir(), "config.db"))
	m := adaptiveOpening("held-before-upgrade", time.Now().UTC())
	require.NoError(t, s.Apply(ctx, m))
	require.NoError(t, s.Checkpoint(ctx, 1, []byte("existing-telemetry")))
	// Recreate the actual pre-identity schema while keeping its data and indexes.
	// Neither additive migration may rebuild or clear lifecycle tables.
	require.NoError(t, db.Exec("ALTER TABLE adaptive_routing_incidents DROP COLUMN provider_revision").Error)
	require.NoError(t, db.Exec("ALTER TABLE adaptive_routing_incidents DROP COLUMN key_revision").Error)
	for range 2 {
		require.NoError(t, migrationAddAdaptiveProviderRevision(ctx, db, testMigrationLogger))
		require.NoError(t, migrationAddAdaptiveKeyRevision(ctx, db, testMigrationLogger))
	}
	state, err := s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Revision)
	require.Equal(t, []byte("existing-telemetry"), state.Checkpoint)
	require.Len(t, state.Conditions, 1)
	require.Equal(t, "credential-v1", state.Conditions[0].KeyRevision)
	require.Len(t, state.OpenIncidents, 1)
	incident := state.OpenIncidents[0]
	require.Empty(t, incident.ProviderRevision)
	require.Empty(t, incident.KeyRevision)
	incident.ProviderRevision = "provider-config-v2"
	incident.KeyRevision = "credential-v1"
	require.NoError(t, s.Apply(ctx, AdaptiveMutation{ExpectedRevision: 1, Revision: 2, UpsertIncidents: []Incident{incident}}))
	detail, err := s.GetIncident(ctx, incident.ID, "", 25)
	require.NoError(t, err)
	require.Equal(t, incident, detail.Incident)
	require.Len(t, detail.Events, 1)
	require.Equal(t, "request-1", detail.Events[0].RequestID)
	data, err := json.Marshal(detail.Incident)
	require.NoError(t, err)
	require.NotContains(t, string(data), "provider-config-v2")
	require.NotContains(t, string(data), "credential-v1")
	require.NotContains(t, string(data), "revision")
}

func TestAdaptiveStoreLongestPaginationSurvivesClosure(t *testing.T) {
	s, _ := adaptiveTestStore(t, filepath.Join(t.TempDir(), "config.db"))
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour)
	m := AdaptiveMutation{Revision: 1}
	for i, id := range []string{"a", "b", "c"} {
		at := base.Add(time.Duration(i) * time.Second)
		m.UpsertIncidents = append(m.UpsertIncidents, Incident{ID: id, Provider: "p", Scope: "key", Status: "open", OpenedAt: at, UpdatedAt: at})
	}
	require.NoError(t, s.Apply(ctx, m))
	q := IncidentQuery{Sort: "longest", Limit: 1}
	first, err := s.ListIncidents(ctx, q)
	require.NoError(t, err)
	require.Len(t, first.Incidents, 1)
	require.Equal(t, "a", first.Incidents[0].ID)
	q.Cursor = first.NextCursor
	cursor, err := decodeIncidentCursor(q.Cursor)
	require.NoError(t, err)
	// Close B and C after page one's reference time. A duration computed at
	// closure would move them ahead of A and silently omit them from page two.
	closed := time.Unix(0, cursor.AsOf).Add(time.Minute).UTC()
	for i := 1; i < len(m.UpsertIncidents); i++ {
		m.UpsertIncidents[i].Status = "resolved"
		m.UpsertIncidents[i].ClosedAt = &closed
		m.UpsertIncidents[i].ResolvedAt = &closed
	}
	require.NoError(t, s.Apply(ctx, AdaptiveMutation{ExpectedRevision: 1, Revision: 2, UpsertIncidents: m.UpsertIncidents[1:]}))
	for _, id := range []string{"b", "c"} {
		page, err := s.ListIncidents(ctx, q)
		require.NoError(t, err)
		require.Len(t, page.Incidents, 1)
		require.Equal(t, id, page.Incidents[0].ID)
		require.Equal(t, int64(3), page.Total)
		q.Cursor = page.NextCursor
		if id == "b" {
			next, err := decodeIncidentCursor(q.Cursor)
			require.NoError(t, err)
			require.Equal(t, cursor.AsOf, next.AsOf)
			require.Equal(t, cursor.AsOf-m.UpsertIncidents[1].OpenedAt.UnixNano(), next.Value)
		}
	}
	require.Empty(t, q.Cursor)
}

func TestAdaptiveStoreCrashHelper(t *testing.T) {
	path := os.Getenv("BIFROST_ADAPTIVE_CRASH_TEST_DB")
	if path == "" {
		t.Skip("subprocess helper")
	}
	s, db := adaptiveTestStore(t, path)
	ctx := context.Background()
	require.NoError(t, s.Apply(ctx, adaptiveOpening("crash-incident", time.Now().UTC())))
	// Leave a new transaction uncommitted. The process exits without closing any
	// connections, preserving the real WAL for the parent's recovery check.
	tx := db.Begin()
	require.NoError(t, tx.Error)
	require.NoError(t, tx.Model(&tables.TableAdaptiveRoutingState{}).Where("id = 1").Update("revision", 2).Error)
	require.NoError(t, tx.Where("id = ?", "condition-1").Delete(&AdaptiveCondition{}).Error)
	os.Exit(23)
}

func TestAdaptiveStoreRecoversCommittedLifecycleAfterCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestAdaptiveStoreCrashHelper$")
	cmd.Env = append(os.Environ(), "BIFROST_ADAPTIVE_CRASH_TEST_DB="+path)
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, string(output))
	require.Equal(t, 23, exitErr.ExitCode(), string(output))
	s, _ := adaptiveTestStore(t, path)
	state, err := s.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Revision)
	require.Len(t, state.Conditions, 1)
	require.Equal(t, "credential-v1", state.Conditions[0].KeyRevision)
	detail, err := s.GetIncident(context.Background(), "crash-incident", "", 25)
	require.NoError(t, err)
	require.Len(t, detail.Events, 1)
}

func TestAdaptiveStoreQueriesAndRetention(t *testing.T) {
	s, _ := adaptiveTestStore(t, filepath.Join(t.TempDir(), "config.db"))
	ctx := context.Background()
	base := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	m := AdaptiveMutation{Revision: 1}
	for _, row := range []struct {
		id    string
		delta time.Duration
	}{{"a", 100 * time.Millisecond}, {"b", 101 * time.Millisecond}, {"c", 101 * time.Millisecond}} {
		at := base.Add(row.delta)
		m.UpsertIncidents = append(m.UpsertIncidents, Incident{ID: row.id, Provider: "p", Scope: "key", Status: "open", OpenedAt: at, UpdatedAt: at, Summary: "100% refused_", KeyName: "Key"})
		m.Events = append(m.Events, IncidentEvent{ID: "e-" + row.id, IncidentID: "a", Timestamp: at, Kind: "check"})
	}
	require.NoError(t, s.Apply(ctx, m))
	q := IncidentQuery{Provider: "p", Sort: "oldest", Limit: 1, Search: "% refused_"}
	ids := []string{}
	for {
		page, err := s.ListIncidents(ctx, q)
		require.NoError(t, err)
		require.Equal(t, int64(3), page.Total)
		require.Len(t, page.Incidents, 1)
		ids = append(ids, page.Incidents[0].ID)
		if page.NextCursor == "" {
			break
		}
		q.Cursor = page.NextCursor
	}
	require.Equal(t, []string{"a", "b", "c"}, ids)
	q.Provider = "other"
	_, err := s.ListIncidents(ctx, q)
	require.ErrorIs(t, err, ErrInvalidIncidentQuery)
	_, err = s.ListIncidents(ctx, IncidentQuery{Cursor: "corrupt"})
	require.ErrorIs(t, err, ErrInvalidIncidentQuery)
	_, err = s.ListIncidents(ctx, IncidentQuery{Limit: 101})
	require.ErrorIs(t, err, ErrInvalidIncidentQuery)
	detail, err := s.GetIncident(ctx, "a", "", 1)
	require.NoError(t, err)
	require.Equal(t, "e-a", detail.Events[0].ID)
	detail, err = s.GetIncident(ctx, "a", detail.NextCursor, 1)
	require.NoError(t, err)
	require.Equal(t, "e-b", detail.Events[0].ID)
	_, err = s.GetIncident(ctx, "b", detail.NextCursor, 1)
	require.ErrorIs(t, err, ErrInvalidIncidentQuery)
	removed, err := s.PruneIncidents(ctx, base.Add(24*time.Hour), 100)
	require.NoError(t, err)
	require.Zero(t, removed)
	closed := m.UpsertIncidents[0]
	end := base.Add(time.Hour)
	closed.ResolvedAt = &end
	closed.ClosedAt = &end
	closed.Status = "resolved"
	require.NoError(t, s.Apply(ctx, AdaptiveMutation{ExpectedRevision: 1, Revision: 2, UpsertIncidents: []Incident{closed}}))
	page, err := s.ListIncidents(ctx, IncidentQuery{Sort: "attention"})
	require.NoError(t, err)
	require.Equal(t, "a", page.Incidents[2].ID)
	page, err = s.ListIncidents(ctx, IncidentQuery{Sort: "longest"})
	require.NoError(t, err)
	require.Len(t, page.Incidents, 3)
	removed, err = s.PruneIncidents(ctx, base.Add(24*time.Hour), 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), removed)
	_, err = s.GetIncident(ctx, "a", "", 25)
	require.ErrorIs(t, err, ErrIncidentNotFound)
	state, err := s.Load(ctx)
	require.NoError(t, err)
	require.Len(t, state.OpenIncidents, 2)
}

func TestAdaptiveStoreRejectsBrokenIncidentLinks(t *testing.T) {
	s, _ := adaptiveTestStore(t, filepath.Join(t.TempDir(), "config.db"))
	ctx := context.Background()
	m := adaptiveOpening("incident", time.Now().UTC())
	m.UpsertIncidents = nil
	require.Error(t, s.Apply(ctx, m))
	state, err := s.Load(ctx)
	require.NoError(t, err)
	require.Zero(t, state.Revision)
	require.Empty(t, state.Conditions)
	m = adaptiveOpening("incident", time.Now().UTC())
	require.NoError(t, s.Apply(ctx, m))
	resolved := time.Now().UTC()
	incident := m.UpsertIncidents[0]
	incident.ResolvedAt = &resolved
	incident.ClosedAt = &resolved
	incident.Status = "resolved"
	require.Error(t, s.Apply(ctx, AdaptiveMutation{ExpectedRevision: 1, Revision: 2, UpsertIncidents: []Incident{incident}}))
	state, err = s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Revision)
	require.Len(t, state.OpenIncidents, 1)
}

func TestAdaptiveStoreClosureWithoutRecovery(t *testing.T) {
	s, _ := adaptiveTestStore(t, filepath.Join(t.TempDir(), "config.db"))
	ctx := context.Background()
	m := adaptiveOpening("rotated", time.Now().UTC())
	require.NoError(t, s.Apply(ctx, m))
	incident := m.UpsertIncidents[0]
	closed := time.Now().UTC()
	incident.ClosedAt = &closed
	incident.Status = "no_longer_observed"
	require.NoError(t, s.Apply(ctx, AdaptiveMutation{ExpectedRevision: 1, Revision: 2, DeleteConditionIDs: []string{"condition-1"}, UpsertIncidents: []Incident{incident}}))
	detail, err := s.GetIncident(ctx, "rotated", "", 25)
	require.NoError(t, err)
	require.NotNil(t, detail.Incident.ClosedAt)
	require.Nil(t, detail.Incident.ResolvedAt)
	state, err := s.Load(ctx)
	require.NoError(t, err)
	require.Empty(t, state.OpenIncidents)
	removed, err := s.PruneIncidents(ctx, closed.Add(time.Hour), 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), removed)
}

func TestAdaptiveNativeConfigDurability(t *testing.T) {
	ctx := context.Background()
	store, err := NewConfigStore(ctx, &Config{Enabled: true, Type: ConfigStoreTypeSQLite, Config: &SQLiteConfig{Path: filepath.Join(t.TempDir(), "native.db")}}, testMigrationLogger)
	require.NoError(t, err)
	sqlDB, err := store.DB().DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	var synchronous int
	require.NoError(t, store.DB().Raw("PRAGMA synchronous").Scan(&synchronous).Error)
	require.Equal(t, 2, synchronous, "acknowledged config commits require WAL FULL")
	s, err := NewAdaptiveRoutingStore(store)
	require.NoError(t, err)
	state, err := s.Load(ctx)
	require.NoError(t, err)
	require.Zero(t, state.Revision)
	require.NoError(t, s.Apply(ctx, adaptiveOpening("native", time.Now().UTC())))
}

// This measures the exact lifecycle transaction with and without WAL sync.
// It is not an inference benchmark: healthy requests do not execute Apply.
func BenchmarkAdaptiveLifecyclePersistence(b *testing.B) {
	for _, mode := range []string{"NORMAL", "FULL"} {
		b.Run(mode, func(b *testing.B) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(b.TempDir(), "config.db")+"?_journal_mode=WAL&_synchronous="+mode+"&_txlock=immediate"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
			if err != nil {
				b.Fatal(err)
			}
			sqlDB, _ := db.DB()
			sqlDB.SetMaxOpenConns(1)
			b.Cleanup(func() { _ = sqlDB.Close() })
			if err := migrationAddAdaptiveRoutingState(context.Background(), db, testMigrationLogger); err != nil {
				b.Fatal(err)
			}
			base := &RDBConfigStore{}
			base.db.Store(db)
			s, _ := NewAdaptiveRoutingStore(base)
			m := adaptiveOpening("incident", time.Now().UTC())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.ExpectedRevision = uint64(i)
				m.Revision = uint64(i + 1)
				m.Events[0].ID = fmt.Sprintf("event-%d", i)
				if err := s.Apply(context.Background(), m); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
