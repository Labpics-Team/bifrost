package configstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/migrator"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrAdaptiveRevisionConflict = errors.New("adaptive routing revision changed")
	ErrIncidentNotFound         = errors.New("adaptive routing incident not found")
	ErrInvalidIncidentQuery     = errors.New("invalid adaptive routing incident query")
)

type AdaptiveCondition = tables.TableAdaptiveCondition
type Incident = tables.TableAdaptiveIncident
type IncidentEvent = tables.TableAdaptiveIncidentEvent

type IncidentQuery struct {
	Provider, Model, KeyID, Status, Scope, Search, Sort, Cursor string
	From, To                                                    time.Time
	Limit                                                       int
}

type IncidentPage struct {
	Incidents  []Incident `json:"incidents"`
	NextCursor string     `json:"next_cursor,omitempty"`
	Total      int64      `json:"total"`
}

type IncidentDetail struct {
	Incident   Incident        `json:"incident"`
	Events     []IncidentEvent `json:"events"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type AdaptiveState struct {
	Revision      uint64
	Conditions    []AdaptiveCondition
	OpenIncidents []Incident
	Checkpoint    []byte
}

type AdaptiveMutation struct {
	ExpectedRevision, Revision uint64
	UpsertConditions           []AdaptiveCondition
	DeleteConditionIDs         []string
	UpsertIncidents            []Incident
	Events                     []IncidentEvent
}

// AdaptiveRoutingStore uses the native pool and transaction boundary. It adds
// no worker, global cache, or per-request writes. A single runtime writer owns
// lifecycle decisions; the CAS also fences late checkpoints and stale writes.
type AdaptiveRoutingStore struct{ store ConfigStore }

func NewAdaptiveRoutingStore(store ConfigStore) (*AdaptiveRoutingStore, error) {
	if store == nil || store.DB() == nil {
		return nil, errors.New("adaptive routing requires a config store")
	}
	return &AdaptiveRoutingStore{store: store}, nil
}

func migrationAddAdaptiveRoutingState(ctx context.Context, db *gorm.DB, logger schemas.Logger) error {
	return RunSingleMigration(ctx, nil, db, logger, &migrator.Migration{
		ID: "add_adaptive_routing_state",
		Migrate: func(tx *gorm.DB) error {
			if err := tx.WithContext(ctx).AutoMigrate(&tables.TableAdaptiveRoutingState{}, &Incident{}, &AdaptiveCondition{}, &IncidentEvent{}); err != nil {
				return err
			}
			return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&tables.TableAdaptiveRoutingState{ID: 1}).Error
		},
		Rollback: func(*gorm.DB) error {
			return errors.New("adaptive routing tables are additive; dropping them would destroy unresolved conditions and incident history")
		},
	})
}

func (s *AdaptiveRoutingStore) Load(ctx context.Context) (AdaptiveState, error) {
	var state AdaptiveState
	err := s.store.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var meta tables.TableAdaptiveRoutingState
		if err := tx.First(&meta, 1).Error; err != nil {
			return err
		}
		state.Revision = meta.Revision
		if meta.CheckpointRevision == meta.Revision {
			state.Checkpoint = meta.Checkpoint
		}
		if err := tx.Order("id").Find(&state.Conditions).Error; err != nil {
			return err
		}
		return tx.Where("closed_ns IS NULL").Order("opened_ns, id").Find(&state.OpenIncidents).Error
	})
	return state, err
}

// Apply commits an entire lifecycle transition before the runtime publishes it.
// A failed event/condition write rolls back the revision and all incident edits.
func (s *AdaptiveRoutingStore) Apply(ctx context.Context, m AdaptiveMutation) error {
	if m.Revision != m.ExpectedRevision+1 || m.Revision == 0 {
		return errors.New("adaptive mutation requires the next revision")
	}
	return s.store.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&tables.TableAdaptiveRoutingState{}).Where("id = ? AND revision = ?", 1, m.ExpectedRevision).Update("revision", m.Revision)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrAdaptiveRevisionConflict
		}
		if len(m.DeleteConditionIDs) > 0 {
			if err := tx.Where("id IN ?", m.DeleteConditionIDs).Delete(&AdaptiveCondition{}).Error; err != nil {
				return err
			}
		}
		for _, incident := range m.UpsertIncidents {
			if incident.ID == "" || incident.OpenedAt.IsZero() {
				return errors.New("incident requires identity and opening time")
			}
			incident.OpenedAt = incident.OpenedAt.UTC()
			incident.UpdatedAt = incident.UpdatedAt.UTC()
			incident.LastSeen = incident.LastSeen.UTC()
			incident.OpenedNS = incident.OpenedAt.UnixNano()
			incident.ResolvedNS = nil
			incident.ClosedNS = nil
			if incident.ResolvedAt != nil {
				resolved := incident.ResolvedAt.UTC()
				ns := resolved.UnixNano()
				incident.ResolvedAt = &resolved
				incident.ResolvedNS = &ns
			}
			if incident.ClosedAt != nil {
				closed := incident.ClosedAt.UTC()
				ns := closed.UnixNano()
				incident.ClosedAt = &closed
				incident.ClosedNS = &ns
			}
			if incident.ResolvedAt != nil && incident.ClosedAt == nil {
				return errors.New("recovered incident requires a closing time")
			}
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&incident).Error; err != nil {
				return err
			}
		}
		for _, condition := range m.UpsertConditions {
			if condition.ID == "" || condition.IncidentID == "" || condition.KeyRevision == "" {
				return errors.New("condition requires identity, incident and credential revision")
			}
			var count int64
			if err := tx.Model(&Incident{}).Where("id = ? AND closed_ns IS NULL", condition.IncidentID).Count(&count).Error; err != nil {
				return err
			}
			if count != 1 {
				return errors.New("condition requires an unresolved incident")
			}
			condition.HoldUntil = condition.HoldUntil.UTC()
			condition.HeldAt = condition.HeldAt.UTC()
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&condition).Error; err != nil {
				return err
			}
		}
		for _, event := range m.Events {
			if event.ID == "" || event.IncidentID == "" || event.Timestamp.IsZero() {
				return errors.New("incident event requires identity, incident and timestamp")
			}
			var count int64
			if err := tx.Model(&Incident{}).Where("id = ?", event.IncidentID).Count(&count).Error; err != nil {
				return err
			}
			if count != 1 {
				return errors.New("event incident does not exist")
			}
			event.Timestamp = event.Timestamp.UTC()
			event.TimestampNS = event.Timestamp.UnixNano()
			if err := tx.Create(&event).Error; err != nil {
				return err
			}
		}
		for _, incident := range m.UpsertIncidents {
			if incident.ClosedAt == nil {
				continue
			}
			var count int64
			if err := tx.Model(&AdaptiveCondition{}).Where("incident_id = ?", incident.ID).Count(&count).Error; err != nil {
				return err
			}
			if count != 0 {
				return errors.New("cannot resolve an incident while its condition is active")
			}
		}
		return nil
	})
}

// Checkpoint never replaces conditions or incidents. Its caller captures the
// revision together with telemetry; an older snapshot cannot cross a transition.
func (s *AdaptiveRoutingStore) Checkpoint(ctx context.Context, expectedRevision uint64, data []byte) error {
	if len(data) > 1<<20 {
		return errors.New("adaptive telemetry checkpoint exceeds 1 MiB")
	}
	r := s.store.DB().WithContext(ctx).Model(&tables.TableAdaptiveRoutingState{}).Where("id = ? AND revision = ?", 1, expectedRevision).Updates(map[string]any{"checkpoint": data, "checkpoint_revision": expectedRevision})
	if r.Error != nil {
		return r.Error
	}
	if r.RowsAffected != 1 {
		return ErrAdaptiveRevisionConflict
	}
	return nil
}

type incidentCursor struct {
	Version int    `json:"v"`
	Filter  string `json:"f"`
	AsOf    int64  `json:"a"`
	Value   int64  `json:"n"`
	Rank    int    `json:"r"`
	ID      string `json:"id"`
}

func encodeIncidentCursor(c incidentCursor) string {
	data, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(data)
}
func decodeIncidentCursor(raw string) (incidentCursor, error) {
	var c incidentCursor
	if len(raw) > 2048 {
		return c, ErrInvalidIncidentQuery
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return c, ErrInvalidIncidentQuery
	}
	if json.Unmarshal(data, &c) != nil || c.Version != 1 || c.ID == "" {
		return c, ErrInvalidIncidentQuery
	}
	return c, nil
}

func normalizeIncidentQuery(q IncidentQuery) (IncidentQuery, error) {
	if q.Limit == 0 {
		q.Limit = 25
	}
	if q.Limit < 1 || q.Limit > 100 {
		return q, ErrInvalidIncidentQuery
	}
	if q.Sort == "" {
		q.Sort = "newest"
	}
	switch q.Sort {
	case "newest", "oldest", "attention", "longest":
	default:
		return q, ErrInvalidIncidentQuery
	}
	if !q.From.IsZero() && !q.To.IsZero() && q.From.After(q.To) {
		return q, ErrInvalidIncidentQuery
	}
	if len(q.Search) > 512 {
		return q, ErrInvalidIncidentQuery
	}
	return q, nil
}

func incidentQueryFingerprint(q IncidentQuery) string {
	q.Cursor = ""
	q.Limit = 0
	data, _ := json.Marshal(q)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
func incidentFilters(db *gorm.DB, q IncidentQuery) *gorm.DB {
	for _, f := range [][2]string{{"provider", q.Provider}, {"model", q.Model}, {"key_id", q.KeyID}, {"status", q.Status}, {"scope", q.Scope}} {
		if f[1] != "" {
			db = db.Where(f[0]+" = ?", f[1])
		}
	}
	if !q.From.IsZero() {
		db = db.Where("opened_ns >= ?", q.From.UnixNano())
	}
	if !q.To.IsZero() {
		db = db.Where("opened_ns <= ?", q.To.UnixNano())
	}
	if q.Search != "" {
		p := "%" + strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(strings.ToLower(q.Search)) + "%"
		db = db.Where("LOWER(summary) LIKE ? ESCAPE '\\' OR LOWER(provider) LIKE ? ESCAPE '\\' OR LOWER(model) LIKE ? ESCAPE '\\' OR LOWER(key_name) LIKE ? ESCAPE '\\'", p, p, p, p)
	}
	return db
}

// ListIncidents uses keyset pagination; nanosecond integers avoid RFC3339Nano
// string ordering errors. A cursor binds its filters and keeps the duration's
// reference time constant while browsing. It does not freeze live incident state.
func (s *AdaptiveRoutingStore) ListIncidents(ctx context.Context, q IncidentQuery) (IncidentPage, error) {
	page := IncidentPage{Incidents: []Incident{}}
	q, err := normalizeIncidentQuery(q)
	if err != nil {
		return page, err
	}
	c := incidentCursor{Version: 1, Filter: incidentQueryFingerprint(q), AsOf: time.Now().UnixNano()}
	if q.Cursor != "" {
		c, err = decodeIncidentCursor(q.Cursor)
		if err != nil || c.Filter != incidentQueryFingerprint(q) {
			return page, ErrInvalidIncidentQuery
		}
	}
	base := incidentFilters(s.store.DB().WithContext(ctx).Model(&Incident{}), q)
	if err := base.Session(&gorm.Session{}).Count(&page.Total).Error; err != nil {
		return page, err
	}
	db := base.Session(&gorm.Session{})
	expr := "opened_ns"
	dir := "DESC"
	op := "<"
	if q.Sort == "oldest" {
		dir = "ASC"
		op = ">"
	}
	if q.Sort == "longest" {
		expr = fmt.Sprintf("(COALESCE(closed_ns, %d) - opened_ns)", c.AsOf)
	}
	if q.Sort == "attention" {
		rank := "CASE WHEN closed_ns IS NULL THEN 0 ELSE 1 END"
		if q.Cursor != "" {
			db = db.Where("("+rank+") > ? OR (("+rank+") = ? AND (opened_ns < ? OR (opened_ns = ? AND id < ?)))", c.Rank, c.Rank, c.Value, c.Value, c.ID)
		}
		db = db.Order(rank + " ASC")
	} else if q.Cursor != "" {
		db = db.Where(expr+" "+op+" ? OR ("+expr+" = ? AND id "+op+" ?)", c.Value, c.Value, c.ID)
	}
	if err := db.Order(expr + " " + dir).Order("id " + dir).Limit(q.Limit + 1).Find(&page.Incidents).Error; err != nil {
		return page, err
	}
	if len(page.Incidents) > q.Limit {
		page.Incidents = page.Incidents[:q.Limit]
		last := page.Incidents[q.Limit-1]
		c.ID = last.ID
		c.Value = last.OpenedNS
		c.Rank = 0
		if last.ClosedNS != nil {
			c.Rank = 1
		}
		if q.Sort == "longest" {
			end := c.AsOf
			if last.ClosedNS != nil {
				end = *last.ClosedNS
			}
			c.Value = end - last.OpenedNS
		}
		page.NextCursor = encodeIncidentCursor(c)
	}
	return page, nil
}

func (s *AdaptiveRoutingStore) GetIncident(ctx context.Context, id, cursor string, limit int) (IncidentDetail, error) {
	detail := IncidentDetail{Events: []IncidentEvent{}}
	if limit == 0 {
		limit = 25
	}
	if limit < 1 || limit > 100 {
		return detail, ErrInvalidIncidentQuery
	}
	if err := s.store.DB().WithContext(ctx).First(&detail.Incident, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return detail, ErrIncidentNotFound
		}
		return detail, err
	}
	db := s.store.DB().WithContext(ctx).Where("incident_id = ?", id)
	if cursor != "" {
		c, err := decodeIncidentCursor(cursor)
		if err != nil || c.Filter != id {
			return detail, ErrInvalidIncidentQuery
		}
		db = db.Where("timestamp_ns > ? OR (timestamp_ns = ? AND id > ?)", c.Value, c.Value, c.ID)
	}
	if err := db.Order("timestamp_ns, id").Limit(limit + 1).Find(&detail.Events).Error; err != nil {
		return detail, err
	}
	if len(detail.Events) > limit {
		detail.Events = detail.Events[:limit]
		last := detail.Events[limit-1]
		detail.NextCursor = encodeIncidentCursor(incidentCursor{Version: 1, Filter: id, Value: last.TimestampNS, ID: last.ID})
	}
	return detail, nil
}

// PruneIncidents deletes only closed lifecycles, in bounded batches. It cannot
// delete active conditions, even if the supplied retention cutoff is in the future.
func (s *AdaptiveRoutingStore) PruneIncidents(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit < 1 || limit > 1000 {
		return 0, errors.New("incident retention batch must be between 1 and 1000")
	}
	var removed int64
	err := s.store.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ids []string
		if err := tx.Model(&Incident{}).Where("closed_ns < ? AND NOT EXISTS (SELECT 1 FROM adaptive_routing_conditions c WHERE c.incident_id = adaptive_routing_incidents.id)", before.UnixNano()).Order("closed_ns, id").Limit(limit).Pluck("id", &ids).Error; err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		if err := tx.Where("incident_id IN ?", ids).Delete(&IncidentEvent{}).Error; err != nil {
			return err
		}
		r := tx.Where("id IN ?", ids).Delete(&Incident{})
		removed = r.RowsAffected
		return r.Error
	})
	return removed, err
}
