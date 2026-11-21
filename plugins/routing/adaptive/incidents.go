package adaptive

import (
	"context"
	"github.com/maximhq/bifrost/framework/configstore"
	"time"
)

var ErrIncidentNotFound = configstore.ErrIncidentNotFound
var ErrInvalidIncidentQuery = configstore.ErrInvalidIncidentQuery

type Incident = configstore.Incident
type IncidentEvent = configstore.IncidentEvent
type IncidentQuery = configstore.IncidentQuery
type IncidentPage = configstore.IncidentPage
type IncidentDetail = configstore.IncidentDetail

type PersistenceStatus struct {
	Healthy       bool       `json:"healthy"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
}

func (c *Controller) PersistenceStatus() PersistenceStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.persistence
}
func (c *Controller) ListIncidents(ctx context.Context, q IncidentQuery) (IncidentPage, error) {
	if c.stateStore == nil {
		return IncidentPage{Incidents: []Incident{}}, nil
	}
	return c.stateStore.ListIncidents(ctx, q)
}
func (c *Controller) GetIncident(ctx context.Context, id, cursor string, limit int) (IncidentDetail, error) {
	if c.stateStore == nil {
		return IncidentDetail{}, ErrIncidentNotFound
	}
	return c.stateStore.GetIncident(ctx, id, cursor, limit)
}
