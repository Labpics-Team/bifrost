package tables

import "time"

// TableAdaptiveRoutingState is the single-writer commit revision. Checkpoints
// contain only disposable telemetry; active conditions have their own rows.
type TableAdaptiveRoutingState struct {
	ID                 uint   `gorm:"primaryKey;autoIncrement:false"`
	Revision           uint64 `gorm:"not null"`
	Checkpoint         []byte
	CheckpointRevision uint64 `gorm:"not null"`
}

func (TableAdaptiveRoutingState) TableName() string { return "adaptive_routing_state" }

// TableAdaptiveCondition is unresolved until a committed lifecycle transition
// removes it. HoldUntil schedules admission of a probe, never row expiry.
type TableAdaptiveCondition struct {
	ID           string `gorm:"primaryKey;type:varchar(64)"`
	Provider     string `gorm:"not null;uniqueIndex:idx_adaptive_condition_route,priority:1"`
	KeyID        string `gorm:"not null;uniqueIndex:idx_adaptive_condition_route,priority:2"`
	Model        string `gorm:"not null;uniqueIndex:idx_adaptive_condition_route,priority:3"`
	KeyRevision  string `gorm:"not null"`
	IncidentID   string `gorm:"not null;index"`
	FailureClass string
	HoldUntil    time.Time
	HeldAt       time.Time
	Rung         int
}

func (TableAdaptiveCondition) TableName() string { return "adaptive_routing_conditions" }

// TableAdaptiveIncident is the canonical lifecycle representation shared by
// storage and the diagnostic API. Numeric time columns are derived by the
// store for exact nanosecond ordering in SQLite and PostgreSQL alike.
type TableAdaptiveIncident struct {
	ID               string     `gorm:"primaryKey;type:varchar(64)" json:"id"`
	Scope            string     `gorm:"index" json:"scope"`
	Provider         string     `gorm:"index" json:"provider"`
	ProviderRevision string     `json:"-"`
	Model            string     `gorm:"index" json:"model,omitempty"`
	KeyID            string     `gorm:"index" json:"key_id,omitempty"`
	KeyRevision      string     `json:"-"`
	KeyName          string     `json:"key_name,omitempty"`
	Status           string     `gorm:"index" json:"status"`
	State            string     `json:"state"`
	WorstState       string     `json:"worst_state"`
	FailureClass     string     `json:"failure_class,omitempty"`
	OpenedAt         time.Time  `gorm:"autoCreateTime:false" json:"opened_at"`
	UpdatedAt        time.Time  `gorm:"autoUpdateTime:false" json:"updated_at"`
	LastSeen         time.Time  `json:"last_seen"`
	ResolvedAt       *time.Time `json:"resolved_at,omitempty"`
	ClosedAt         *time.Time `json:"closed_at,omitempty"`
	Summary          string     `json:"summary"`
	Action           string     `json:"action"`
	Checks           uint64     `json:"checks"`
	FailedChecks     uint64     `json:"failed_checks"`
	RefusedAgain     uint64     `json:"refused_again"`
	FirstRequestID   string     `json:"first_request_id,omitempty"`
	LastRequestID    string     `json:"last_request_id,omitempty"`
	OpenedNS         int64      `gorm:"not null;index:idx_adaptive_incident_opened" json:"-"`
	ResolvedNS       *int64     `gorm:"index:idx_adaptive_incident_resolved" json:"-"`
	ClosedNS         *int64     `gorm:"index:idx_adaptive_incident_closed" json:"-"`
}

func (TableAdaptiveIncident) TableName() string { return "adaptive_routing_incidents" }

type TableAdaptiveIncidentEvent struct {
	ID           string    `gorm:"primaryKey;type:varchar(64)" json:"id"`
	IncidentID   string    `gorm:"not null;index:idx_adaptive_event_incident_time,priority:1" json:"-"`
	Timestamp    time.Time `json:"timestamp"`
	TimestampNS  int64     `gorm:"not null;index:idx_adaptive_event_incident_time,priority:2" json:"-"`
	Kind         string    `json:"kind"`
	State        string    `json:"state,omitempty"`
	FailureClass string    `json:"failure_class,omitempty"`
	RequestID    string    `json:"request_id,omitempty"`
	Message      string    `json:"message"`
}

func (TableAdaptiveIncidentEvent) TableName() string { return "adaptive_routing_incident_events" }
