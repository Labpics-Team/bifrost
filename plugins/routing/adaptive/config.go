// Package adaptive ranks configured routes from real provider attempts. It never
// sends probes, changes credentials, or grants access to a provider.
package adaptive

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
)

// Config uses Bifrost's existing load_balancer_config vocabulary. Settings are
// independent: disabling provider selection still permits adaptive key selection.
type Config struct {
	DirectionSelectionEnabled bool   `json:"direction_selection_enabled"`
	RouteSelectionEnabled     bool   `json:"route_selection_enabled"`
	AppendFallbacksToPinned   bool   `json:"append_fallbacks_to_pinned"`
	RerouteFailedDirections   bool   `json:"reroute_failed_directions"`
	RerouteTarget             string `json:"reroute_target"`
	PruneFailedFallbacks      bool   `json:"prune_failed_fallbacks"`
	HistoryEnabled            bool   `json:"history_enabled"`
	HistoryRetentionDays      int    `json:"history_retention_days"`
	SnapshotIntervalSeconds   int    `json:"snapshot_persist_interval_seconds"`
}

func DefaultConfig() Config {
	return Config{DirectionSelectionEnabled: true, RouteSelectionEnabled: true,
		RerouteTarget: "pool", HistoryEnabled: true, HistoryRetentionDays: 7, SnapshotIntervalSeconds: 300}
}

func DisabledConfig() Config {
	c := DefaultConfig()
	c.DirectionSelectionEnabled, c.RouteSelectionEnabled = false, false
	return c
}

// ParseFileConfig applies the documented partial config.json defaults and
// ranges. The PUT API validates a complete replacement instead.
func ParseFileConfig(data []byte) (Config, error) {
	defaults := DefaultConfig()
	c := defaults
	if err := json.Unmarshal(data, &c); err != nil {
		return c, err
	}
	if c.HistoryRetentionDays <= 0 {
		c.HistoryRetentionDays = defaults.HistoryRetentionDays
	}
	if c.SnapshotIntervalSeconds < 0 {
		c.SnapshotIntervalSeconds = defaults.SnapshotIntervalSeconds
	}
	c.HistoryRetentionDays = max(1, min(30, c.HistoryRetentionDays))
	c.SnapshotIntervalSeconds = max(5, min(86400, c.SnapshotIntervalSeconds))
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.RerouteTarget != "pool" && c.RerouteTarget != "fallbacks" {
		return fmt.Errorf("reroute_target must be pool or fallbacks")
	}
	if c.HistoryRetentionDays < 1 || c.HistoryRetentionDays > 30 {
		return fmt.Errorf("history_retention_days must be between 1 and 30")
	}
	if c.SnapshotIntervalSeconds < 5 || c.SnapshotIntervalSeconds > 86400 {
		return fmt.Errorf("snapshot_persist_interval_seconds must be between 5 and 86400")
	}
	return nil
}

type configValue struct{ value atomic.Pointer[Config] }

func (v *configValue) load() Config   { return *v.value.Load() }
func (v *configValue) store(c Config) { v.value.Store(&c) }
