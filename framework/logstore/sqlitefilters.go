package logstore

import (
	"context"
	"fmt"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/migrator"
	"gorm.io/gorm"
)

// Dropdowns sort by their displayed value, then deduplicate. The timestamp and
// object type must also be in the index: reading either from logs visits the
// content-bearing row for every request, not just each distinct model or key.
// Existing (key ID, timestamp) indexes continue to serve filtered log lists.
var sqliteFilterIndexes = []struct {
	name      string
	columns   string
	predicate string
}{
	{"idx_logs_filter_model", "model, timestamp, object_type", "model IS NOT NULL AND model != ''"},
	{"idx_logs_filter_selected_key", "selected_key_name, selected_key_id, timestamp, object_type", "selected_key_id IS NOT NULL AND selected_key_id != '' AND selected_key_name IS NOT NULL AND selected_key_name != ''"},
	{"idx_logs_filter_virtual_key", "virtual_key_name, virtual_key_id, timestamp, object_type", "virtual_key_id IS NOT NULL AND virtual_key_id != '' AND virtual_key_name IS NOT NULL AND virtual_key_name != ''"},
}

func migrationAddSQLiteFilterIndexes(ctx context.Context, db *gorm.DB, _ schemas.Logger) error {
	return migrator.New(db, migrator.DefaultOptions, []*migrator.Migration{{
		ID: "logs_add_sqlite_filter_indexes",
		Migrate: func(tx *gorm.DB) error {
			if tx.Dialector.Name() != "sqlite" {
				return nil
			}
			for _, index := range sqliteFilterIndexes {
				if err := tx.WithContext(ctx).Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON logs(%s) WHERE %s", index.name, index.columns, index.predicate)).Error; err != nil {
					return fmt.Errorf("create filter index %s: %w", index.name, err)
				}
			}
			return nil
		},
		Rollback: func(tx *gorm.DB) error {
			if tx.Dialector.Name() != "sqlite" {
				return nil
			}
			for _, index := range sqliteFilterIndexes {
				if err := tx.WithContext(ctx).Exec("DROP INDEX IF EXISTS " + index.name).Error; err != nil {
					return fmt.Errorf("drop filter index %s: %w", index.name, err)
				}
			}
			return nil
		},
	}}).Migrate()
}
