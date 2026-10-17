package office

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type GrowthReport struct {
	Database           string            `json:"database"`
	Schema             string            `json:"schema"`
	CollectedAt        time.Time         `json:"collectedAt"`
	TotalEventBytes    int64             `json:"totalEventBytes"`
	EstimatedEventRows int64             `json:"estimatedEventRows"`
	Partitions         []PartitionGrowth `json:"partitions"`
	Detailed           *DetailedGrowth   `json:"detailed,omitempty"`
}

type PartitionGrowth struct {
	Name          string `json:"name"`
	Bytes         int64  `json:"bytes"`
	EstimatedRows int64  `json:"estimatedRows"`
}

type DetailedGrowth struct {
	OldestReceivedAt *time.Time           `json:"oldestReceivedAt"`
	NewestReceivedAt *time.Time           `json:"newestReceivedAt"`
	Organizations    []OrganizationGrowth `json:"organizations"`
}

type OrganizationGrowth struct {
	OrganizationID   string     `json:"organizationId"`
	EventCount       int64      `json:"eventCount"`
	OldestReceivedAt *time.Time `json:"oldestReceivedAt"`
	NewestReceivedAt *time.Time `json:"newestReceivedAt"`
}

// CollectGrowthReport reports PostgreSQL event storage without changing it.
// Detailed mode performs a full aggregate over the append-only event table.
func CollectGrowthReport(ctx context.Context, db *sql.DB, detailed bool) (GrowthReport, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return GrowthReport{}, fmt.Errorf("begin read-only growth report transaction: %w", err)
	}
	defer tx.Rollback()

	var report GrowthReport
	if err := tx.QueryRowContext(ctx, `SELECT current_database(), current_schema(), clock_timestamp(), pg_total_relation_size('office_events'::regclass)`).Scan(
		&report.Database, &report.Schema, &report.CollectedAt, &report.TotalEventBytes,
	); err != nil {
		return GrowthReport{}, fmt.Errorf("read event storage summary: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT child.relname, pg_total_relation_size(child.oid), COALESCE(stats.n_live_tup, 0)::bigint
		FROM pg_inherits AS inheritance
		JOIN pg_class AS parent ON parent.oid = inheritance.inhparent
		JOIN pg_class AS child ON child.oid = inheritance.inhrelid
		LEFT JOIN pg_stat_user_tables AS stats ON stats.relid = child.oid
		WHERE parent.oid = 'office_events'::regclass
		ORDER BY child.relname`)
	if err != nil {
		return GrowthReport{}, fmt.Errorf("read event partition statistics: %w", err)
	}
	report.Partitions = make([]PartitionGrowth, 0, 8)
	for rows.Next() {
		var partition PartitionGrowth
		if err := rows.Scan(&partition.Name, &partition.Bytes, &partition.EstimatedRows); err != nil {
			_ = rows.Close()
			return GrowthReport{}, fmt.Errorf("scan event partition statistics: %w", err)
		}
		report.EstimatedEventRows += partition.EstimatedRows
		report.Partitions = append(report.Partitions, partition)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return GrowthReport{}, fmt.Errorf("iterate event partition statistics: %w", err)
	}
	if err := rows.Close(); err != nil {
		return GrowthReport{}, fmt.Errorf("close event partition statistics: %w", err)
	}

	if detailed {
		growth, err := collectDetailedGrowth(ctx, tx)
		if err != nil {
			return GrowthReport{}, err
		}
		report.Detailed = growth
	}
	if err := tx.Commit(); err != nil {
		return GrowthReport{}, fmt.Errorf("finish read-only growth report transaction: %w", err)
	}
	return report, nil
}

func collectDetailedGrowth(ctx context.Context, tx *sql.Tx) (*DetailedGrowth, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT organization_id, count(*)::bigint, min(received_at), max(received_at)
		FROM office_events
		GROUP BY organization_id
		ORDER BY organization_id`)
	if err != nil {
		return nil, fmt.Errorf("read detailed organization event aggregates: %w", err)
	}
	detailed := &DetailedGrowth{Organizations: make([]OrganizationGrowth, 0)}
	for rows.Next() {
		var organization OrganizationGrowth
		var oldest, newest sql.NullTime
		if err := rows.Scan(&organization.OrganizationID, &organization.EventCount, &oldest, &newest); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan detailed organization event aggregate: %w", err)
		}
		if oldest.Valid {
			value := oldest.Time
			organization.OldestReceivedAt = &value
			if detailed.OldestReceivedAt == nil || value.Before(*detailed.OldestReceivedAt) {
				detailed.OldestReceivedAt = &value
			}
		}
		if newest.Valid {
			value := newest.Time
			organization.NewestReceivedAt = &value
			if detailed.NewestReceivedAt == nil || value.After(*detailed.NewestReceivedAt) {
				detailed.NewestReceivedAt = &value
			}
		}
		detailed.Organizations = append(detailed.Organizations, organization)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate detailed organization event aggregates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close detailed organization event aggregates: %w", err)
	}
	return detailed, nil
}
