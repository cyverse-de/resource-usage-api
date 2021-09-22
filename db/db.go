package db

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

type Database struct {
	db *sqlx.DB
}

type CPUHours struct {
	ID             string    `db:"id" json:"id"`
	UserID         string    `db:"user_id" json:"user_id"`
	Username       string    `db:"username" json:"username"`
	Total          int64     `db:"total" json:"total"`
	EffectiveStart time.Time `db:"effective_start" json:"effective_start"`
	EffectiveEnd   time.Time `db:"effective_end" json:"effective_end"`
	LastModified   time.Time `db:"last_modified" json:"last_modified"`
}

type CPUUsageEvent struct {
	ID            string `db:"id" json:"id"`
	RecordDate    string `db:"record_date" json:"record_date"`
	EffectiveDate string `db:"effective_date" json:"effective_date"`
	EventType     string `db:"event_type" json:"event_type"`
	Value         int64  `db:"value" json:"value"`
	CreatedBy     string `db:"created_by" json:"created_by"`
	LastModified  string `db:"last_modified" json:"last_modified"`
}

func (d *Database) CurrentCPUHoursForUser(context context.Context, username string) (*CPUHours, error) {
	var cpuHours CPUHours
	err := d.db.QueryRowxContext(context, currentCPUHoursForUserQuery, username).Scan(&cpuHours)
	return &cpuHours, err
}

func (d *Database) AllCPUHoursForUser(context context.Context, username string) ([]CPUHours, error) {
	var cpuHours []CPUHours

	rows, err := d.db.QueryxContext(context, allCPUHoursForUserQuery, username)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var h CPUHours
		err = rows.StructScan(&h)
		if err != nil {
			return nil, err
		}
		cpuHours = append(cpuHours, h)
	}

	if err = rows.Err(); err != nil {
		return cpuHours, err
	}

	return cpuHours, nil
}

func (d *Database) AdminAllCurrentCPUHours(context context.Context) ([]CPUHours, error) {
	var cpuHours []CPUHours

	rows, err := d.db.QueryxContext(context, currentCPUHoursQuery)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var h CPUHours
		err = rows.StructScan(&h)
		if err != nil {
			return nil, err
		}
		cpuHours = append(cpuHours, h)
	}

	if err = rows.Err(); err != nil {
		return cpuHours, err
	}

	return cpuHours, nil
}

func (d *Database) AdminAllCPUHours(context context.Context) ([]CPUHours, error) {
	var cpuHours []CPUHours

	rows, err := d.db.QueryxContext(context, allCPUHoursQuery)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var h CPUHours
		err = rows.StructScan(&h)
		if err != nil {
			return nil, err
		}
		cpuHours = append(cpuHours, h)
	}

	if err = rows.Err(); err != nil {
		return cpuHours, err
	}

	return cpuHours, nil
}
