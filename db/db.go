package db

import (
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
