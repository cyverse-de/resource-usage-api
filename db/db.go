package db

import "github.com/jmoiron/sqlx"

type Database struct {
	db *sqlx.DB
}
