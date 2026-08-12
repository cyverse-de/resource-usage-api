package clients

import "time"

// UserDataUsage contains a user's current data store usage.
type UserDataUsage struct {
	ID           string     `json:"id"`
	UserID       string     `json:"user_id"`
	Username     string     `json:"username"`
	Total        int64      `json:"total"`
	Time         *time.Time `json:"time"`
	LastModified *time.Time `json:"last_modified"`
}
