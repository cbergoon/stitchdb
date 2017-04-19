package stitchdb

import "time"

type SystemPerformanceEntry struct {
	Transaction    bool          `json:"transaction"`
	Bucket         string        `json:"bucket"`
	Index          string        `json:"index"`
	Op             string        `json:"op"`
	OpTime         time.Duration `json:"opTime"`
	Commit         bool          `json:"commit"`
	Rollback       bool          `json:"rollback"`
	SyncTime       time.Duration `json:"SyncTime"`
	ManageTime     time.Duration `json:"manageTime"`
	ManageSynced   bool          `json:"manageSynced"`
	ManageSyncTime time.Duration `json:"manageSyncTime"`
}
