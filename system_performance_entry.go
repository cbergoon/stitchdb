package stitchdb

import "time"

type SystemPerformanceEntry struct {
	Transaction    bool          `json:"transaction"`
	Mode           RWMode        `json:"mode"`
	Bucket         string        `json:"bucket"`
	Commit         bool          `json:"commit"`
	Rollback       bool          `json:"rollback"`
	SyncTime       time.Duration `json:"SyncTime"`
	TxTime         time.Duration `json:"TxTime"`
	ManageTime     time.Duration `json:"manageTime"`
	ManageSynced   bool          `json:"manageSynced"`
	ManageSyncTime time.Duration `json:"manageSyncTime"`
}
