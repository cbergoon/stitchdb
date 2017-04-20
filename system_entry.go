package stitchdb

import "time"

type SystemEntry struct {
	InitialLoadTime time.Time     `json:"InitialLoadTime"`
	StartUpTime     time.Duration `json:"startUpTime"`
	LoadTime        time.Duration `json:"loadTime"`
	BucketCount     int           `json:"bucketCount"`
	BucketList      []string      `json:"bucketList"`
	DbManagerTime   time.Duration `json:"dbManagerTime"`
	Version         string        `json:"version"`
}
