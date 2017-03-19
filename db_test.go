package stitchdb

import (
	"testing"
	"time"
)

func TestNewStitchDB(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("path/to/loc/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	NewStitchDB(c)

}
