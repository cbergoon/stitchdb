// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"testing"
	"time"
)

func TestNewStitchDB(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("path/to/loc/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	NewStitchDB(c)
}

func TestStitchDB_Open(t *testing.T) {

}

func TestStitchDB_Close(t *testing.T) {

}

func TestStitchDB_GetConfig(t *testing.T) {

}

func TestStitchDB_SetConfig(t *testing.T) {

}

func TestStitchDB_View(t *testing.T) {

}

func TestStitchDB_Update(t *testing.T) {

}

func TestStitchDB_CreateBucket(t *testing.T) {

}

func TestStitchDB_DropBucket(t *testing.T) {

}
