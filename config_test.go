// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"testing"
	"time"
)

func TestPersist(t *testing.T) {
	config, err := NewConfig(Persist)
	if err != nil {
		t.Errorf("Failure: NewConfig(Persist) returned error \"%v\"", err)
	}
	if config == nil {
		t.Errorf("Failure: NewConfig(Persist) returned nil config")
	}
	if config.persist != true {
		t.Errorf("Failure: NewConfig(Persist) expected config.persist == true got config.persist == %v", config.persist)
	}
}

func TestDirPath(t *testing.T) {
	config, err := NewConfig(DirPath("/path/to/dir"))
	if err != nil {
		t.Errorf("Failure: NewConfig(DirPath(\"/path/to/dir\")) returned error \"%v\"", err)
	}
	if config == nil {
		t.Errorf("Failure: NewConfig(DirPath(\"/path/to/dir\")) returned nil config")
	}
	if config.dirPath != "/path/to/dir" {
		t.Errorf("Failure: NewConfig(DirPath(\"/path/to/dir\")) expected config.dirPath == \"/path/to/dir\" got config.dirPath == %v", config.dirPath)
	}
}

func TestSync(t *testing.T) {
	config, err := NewConfig(Sync(NONE))
	if err != nil {
		t.Errorf("Failure: NewConfig(Sync(NONE)) returned error \"%v\"", err)
	}
	if config == nil {
		t.Errorf("Failure: NewConfig(Sync(NONE)) returned nil config")
	}
	if config.syncFreq != NONE {
		t.Errorf("Failure: NewConfig(Sync(NONE)) expected config.syncFreq == NONE got config.syncFreq == %v", config.syncFreq)
	}
}

func TestBucketFileMultLimit(t *testing.T) {
	config, err := NewConfig(BucketFileMultLimit(10))
	if err != nil {
		t.Errorf("Failure: NewConfig(Sync(NONE)) returned error \"%v\"", err)
	}
	if config == nil {
		t.Errorf("Failure: NewConfig(Sync(NONE)) returned nil config")
	}
	if config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewConfig(Sync(NONE)) expected config.bucketFileMultLimit == 10 got config.bucketFileMultLimit == %v", config.bucketFileMultLimit)
	}
}

func TestManageFrequency(t *testing.T) {
	config, err := NewConfig(ManageFrequency(time.Second))
	if err != nil {
		t.Errorf("Failure: NewConfig(ManageFrequency(time.Second)) returned error \"%v\"", err)
	}
	if config == nil {
		t.Errorf("Failure: NewConfig(ManageFrequency(time.Second)) returned nil config")
	}
	if config.manageFrequency != time.Second {
		t.Errorf("Failure: NewConfig(ManageFrequency(time.Second)) expected config.manageFrequency == 10 got config.manageFrequency == %v", config.manageFrequency)
	}
}

func TestDeveloper(t *testing.T) {
	config, err := NewConfig(Developer)
	if err != nil {
		t.Errorf("Failure: NewConfig(Developer) returned error \"%v\"", err)
	}
	if config == nil {
		t.Errorf("Failure: NewConfig(Developer) returned nil config")
	}
	if config.developer != true {
		t.Errorf("Failure: NewConfig(Developer) expected config.developer == true got config.developer == %v", config.developer)
	}
}

func TestPerformanceMonitor(t *testing.T) {
	config, err := NewConfig(PerformanceMonitor)
	if err != nil {
		t.Errorf("Failure: NewConfig(PerformanceMonitor) returned error \"%v\"", err)
	}
	if config == nil {
		t.Errorf("Failure: NewConfig(PerformanceMonitor) returned nil config")
	}
	if config.performanceMonitor != true {
		t.Errorf("Failure: NewConfig(PerformanceMonitor) expected config.performanceMonitor == true got config.performanceMonitor == %v", config.performanceMonitor)
	}
}

func TestNewConfig(t *testing.T) {
	config, err := NewConfig(PerformanceMonitor)
	if err != nil {
		t.Errorf("Failure: NewConfig(PerformanceMonitor) returned error \"%v\"", err)
	}
	if config == nil {
		t.Errorf("Failure: NewConfig(PerformanceMonitor) returned nil config")
	}
}
