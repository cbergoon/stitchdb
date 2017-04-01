// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"strconv"
	"testing"
	"time"
)

func TestNewStitchDB(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
}

func TestStitchDB_Open(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	db.Open()
	if !db.open {
		t.Errorf("Failure: db.Open() expected db to be open got db.open == false")
	}
}

func TestStitchDB_Close(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	db.Open()
	if !db.open {
		t.Errorf("Failure: db.Open() expected db to be open got db.open == false")
	}
	time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestStitchDB_GetConfig(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Errorf("Failure: db.GetConfig() returned config not equal to original config")
	}
}

func TestStitchDB_SetConfig(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	cnew, _ := NewConfig(Persist, DirPath("new/stitch/test/db"), Sync(EACH), ManageFrequency(2*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(100))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	db.SetConfig(cnew)
	if db.GetConfig() != cnew {
		t.Errorf("Failure: db.SetConfig(cnew) returned config not equal to new config")
	}
	db.SetConfig(c)
	if db.GetConfig() != c {
		t.Errorf("Failure: db.SetConfig() returned config not equal to original config")
	}
}

func TestStitchDB_View(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Errorf("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Errorf("Failure: db.Open() expected db to be open got db.open == false")
	}
	count := 0
	db.View("test", func(t *Tx) error {
		err := t.Ascend("", func(e *Entry) bool {
			count++
			return true
		})
		return err
	})
	if count != 256 {
		t.Errorf("Failure: db.View(...) iteration with transaction failed")
	}
	time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestStitchDB_Update(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Errorf("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Errorf("Failure: db.Open() expected db to be open got db.open == false")
	}
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-", "{ \"value\":\"test\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	db.Update("test", func(t *Tx) error {
		_, err := t.Set(e)
		return err
	})
	count := 0
	db.View("test", func(t *Tx) error {
		err := t.Ascend("", func(e *Entry) bool {
			count++
			return true
		})
		return err
	})
	if count != 257 {
		t.Errorf("Failure: db.View(...) iteration with transaction failed")
	}
	db.Update("test", func(t *Tx) error {
		_, err := t.Delete(e)
		return err
	})
	count = 0
	db.View("test", func(t *Tx) error {
		err := t.Ascend("", func(e *Entry) bool {
			count++
			return true
		})
		return err
	})
	if count != 256 {
		t.Errorf("Failure: db.View(...) iteration with transaction failed")
	}
	time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestStitchDB_CreateBucket(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Errorf("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Errorf("Failure: db.Open() expected db to be open got db.open == false")
	}
	opts, _ := NewBucketOptions(BTreeDegree(32))
	db.CreateBucket("new", opts)
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-", "{ \"value\":\"test\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	db.Update("new", func(t *Tx) error {
		_, err := t.Set(e)
		return err
	})
	count := 0
	db.View("new", func(t *Tx) error {
		err := t.Ascend("", func(e *Entry) bool {
			count++
			return true
		})
		return err
	})
	if count != 1 {
		t.Errorf("Failure: db.View(...) iteration with transaction failed")
	}
	db.Update("new", func(t *Tx) error {
		_, err := t.Delete(e)
		return err
	})
	count = 0
	db.View("new", func(t *Tx) error {
		err := t.Ascend("", func(e *Entry) bool {
			count++
			return true
		})
		return err
	})
	if count != 0 {
		t.Errorf("Failure: db.View(...) iteration with transaction failed")
	}
	time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestStitchDB_DropBucket(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Errorf("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Errorf("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Errorf("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Errorf("Failure: db.Open() expected db to be open got db.open == false")
	}
	opts, _ := NewBucketOptions(BTreeDegree(32))
	db.CreateBucket("new", opts)
	err = db.DropBucket("new")
	if err != nil {
		t.Errorf("Failure: db.DropBucket(\"new\") returned error \"%v\"", err)
	}
	time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}
