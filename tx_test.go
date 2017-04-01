// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestTx_Ascend(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
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
		t.Error("Failure: t.Ascend(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_AscendGreaterOrEqual(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		return nil
	})
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-1", "{ \"value\":\"100\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	count := 0
	icount := 0
	err = db.View("test", func(t *Tx) error {
		err := t.AscendGreaterOrEqual("", e, func(e *Entry) bool {
			count++
			return true
		})
		err = t.AscendGreaterOrEqual("value", e, func(e *Entry) bool {
			icount++
			return true
		})
		return err
	})
	if count != 255 {
		t.Error("Failure: t.AscendGreaterOrEqual(...) unexpected iteration count")
	}
	if icount != 157 {
		t.Error("Failure: t.AscendGreaterOrEqual(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_AscendLessThan(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		return nil
	})
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-255", "{ \"value\":\"100\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	count := 0
	icount := 0
	err = db.View("test", func(t *Tx) error {
		err := t.AscendLessThan("", e, func(e *Entry) bool {
			count++
			return true
		})
		err = t.AscendLessThan("value", e, func(e *Entry) bool {
			icount++
			return true
		})
		return err
	})
	if count != 174 {
		t.Error("Failure: t.AscendLessThan(...) unexpected iteration count")
	}
	if icount != 99 {
		t.Error("Failure: t.AscendLessThan(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_AscendRange(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		return nil
	})
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-1", "{ \"value\":\"100\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	e1, _ := NewEntry("key-255", "{ \"value\":\"200\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	count := 0
	icount := 0
	err = db.View("test", func(t *Tx) error {
		err := t.AscendRange("", e, e1, func(e *Entry) bool {
			count++
			return true
		})
		err = t.AscendRange("value", e, e1, func(e *Entry) bool {
			icount++
			return true
		})
		return err
	})
	if count != 173 {
		t.Error("Failure: t.AscendRange(...) unexpected iteration count")
	}
	if icount != 100 {
		t.Error("Failure: t.AscendRange(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Descend(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	count := 0
	db.View("test", func(t *Tx) error {
		err := t.Descend("", func(e *Entry) bool {
			count++
			return true
		})
		return err
	})
	fmt.Println(count)
	if count != 256 {
		t.Error("Failure: t.Descend(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_DescendGreaterThan(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		return nil
	})
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-1", "{ \"value\":\"100\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	count := 0
	icount := 0
	err = db.View("test", func(t *Tx) error {
		err := t.DescendGreaterThan("", e, func(e *Entry) bool {
			count++
			return true
		})
		err = t.DescendGreaterThan("value", e, func(e *Entry) bool {
			icount++
			return true
		})
		return err
	})
	if count != 254 {
		t.Error("Failure: t.DescendGreaterThan(...) unexpected iteration count")
	}
	if icount != 156 {
		t.Error("Failure: t.DescendGreaterThan(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_DescendLessOrEqual(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		return nil
	})
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-255", "{ \"value\":\"100\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	count := 0
	icount := 0
	err = db.View("test", func(t *Tx) error {
		err := t.DescendLessOrEqual("", e, func(e *Entry) bool {
			count++
			return true
		})
		err = t.DescendLessOrEqual("value", e, func(e *Entry) bool {
			icount++
			return true
		})
		return err
	})
	if count != 175 {
		t.Error("Failure: t.DescendLessOrEqual(...) unexpected iteration count")
	}
	if icount != 100 {
		t.Error("Failure: t.DescendLessOrEqual(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_DescendRange(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		return nil
	})
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-1", "{ \"value\":\"100\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	e1, _ := NewEntry("key-255", "{ \"value\":\"200\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	count := 0
	icount := 0
	err = db.View("test", func(t *Tx) error {
		err := t.DescendRange("", e1, e, func(e *Entry) bool {
			count++
			return true
		})
		err = t.DescendRange("value", e1, e, func(e *Entry) bool {
			icount++
			return true
		})
		return err
	})
	if count != 173 {
		t.Error("Failure: t.DescendRange(...) unexpected iteration count")
	}
	if icount != 100 {
		t.Error("Failure: t.DescendRange(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Get(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-1", "{ \"value\":\"100\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	var eret *Entry
	db.View("test", func(t *Tx) error {
		eret, err = t.Get(e)
		return err
	})
	if eret == nil {
		t.Error("Failure: t.Get(e) returned nil entry")
	}
	if eret.k != "key-1" {
		t.Error("Failure: t.Get(e) returned incorrect entry")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Set(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-999", "{ \"value\":\"999\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	var eret *Entry
	db.View("test", func(t *Tx) error {
		_, err = t.Set(e)
		eret, err = t.Get(e)
		return err
	})
	if eret == nil {
		t.Error("Failure: t.Get(e) returned nil entry")
	}
	if eret.k != "key-999" {
		t.Error("Failure: t.Get(e) returned incorrect entry")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Delete(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-999", "{ \"value\":\"999\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	var eret *Entry
	db.View("test", func(t *Tx) error {
		_, err = t.Set(e)
		_, err = t.Get(e)
		eret, err = t.Delete(e)
		return err
	})
	if eret == nil {
		t.Error("Failure: t.Get(e) returned nil entry")
	}
	if eret.k != "key-999" {
		t.Error("Failure: t.Get(e) returned incorrect entry")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_CreateIndex(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		return nil
	})
	var idxs []string
	db.View("test", func(t *Tx) error {
		idxs, err = t.Indexes()
		return err
	})
	if len(idxs) != 1 {
		t.Error("Failure: t.Indexes() returned invalid indexes")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_DropIndex(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		t.DropIndex("value")
		return nil
	})
	var idxs []string
	db.View("test", func(t *Tx) error {
		idxs, err = t.Indexes()
		return err
	})
	if len(idxs) != 0 {
		t.Error("Failure: t.Indexes() returned invalid indexes")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Indexes(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	db.Update("test", func(t *Tx) error {
		t.CreateIndex("value", INT_INDEX)
		t.CreateIndex("coords", STRING_INDEX)
		return nil
	})
	var idxs []string
	db.View("test", func(t *Tx) error {
		idxs, err = t.Indexes()
		return err
	})
	if len(idxs) != 2 {
		t.Error("Failure: t.Indexes() returned invalid indexes")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Min(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	var eret *Entry
	db.View("test", func(t *Tx) error {
		eret, err = t.Min("")
		return err
	})
	fmt.Println(eret)
	if eret.k != "key-0" {
		t.Error("Failure: t.Min() returned incorrect minimum value")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Max(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	var eret *Entry
	db.View("test", func(t *Tx) error {
		eret, err = t.Max("")
		return err
	})
	fmt.Println(eret)
	if eret.k != "key-99" {
		t.Error("Failure: t.Max() returned incorrect minimum value")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Has(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	eopt, _ := NewEntryOptions()
	e, _ := NewEntry("key-25", "{ \"value\":\"999\", \"coords\": ["+strconv.Itoa(999)+", "+strconv.Itoa(999)+"]}", true, eopt)
	var eret bool
	db.View("test", func(t *Tx) error {
		eret, err = t.Has("", e)
		return err
	})
	fmt.Println(eret)
	if !eret {
		t.Error("Failure: t.Has() returned incorrect membership")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Size(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	var size int
	db.View("test", func(t *Tx) error {
		size, err = t.Size("")
		return err
	})
	fmt.Println(size)
	if size != 256 {
		t.Error("Failure: t.Size() returned incorrect bucket size")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_SearchIntersect(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	var entries []*Entry
	db.View("test", func(t *Tx) error {
		pt := Point{-1.0, -1.0}
		rt, err := NewRect(pt, []float64{258, 258})
		entries, err = t.SearchIntersect(rt)
		return err
	})
	if len(entries) != 256 {
		t.Error("Failure: t.SearchIntersect() returned an invalid result set")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_SearchWithinRadius(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	var entries []*Entry
	db.View("test", func(t *Tx) error {
		pt := Point{1.0, 256.0}
		entries, err = t.SearchWithinRadius(pt, 10)
		return err
	})
	if len(entries) != 8 {
		t.Error("Failure: t.SearchWithinRadius() returned an invalid result set")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_NearestNeighbors(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	var entries []*Entry
	db.View("test", func(t *Tx) error {
		pt := Point{1.0, 256.0}
		//rt, err := NewRect(pt, []float64{258, 258})
		entries, err = t.NearestNeighbors(10, pt)
		return err
	})
	if len(entries) != 10 {
		t.Error("Failure: t.NearestNeighbors() returned an invalid result set")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_NearestNeighbor(t *testing.T) {
	c, _ := NewConfig(Persist, DirPath("stitch/test/db/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	db, err := NewStitchDB(c)
	if err != nil {
		t.Errorf("Failure: NewStitchDB(c) returned error \"%v\"", err)
	}
	if db == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db got nil")
	}
	if db.config == nil {
		t.Error("Failure: NewStitchDB(c) expected not nil db.config got nil")
	}
	if !db.config.persist || !db.config.developer || !db.config.performanceMonitor {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.config.dirPath != "stitch/test/db/" || db.config.syncFreq != MNGFREQ || db.config.manageFrequency != time.Second || db.config.bucketFileMultLimit != 10 {
		t.Error("Failure: NewStitchDB(c) resulted in invalid db configuration")
	}
	if db.GetConfig() != c {
		t.Error("Failure: db.GetConfig() returned config not equal to original config")
	}
	db.Open()
	if !db.open {
		t.Error("Failure: db.Open() expected db to be open got db.open == false")
	}
	var entry *Entry
	db.View("test", func(t *Tx) error {
		pt := Point{1.0, 256.0}
		//rt, err := NewRect(pt, []float64{258, 258})
		entry, err = t.NearestNeighbor(pt)
		return err
	})
	if entry.k != "key-0" {
		t.Error("Failure: t.NearestNeighbor() returned an invalid result set")
	}
	db.Close()
	if db.open {
		t.Error("Failure: db.Close() expected db to be not open got db.open == true")
	}
}
