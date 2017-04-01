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
		t.Errorf("Failure: t.Ascend(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_AscendGreaterOrEqual(t *testing.T) {
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
		t.Errorf("Failure: t.AscendGreaterOrEqual(...) unexpected iteration count")
	}
	if icount != 157 {
		t.Errorf("Failure: t.AscendGreaterOrEqual(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_AscendLessThan(t *testing.T) {
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
		t.Errorf("Failure: t.AscendLessThan(...) unexpected iteration count")
	}
	if icount != 99 {
		t.Errorf("Failure: t.AscendLessThan(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_AscendRange(t *testing.T) {
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
		t.Errorf("Failure: t.AscendRange(...) unexpected iteration count")
	}
	if icount != 100 {
		t.Errorf("Failure: t.AscendRange(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Descend(t *testing.T) {
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
		err := t.Descend("", func(e *Entry) bool {
			count++
			return true
		})
		return err
	})
	fmt.Println(count)
	if count != 256 {
		t.Errorf("Failure: t.Descend(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_DescendGreaterThan(t *testing.T) {
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
		t.Errorf("Failure: t.DescendGreaterThan(...) unexpected iteration count")
	}
	if icount != 156 {
		t.Errorf("Failure: t.DescendGreaterThan(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_DescendLessOrEqual(t *testing.T) {
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
		t.Errorf("Failure: t.DescendLessOrEqual(...) unexpected iteration count")
	}
	if icount != 100 {
		t.Errorf("Failure: t.DescendLessOrEqual(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_DescendRange(t *testing.T) {
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
		t.Errorf("Failure: t.DescendRange(...) unexpected iteration count")
	}
	if icount != 100 {
		t.Errorf("Failure: t.DescendRange(...) unexpected iteration count")
	}
	//time.Sleep(time.Second * 2)
	db.Close()
	if db.open {
		t.Errorf("Failure: db.Close() expected db to be not open got db.open == true")
	}
}

func TestTx_Get(t *testing.T) {

}

func TestTx_Set(t *testing.T) {

}

func TestTx_Delete(t *testing.T) {

}

func TestTx_CreateIndex(t *testing.T) {

}

func TestTx_DropIndex(t *testing.T) {

}

func TestTx_Indexes(t *testing.T) {

}

func TestTx_Min(t *testing.T) {

}

func TestTx_Max(t *testing.T) {

}

func TestTx_Has(t *testing.T) {

}

func TestTx_Size(t *testing.T) {

}

func TestTx_SearchIntersect(t *testing.T) {

}

func TestTx_SearchWithinRadius(t *testing.T) {

}

func TestTx_NearestNeighbors(t *testing.T) {

}

func TestTx_NearestNeighbor(t *testing.T) {

}
