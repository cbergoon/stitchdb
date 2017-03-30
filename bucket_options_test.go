// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"strings"
	"testing"
)

func TestSystem(t *testing.T) {
	bucketOptions, err := NewBucketOptions(System)
	if err != nil {
		t.Errorf("Failure: NewBucketOptions(System) returned error \"%v\"", err)
	}
	if bucketOptions == nil {
		t.Errorf("Failure: NewBucketOptions(System) returned nil bucket options")
	}
	if bucketOptions.system != true {
		t.Errorf("Failure: NewBucketOptions(System) expected bucketOptions.system == true got bucketOptions.system == %v", bucketOptions.system)
	}
}

func TestGeo(t *testing.T) {
	bucketOptions, err := NewBucketOptions(Geo)
	if err != nil {
		t.Errorf("Failure: NewBucketOptions(Geo) returned error \"%v\"", err)
	}
	if bucketOptions == nil {
		t.Errorf("Failure: NewBucketOptions(Geo) returned nil bucket options")
	}
	if bucketOptions.geo != true {
		t.Errorf("Failure: NewBucketOptions(GeoRangeIsInclusive) expected bucketOptions.geo == true got bucketOptions.geo == %v", bucketOptions.geo)
	}
}

func TestGeoRangeIsInclusive(t *testing.T) {
	bucketOptions, err := NewBucketOptions(GeoRangeIsInclusive)
	if err != nil {
		t.Errorf("Failure: NewBucketOptions(GeoRangeIsInclusive) returned error \"%v\"", err)
	}
	if bucketOptions == nil {
		t.Errorf("Failure: NewBucketOptions(GeoRangeIsInclusive) returned nil bucket options")
	}
	if bucketOptions.georincl != true {
		t.Errorf("Failure: NewBucketOptions(GeoRangeIsInclusive) expected bucketOptions.georincl == true got bucketOptions.georincl == %v", bucketOptions.georincl)
	}
}

func TestTime(t *testing.T) {
	bucketOptions, err := NewBucketOptions(Time)
	if err != nil {
		t.Errorf("Failure: NewBucketOptions(Time) returned error \"%v\"", err)
	}
	if bucketOptions == nil {
		t.Errorf("Failure: NewBucketOptions(Time) returned nil bucket options")
	}
	if bucketOptions.time != true {
		t.Errorf("Failure: NewBucketOptions(Time) expected bucketOptions.time == true got bucketOptions.time == %v", bucketOptions.time)
	}
}

func TestDims(t *testing.T) {
	for i := 0; i < 24; i++ {
		bucketOptions, err := NewBucketOptions(Dims(i))
		if err != nil {
			t.Errorf("Failure: NewBucketOptions(Dims(%v)) returned error \"%v\"", i, err)
		}
		if bucketOptions == nil {
			t.Errorf("Failure: NewBucketOptions(Dims(%v)) returned nil bucket options", i)
		}
		if bucketOptions.dims != i {
			t.Errorf("Failure: NewBucketOptions(Dims(%v)) expected bucketOptions.dims == %v got bucketOptions.dims == %v", i, i, bucketOptions.dims)
		}
	}
}

func TestBTreeDegree(t *testing.T) {
	for i := 1; i <= 128; i++ {
		bucketOptions, err := NewBucketOptions(BTreeDegree(i))
		if err != nil {
			t.Errorf("Failure: NewBucketOptions(BTreeDegree(%v)) returned error \"%v\"", i, err)
		}
		if bucketOptions == nil {
			t.Errorf("Failure: NewBucketOptions(BTreeDegree(%v)) returned nil bucket options", i)
		}
		if bucketOptions.btdeg != i {
			t.Errorf("Failure: NewBucketOptions(BTreeDegree(%v)) expected bucketOptions.btdeg == %v got bucketOptions.btdeg == %v", i, i, bucketOptions.btdeg)
		}
	}
}

func TestNewBucketOptions(t *testing.T) {
	bucketOptions, err := NewBucketOptions(System, Geo, GeoRangeIsInclusive, Time, Dims(3), BTreeDegree(256))
	if err != nil {
		t.Errorf("Failure: NewBucketOptions(System, Geo, GeoRangeIsInclusive, Time, Dims(3), BTreeDegree(256)) returned error \"%v\"", err)
	}
	if bucketOptions == nil {
		t.Errorf("Failure: NewBucketOptions(System, Geo, GeoRangeIsInclusive, Time, Dims(3), BTreeDegree(256)) returned nil bucket options")
	}
	if bucketOptions.btdeg != 256 {
		t.Errorf("Failure: Expected bucketOptions.btdeg == %v got bucketOptions.btdeg == %v", 256, bucketOptions.btdeg)
	}
	if bucketOptions.dims != 3 {
		t.Errorf("Failure: Expected bucketOptions.dims == %v got bucketOptions.dims == %v", 3, bucketOptions.dims)
	}
	if bucketOptions.time != true {
		t.Errorf("Failure: Expected bucketOptions.time == true got bucketOptions.time == %v", bucketOptions.time)
	}
	if bucketOptions.georincl != true {
		t.Errorf("Failure: Expected bucketOptions.georincl == true got bucketOptions.georincl == %v", bucketOptions.georincl)
	}
	if bucketOptions.geo != true {
		t.Errorf("Failure: Expected bucketOptions.geo == true got bucketOptions.geo == %v", bucketOptions.geo)
	}
	if bucketOptions.system != true {
		t.Errorf("Failure: Expected bucketOptions.system == true got bucketOptions.system == %v", bucketOptions.system)
	}
}

func TestBucketOptions_bucketOptionsCreateStmt(t *testing.T) {
	bucketOptions, err := NewBucketOptions(System, Geo, GeoRangeIsInclusive, Time, Dims(3), BTreeDegree(256))
	if err != nil {
		t.Errorf("Failure: NewBucketOptions(System, Geo, GeoRangeIsInclusive, Time, Dims(3), BTreeDegree(256)) returned error \"%v\"", err)
	}
	opts := bucketOptions.bucketOptionsCreateStmt()
	if len(opts) != 13 {
		t.Errorf("Failure: bucketOptionsCreateStmt() statement expected length 13 got length %v", len(opts))
	}
}

func TestNewBucketOptionsFromStmt(t *testing.T) {
	bucketOptions, err := NewBucketOptions(System, Geo, GeoRangeIsInclusive, Time, Dims(3), BTreeDegree(256))
	if err != nil {
		t.Errorf("Failure: NewBucketOptions(System, Geo, GeoRangeIsInclusive, Time, Dims(3), BTreeDegree(256)) returned error \"%v\"", err)
	}
	opts := bucketOptions.bucketOptionsCreateStmt()
	if len(opts) != 13 {
		t.Errorf("Failure: bucketOptionsCreateStmt() statement expected length 13 got length %v", len(opts))
	}
	parts := strings.Split(string(opts), ":")
	var partsFull []string
	partsFull = append(partsFull, "")
	partsFull = append(partsFull, parts...)
	parsedBucketOptions, err := NewBucketOptionsFromStmt(partsFull)
	if err != nil {
		t.Errorf("Failure: NewBucketOptionsFromStmt(parts) returned error \"%v\"", err)
	}
	if parsedBucketOptions == nil {
		t.Errorf("Failure: NewBucketOptionsFromStmt(parts) statement returned nil bucket options")
	}
	if parsedBucketOptions.btdeg != 256 {
		t.Errorf("Failure: Expected bucketOptions.btdeg == %v got bucketOptions.btdeg == %v", 256, parsedBucketOptions.btdeg)
	}
	if parsedBucketOptions.dims != 3 {
		t.Errorf("Failure: Expected bucketOptions.dims == %v got bucketOptions.dims == %v", 3, parsedBucketOptions.dims)
	}
	if parsedBucketOptions.time != true {
		t.Errorf("Failure: Expected bucketOptions.time == true got bucketOptions.time == %v", parsedBucketOptions.time)
	}
	if parsedBucketOptions.georincl != true {
		t.Errorf("Failure: Expected bucketOptions.georincl == true got bucketOptions.georincl == %v", parsedBucketOptions.georincl)
	}
	if parsedBucketOptions.geo != true {
		t.Errorf("Failure: Expected bucketOptions.geo == true got bucketOptions.geo == %v", parsedBucketOptions.geo)
	}
	if parsedBucketOptions.system != true {
		t.Errorf("Failure: Expected bucketOptions.system == true got bucketOptions.system == %v", parsedBucketOptions.system)
	}
}
