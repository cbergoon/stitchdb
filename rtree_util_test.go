// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import "testing"

func TestRtreegoPoint(t *testing.T) {
	rpt := rtreegoPoint(Point{1.0, 2.0, 3.0})
	if rpt == nil {
		t.Errorf("Failure: rtreegoPoint(Point{1.0, 2.0, 3.0}) returned nil value")
	}
	if rpt[0] != 1.0 {
		t.Errorf("Failure: rtreegoPoint(Point{1.0, 2.0, 3.0}) expected value at [0] == 1.0 got %v", rpt[0])
	}
	if rpt[1] != 2.0 {
		t.Errorf("Failure: rtreegoPoint(Point{1.0, 2.0, 3.0}) expected value at [1] == 2.0 got %v", rpt[1])
	}
	if rpt[2] != 3.0 {
		t.Errorf("Failure: rtreegoPoint(Point{1.0, 2.0, 3.0}) expected value at [2] == 3.0 got %v", rpt[2])
	}
}

func TestRtreegoRect(t *testing.T) {
	rt, err := NewRect(Point{1.0, 2.0}, []float64{3.0, 4.0})
	if err != nil {
		t.Errorf("Failure: NewRect(Point{1.0, 2.0}, []float64{3.0, 4.0}) returned error \"%v\"", err)
	}
	rrt, err := rtreegoRect(rt)
	if err != nil {
		t.Errorf("Failure: rtreegoRect(rt) returned error \"%v\"", err)
	}
	if rrt.PointCoord(0) != 1.0 {
		t.Errorf("Failure: Expected point coordinate at index 0 == 1.0 got %v", rrt.PointCoord(0))
	}
	if rrt.PointCoord(1) != 2.0 {
		t.Errorf("Failure: Expected point coordinate at index 1 == 2.0 got %v", rrt.PointCoord(1))
	}
	if rrt.LengthsCoord(0) != 3.0 {
		t.Errorf("Failure: Expected length coordinate at index 0 == 3.0 got %v", rrt.LengthsCoord(0))
	}
	if rrt.LengthsCoord(1) != 4.0 {
		t.Errorf("Failure: Expected length coordinate at index 0 == 3.0 got %v", rrt.LengthsCoord(0))
	}
}

func TestNewRect(t *testing.T) {
	rt, err := NewRect(Point{1.0, 2.0}, []float64{3.0, 4.0})
	if err != nil {
		t.Errorf("Failure: NewRect(Point{1.0, 2.0}, []float64{3.0, 4.0}) returned error \"%v\"", err)
	}
	if rt.p[0] != 1.0 {
		t.Errorf("Failure: Expected point coordinate at index 0 == 1.0 got %v", rt.p[0])
	}
	if rt.p[1] != 2.0 {
		t.Errorf("Failure: Expected point coordinate at index 1 == 2.0 got %v", rt.p[1])
	}
	if rt.lengths[0] != 3.0 {
		t.Errorf("Failure: Expected length coordinate at index 3 == 3.0 got %v", rt.lengths[0])
	}
	if rt.lengths[1] != 4.0 {
		t.Errorf("Failure: Expected length coordinate at index 4 == 4.0 got %v", rt.lengths[1])
	}
}
