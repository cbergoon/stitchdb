// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import "testing"

func TestBoolToInt(t *testing.T) {
	tr := boolToInt(true)
	fa := boolToInt(false)
	if tr != 1 {
		t.Errorf("Failure: Expected boolToInt(true) == 1 got %v", tr)
	}
	if fa != 0 {
		t.Errorf("Failure: Expected boolToInt(false) == 0 got %v", tr)
	}
}
