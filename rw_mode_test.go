// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import "testing"

func TestRWMode(t *testing.T) {
	//Test ensures API does not break
	if MODE_READ != 0 {
		t.Errorf("Failure: Expected MODE_READ == 0 got MODE_READ = %v", MODE_READ)
	}
	if MODE_READ_WRITE != 1 {
		t.Errorf("Failure: Expected MODE_READ_WRITE == 0 got MODE_READ_WRITE = %v", MODE_READ_WRITE)
	}
}
