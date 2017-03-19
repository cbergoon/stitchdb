// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
