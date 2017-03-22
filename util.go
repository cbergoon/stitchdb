// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

//boolToInt returns an integer representation of a provided bool. Returns 1 for a true value and 0 for a false value.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
