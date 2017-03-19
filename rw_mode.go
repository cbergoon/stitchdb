// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

type RWMode int

const (
	MODE_READ RWMode = iota
	MODE_READ_WRITE
)
