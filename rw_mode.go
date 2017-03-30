// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

//RWMode represents the R/W access modifier.
type RWMode int

const (
	//MODE_READ Read Only
	MODE_READ RWMode = iota
	//MODE_READ_WRITE Read and Write
	MODE_READ_WRITE
)
