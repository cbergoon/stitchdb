// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

//RWMode represents the R/W access modifier.
type RWMode int

const (
	//Read Only
	MODE_READ RWMode = iota
	//Read and Write
	MODE_READ_WRITE
)
