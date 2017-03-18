package stitchdb

type RWMode int

const (
	MODE_READ RWMode = iota
	MODE_READ_WRITE
)