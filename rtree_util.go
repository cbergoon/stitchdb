// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import "github.com/dhconnelly/rtreego"

type Point []float64

func rtreegoPoint(p Point) rtreego.Point {
	var rp rtreego.Point
	for i := 0; i < len(p); i++ {
		rp = append(rp, p[i])
	}
	return rp
}

type Rect struct {
	p       Point
	lengths []float64
}

func NewRect(p Point, lengths []float64) (r *Rect, err error) {
	return &Rect{p: p, lengths: lengths}, nil
}

func rtreegoRect(r *Rect) (*rtreego.Rect, error) {
	return rtreego.NewRect(rtreegoPoint(r.p), r.lengths)
}
