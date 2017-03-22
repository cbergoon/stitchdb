// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import "github.com/dhconnelly/rtreego"

//Point provides an abstraction over the rtreego Point type so that dhconnelly/rtreego does not need to be included
//from the application using this library.
type Point []float64

//rtreegoPoint returns a rtreego.Point converted from Point type.
func rtreegoPoint(p Point) rtreego.Point {
	var rp rtreego.Point
	for i := 0; i < len(p); i++ {
		rp = append(rp, p[i])
	}
	return rp
}

//Rect provides an abstraction over the rtreego Rect type so that dhconnelly/rtreego does not need to be included
//from the application using this library.
type Rect struct {
	p       Point
	lengths []float64
}

//NewRect returns a new Rect.
func NewRect(p Point, lengths []float64) (r *Rect, err error) {
	return &Rect{p: p, lengths: lengths}, nil
}

//rtreegoRect returns a rtreego.Rect converted from Rect type.
func rtreegoRect(r *Rect) (*rtreego.Rect, error) {
	return rtreego.NewRect(rtreegoPoint(r.p), r.lengths)
}
