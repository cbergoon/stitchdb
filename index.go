package main

import (
	"github.com/cbergoon/btree"
	"github.com/tidwall/gjson"
)

type Index struct {
	t     *btree.BTree
	ppath string
	bkt   *Bucket
}

func NewIndex(ppath string, bkt *Bucket) (*Index, error) {
	index := &Index{
		ppath: ppath,
		bkt:   bkt,
	}
	index.t = btree.New(bkt.options.btdeg, index)
	return index, nil
}

func (i *Index) less(x, y *Entry) bool {
	xvr := gjson.Get(x.v, i.ppath)
	xv := xvr.String()
	yvr := gjson.Get(y.v, i.ppath)
	yv := yvr.String()
	return xv < yv
}

func (i *Index) build(bucket *Bucket) {
	//Todo: Indexes: Implement
}

func (i *Index) rebuild(bucket *Bucket) {
	//Todo: Indexes: Implement
}
