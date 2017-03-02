package main

import (
	"github.com/cbergoon/btree"
	"github.com/tidwall/gjson"
)

type IndexValueType int

const (
	STRING_INDEX = iota
	UINT_INDEX
	INT_INDEX
	FLOAT_INDEX
)

type Index struct {
	t     *btree.BTree
	ppath string
	vtype IndexValueType
	bkt   *Bucket
}

func NewIndex(ppath string, vtype IndexValueType, bkt *Bucket) (*Index, error) {
	index := &Index{
		ppath: ppath,
		bkt:   bkt,
		vtype: vtype,
	}
	index.t = btree.New(bkt.options.btdeg, index)
	return index, nil
}

func (i *Index) less(x, y *Entry) bool {
	switch i.vtype {
	case INT_INDEX:
		return gjson.Get(x.v, i.ppath).Int() < gjson.Get(y.v, i.ppath).Int()
	case UINT_INDEX:
		return gjson.Get(x.v, i.ppath).Uint() < gjson.Get(y.v, i.ppath).Uint()
	case FLOAT_INDEX:
		return gjson.Get(x.v, i.ppath).Float() < gjson.Get(y.v, i.ppath).Float()
	default: //Use String Value
		return gjson.Get(x.v, i.ppath).String() < gjson.Get(y.v, i.ppath).String()
	}
}

func (i *Index) entryValidForIndex(e *Entry) bool {
	//Todo: Indexes: Implement
	return false
}

func (i *Index) get(e *Entry) *Entry {
	//Todo: Check entry is valid for index
	res := i.t.Get(e)
	var eres *Entry
	if res != nil {
		eres = res.(*Entry)
	}
	return eres
}

func (i *Index) insert(e *Entry) *Entry {
	//Todo: error if entry == nil || if db is nil || if db not open || if bucket is nil || if bucket not open || if index is nil || if index tree is nil
	if e == nil {
		//Error
	}
	if i.bkt.db == nil || !i.bkt.db.open {
		//Error
	}
	if i.bkt == nil || !i.bkt.open {
		//Error
	}
	if i == nil || i.t == nil {
		//Error
	}
	//Todo: Check entry is valid for index
	var epres *Entry
	pres := i.t.ReplaceOrInsert(e)
	if pres != nil {
		epres = pres.(*Entry)
	}
	return epres
}

func (i *Index) delete(e *Entry) *Entry {
	//Todo: error if entry == nil || if db is nil || if db not open || if bucket is nil || if bucket not open || if index is nil || if index tree is nil
	//Todo: Check entry is valid for index
	var edres *Entry
	dres := i.t.Delete(e)
	if dres != nil {
		edres = dres.(*Entry)
	}
	return edres
}

func (i *Index) build(bucket *Bucket) {
	//Todo: Indexes: Implement
}

func (i *Index) rebuild(bucket *Bucket) {
	//Todo: Indexes: Implement
}
