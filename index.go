package stitchdb

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
	case STRING_INDEX:
		return gjson.Get(x.v, i.ppath).Float() < gjson.Get(y.v, i.ppath).Float()
	default: //Use String Value
		return gjson.Get(x.v, i.ppath).String() < gjson.Get(y.v, i.ppath).String()
	}
}

func (i *Index) get(e *Entry) *Entry {
	if !gjson.Get(e.v, i.ppath).Exists() {
		return nil
	}
	res := i.t.Get(e)
	var eres *Entry
	if res != nil {
		eres = res.(*Entry)
	}
	return eres
}

func (i *Index) insert(e *Entry) *Entry {
	if !gjson.Get(e.v, i.ppath).Exists() {
		return nil
	}
	var epres *Entry
	pres := i.t.ReplaceOrInsert(e)
	if pres != nil {
		epres = pres.(*Entry)
	}
	return epres
}

func (i *Index) delete(e *Entry) *Entry {
	if !gjson.Get(e.v, i.ppath).Exists() {
		return nil
	}
	var edres *Entry
	dres := i.t.Delete(e)
	if dres != nil {
		edres = dres.(*Entry)
	}
	return edres
}

func (i *Index) build() {
	i.bkt.data.Ascend(func(item btree.Item) bool {
		eItem := item.(*Entry)
		return func(e *Entry) bool {
			i.insert(e)
			return true
		}(eItem)
	})
}

func (i *Index) rebuild() {
	i.t = btree.New(i.bkt.options.btdeg, i)
	i.build()
}
