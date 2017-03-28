// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"github.com/cbergoon/btree"
	"github.com/tidwall/gjson"
)

//IndexValueType represents the type of the field that the index will be built over.
type IndexValueType int

const (
	//STRING_INDEX indicates data sorting should assume string.
	STRING_INDEX IndexValueType = iota
	//UINT_INDEX indicates data sorting should assume uint.
	UINT_INDEX
	//INT_INDEX indicates data sorting should assume int.
	INT_INDEX
	//FLOAT_INDEX indicates data sorting should assume float.
	FLOAT_INDEX
)

//Index represents an index for a bucket. Buckets can have multiple indexes but indexes cannot have entries from multiple
//buckets.
type Index struct {
	t     *btree.BTree   //Index tree representation with defined ordering.
	ppath string         //Path to field that the index will be ordered using. Uses tidwall/gjson access format.
	vtype IndexValueType //Defines the type of the field in question and determines how the value will be compared.
	bkt   *Bucket        //Reference back to the bucket the the index is built using.
}

//NewIndex returns an index for the values provided. The index will be initialized but NOT built.
func NewIndex(ppath string, vtype IndexValueType, bkt *Bucket) (*Index, error) {
	index := &Index{
		ppath: ppath,
		bkt:   bkt,
		vtype: vtype,
	}
	index.t = btree.New(bkt.options.btdeg, index)
	return index, nil
}

//less is a comparator for the index tree that utilizes the IndexValueType to determine how to compare the entries. The
//comparator also retrieves the field value from the entry value json string.
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

//get searches the tree for an entry that matches the provided entry's index field value. Returns the entry if it exists,
//nil otherwise. If the provided entry does not contain the index field matching the index field path then the function
//returns nil.
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

//insert adds an entry to the index tree. If an entry is replace, the replaced entry is returned, otherwise returns nil.
//If the provided entry does not contain the index field matching the index field path then the function
//returns nil.
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

//delete removes an entry from the index tree. Returns the entry that was deleted, if no entry was deleted the the function
//returns nil. If the provided entry does not contain the index field matching the index field path then the function
//returns nil.
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

//build iterates over all entries in the bucket attempting to insert each entry into the tree.
func (i *Index) build() {
	i.bkt.data.Ascend(func(item btree.Item) bool {
		eItem := item.(*Entry)
		return func(e *Entry) bool {
			i.insert(e)
			return true
		}(eItem)
	})
}

//rebuild reinitializes the index tree and builds the index using the new index tree.
func (i *Index) rebuild() {
	i.t = btree.New(i.bkt.options.btdeg, i)
	i.build()
}
