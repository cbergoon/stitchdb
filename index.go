package main

import "github.com/cbergoon/btree"

type Index struct {
	t     *btree.BTree
	ppath string
	less  func(x, y *Entry) bool
}

func NewIndex(ppath string) (*Index, error) {
	return nil, nil
}

func (i *Index) build() {

}

func (i *Index) rebuild() {

}
