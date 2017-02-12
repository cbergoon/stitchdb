package main

import "github.com/cbergoon/btree"

type Entry struct {
	k    string
	v    string
	opts *EntryOptions
}

func NewEntry(k string, v string, options *EntryOptions) (*Entry, error) {
	return &Entry{
		k:    k,
		v:    v,
		opts: options,
	}, nil
}

func (e *Entry) Less(than btree.Item, itype interface{}) bool {
	return false
}
