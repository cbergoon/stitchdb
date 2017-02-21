package main

import "github.com/cbergoon/btree"

type Entry struct {
	k    string
	v    string
	opts *EntryOptions
}

func NewEntry(k string, v string, options *EntryOptions) (*Entry, error) {
	opts, err := NewEntryOptions()
	if err != nil {
		//Todo: Error
	}
	if options != nil {
		opts = options
	}
	return &Entry{
		k:    k,
		v:    v,
		opts: opts,
	}, nil
}

func (e *Entry) Less(than btree.Item, itype interface{}) bool {
	tl := than.(*Entry)
	return e.k < tl.k
}

func (e *Entry) IsExpired() bool {
	return false
}

func (e *Entry) IsInvalid() bool {
	return false
}
