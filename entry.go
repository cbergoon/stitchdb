package main

import (
	"errors"
	"time"

	"github.com/cbergoon/btree"
)

type Entry struct {
	k    string
	v    string
	opts *EntryOptions
}

func NewEntry(k string, v string, options *EntryOptions) (*Entry, error) {
	opts, err := NewEntryOptions()
	if err != nil {
		return nil, errors.New("error: failed to create entry options")
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
	if e.opts.doesExp {
		if e.opts.expTime.After(time.Now()) {
			return true
		}
		return false
	}
	return false
}

func (e *Entry) IsInvalid() bool {
	if e.opts.doesInv {
		if e.opts.invTime.After(time.Now()) {
			return true
		}
		return false
	}
	return false
}
