// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"strconv"
	"time"

	"github.com/cbergoon/btree"
	"github.com/dhconnelly/rtreego"
	"github.com/juju/errors"
	"github.com/tidwall/gjson"
)

//Entry represents an item to be stored in the database.
type Entry struct {
	k        string        //Key of the entry.
	v        string        //Value (as JSON) of the entry.
	opts     *EntryOptions //Entry configuration.
	invalid  bool          //Indicates validity of the entry.
	location rtreego.Point //Geo representation if geo-enabled.
}

//NewEntry creates a new entry object with the provided values. Returns an error if the default options failed to create.
func NewEntry(k string, v string, geo bool, options *EntryOptions) (*Entry, error) {
	opts, err := NewEntryOptions()
	if err != nil {
		return nil, errors.Annotate(err, "error: entry: failed to create entry options")
	}
	if options != nil {
		opts = options
	}
	var l rtreego.Point
	if geo {
		ljson := gjson.Get(v, "coords")
		if ljson.Exists() {
			ljson.ForEach(func(key, value gjson.Result) bool {
				l = append(l, value.Float())
				return true // keep iterating
			})
		}
	}
	return &Entry{
		k:        k,
		v:        v,
		opts:     opts,
		location: l,
	}, nil
}

//NewEntryWithGeo creates a new entry; the entry value is expected to provide a "coords" field in the json provided.
//Returns an error if the default options failed to create.
func NewEntryWithGeo(k string, v string, options *EntryOptions) (*Entry, error) {
	opts, err := NewEntryOptions()
	if err != nil {
		return nil, errors.Annotate(err, "error: entry: failed to create entry options")
	}
	if options != nil {
		opts = options
	}
	var l rtreego.Point
	ljson := gjson.Get(v, "coords")
	if ljson.Exists() {
		ljson.ForEach(func(key, value gjson.Result) bool {
			l = append(l, value.Float())
			return true // keep iterating
		})
	}
	return &Entry{
		k:    k,
		v:    v,
		opts: opts,
	}, nil
}

//Less is the comparator provided used to build the indexes over a bucket.
func (e *Entry) Less(than btree.Item, itype interface{}) bool {
	tl := than.(*Entry)
	switch i := itype.(type) {
	case *eItype:
		return e.ExpiresAt().Before(tl.ExpiresAt()) //Todo: May need to catch edge case
	case *iItype:
		return e.InvalidatesAt().Before(tl.InvalidatesAt()) //Todo: May need to catch edge case
	case *Index:
		return i.less(e, tl)
	default:
		return e.k < tl.k
	}
	return e.k < tl.k
}

//IsExpired checks if the expire time for an entry has passed.
func (e *Entry) IsExpired() bool {
	if e.opts.doesExp {
		if e.opts.expTime.After(time.Now()) {
			return true
		}
		return false
	}
	return false
}

//IsInvalid checks if the invalid time for an entry has passed.
func (e *Entry) IsInvalid() bool {
	if e.invalid {
		return true
	}
	if e.opts.doesInv {
		if e.opts.invTime.After(time.Now()) {
			return true
		}
		return false
	}
	return false
}

//ExpiresAt returns the time that an entry will expire.
func (e *Entry) ExpiresAt() time.Time {
	return e.opts.expTime
}

//InvalidatesAt returns the time that an entry will invalidate.
func (e *Entry) InvalidatesAt() time.Time {
	return e.opts.invTime
}

//Bounds is used by rtree. Returns a Rect representation of the specified point using the entry options tolerance.
func (e *Entry) Bounds() *rtreego.Rect {
	// define the bounds of s to be a rectangle centered at s.location
	// with side lengths 2 * tol:
	return e.location.ToRect(e.opts.tol)
}

//GetEntryComparator returns a function that is used by the rtree to compare entries. This function will compare on the
//key value (k) of the entry as a string.
//Todo: Maybe make the returned function an option that can be set
func GetEntryComparator() func(obj1, obj2 rtreego.Spatial) bool {
	return func(obj1, obj2 rtreego.Spatial) bool {
		sp1 := obj1.(*Entry)
		sp2 := obj2.(*Entry)
		return sp1.k == sp2.k
	}
}

//func (e *Entry) ValidForEntry(e *Entry) bool {
//	return
//}

//EntryInsertStmt builds and returns the insert statement for a given entity.
func (e *Entry) EntryInsertStmt() []byte {
	var buf, cbuf []byte

	cbuf = append(cbuf, "INSERT"...)
	cbuf = append(cbuf, '~')
	cbuf = append(cbuf, e.k...)
	cbuf = append(cbuf, '~')
	cbuf = append(cbuf, e.v...)
	cbuf = append(cbuf, '~')
	cbuf = append(cbuf, e.opts.entryOptionsCreateStmt()...)
	cbuf = append(cbuf, '\n')

	buf = append(buf, strconv.Itoa(len(cbuf))...)
	buf = append(buf, '\n')
	buf = append(buf, cbuf...)

	return buf
}

//EntryDeleteStmt builds and returns the delete statement for a given entity.
func (e *Entry) EntryDeleteStmt() []byte {
	var buf, cbuf []byte

	cbuf = append(cbuf, "DELETE"...)
	cbuf = append(cbuf, '~')
	cbuf = append(cbuf, e.k...)
	cbuf = append(cbuf, '~')
	cbuf = append(cbuf, e.v...)
	cbuf = append(cbuf, '~')
	cbuf = append(cbuf, e.opts.entryOptionsCreateStmt()...)
	cbuf = append(cbuf, '\n')

	buf = append(buf, strconv.Itoa(len(cbuf))...)
	buf = append(buf, '\n')
	buf = append(buf, cbuf...)

	return buf
}

//NewEntryFromStmt parses the statement provided and returns an entry representation. Returns an error if the statement
//could not be parsed or if the resulting entry could not be created.
func NewEntryFromStmt(stmtParts []string) (*Entry, error) {
	opts, err := NewEntryOptionsFromStmt(stmtParts[3:])
	if err != nil {
		return nil, errors.Annotate(err, "error: entry: failed to parse entry options")
	}
	var entry *Entry
	if gjson.Get(stmtParts[2], "coords").Exists() {
		entry, err = NewEntry(stmtParts[1], stmtParts[2], true, opts)
	} else {
		entry, err = NewEntry(stmtParts[1], stmtParts[2], false, opts)
	}
	if err != nil {
		return nil, errors.Annotate(err, "error: entry: failed to create entry")
	}
	return entry, nil
}
