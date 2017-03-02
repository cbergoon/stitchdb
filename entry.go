package main

import (
	"strconv"
	"time"

	"github.com/cbergoon/btree"
	"github.com/juju/errors"
)

//Todo: Implement Less function

type Entry struct {
	k       string
	v       string
	opts    *EntryOptions
	invalid bool
}

func NewEntry(k string, v string, options *EntryOptions) (*Entry, error) {
	opts, err := NewEntryOptions()
	if err != nil {
		return nil, errors.Annotate(err, "error: entry: failed to create entry options")
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

func (e *Entry) ExpiresAt() time.Time {
	return e.opts.expTime
}

func (e *Entry) InvalidatesAt() time.Time {
	return e.opts.invTime
}

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

func NewEntryFromStmt(stmtParts []string) (*Entry, error) {
	opts, err := NewEntryOptionsFromStmt(stmtParts[3:])
	if err != nil {
		return nil, errors.Annotate(err, "error: entry: failed to parse entry options")
	}
	entry, err := NewEntry(stmtParts[1], stmtParts[2], opts)
	if err != nil {
		return nil, errors.Annotate(err, "error: entry: failed to create entry")
	}
	return entry, nil
}
