// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
)

//EntryOptions represents the configuration for an entry determining how an entry will function within a bucket.
type EntryOptions struct {
	doesExp bool      //Indicates if the entry will expire at expTime.
	doesInv bool      //Indicates if the entry will invalidate at invTime.
	expTime time.Time //Time at which the entry will expire if doesExp is true.
	invTime time.Time //Time at which the entry will invalidate if doesInv is true.
	tol     float64   //Tolerance of the entry's geo-location. Used to create a rectangle to insert into rtree.
}

//ExpireTime sets the time the entry will expire and enables expiration for the entry.
func ExpireTime(time time.Time) func(*EntryOptions) error {
	return func(e *EntryOptions) error {
		e.doesExp = true
		e.expTime = time
		return nil
	}
}

//InvalidTime sets the time the entry will invalidate and enables invalidation for the entry.
func InvalidTime(time time.Time) func(*EntryOptions) error {
	return func(e *EntryOptions) error {
		e.doesInv = true
		e.invTime = time
		return nil
	}
}

//Sets the tolerance (accuracy) of the geo-location for the entry primarily used to build the rtree.
func Tol(t float64) func(*EntryOptions) error {
	return func(e *EntryOptions) error {
		e.tol = t
		return nil
	}
}

//NewEntryOptions creates a new entry using the provided option modifiers.
func NewEntryOptions(options ...func(*EntryOptions) error) (*EntryOptions, error) {
	c := &EntryOptions{}
	for _, option := range options {
		err := option(c)
		if err != nil {
			return nil, errors.Annotate(err, "error: entry_options: failed to create entry options")
		}
	}
	return c, nil
}

//entryOptionsCreateStmt returns the options portion of the entry insert statement.
func (e *EntryOptions) entryOptionsCreateStmt() []byte {
	var cbuf []byte
	if e != nil {
		cbuf = append(cbuf, strconv.Itoa(boolToInt(e.doesExp))...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.Itoa(boolToInt(e.doesInv))...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.FormatInt(e.expTime.Unix(), 10)...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.FormatInt(e.invTime.Unix(), 10)...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.FormatFloat(e.tol, 'f', -1, 64)...)
	} else {
		cbuf = append(cbuf, strconv.Itoa(boolToInt(false))...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.Itoa(boolToInt(false))...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.FormatInt(0, 10)...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.FormatInt(0, 10)...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.FormatFloat(0.01, 'f', -1, 64)...)
	}
	return cbuf
}

//NewEntryOptionsFromStmt returns entry options representing the options portion of the statement. Returns an error if the
//entry statement could not be parsed.
func NewEntryOptionsFromStmt(stmt []string) (*EntryOptions, error) {
	doesExp, err := strconv.ParseBool(stmt[0])
	if err != nil {
		return nil, errors.Annotate(err, "error: entry_options: failed to parse entry options")
	}
	doesInv, err := strconv.ParseBool(stmt[1])
	if err != nil {
		return nil, errors.Annotate(err, "error: entry_options: failed to parse entry options")
	}
	ets := strings.TrimSpace(stmt[2])
	expInt, err := strconv.ParseInt(ets, 10, 64)
	if err != nil {
		return nil, errors.Annotate(err, "error: entry_options: failed to parse entry options")
	}
	expTime := time.Unix(expInt, 0)
	its := strings.TrimSpace(stmt[3])
	invInt, err := strconv.ParseInt(its, 10, 64)
	if err != nil {
		return nil, errors.Annotate(err, "error: entry_options: failed to parse entry options")
	}
	invTime := time.Unix(invInt, 0)
	tol, err := strconv.ParseFloat(strings.TrimSpace(stmt[4]), 64)
	if err != nil {
		return nil, errors.Annotate(err, "error: entry_options: failed to parse entry options")
	}
	return &EntryOptions{
		doesExp: doesExp,
		doesInv: doesInv,
		expTime: expTime,
		invTime: invTime,
		tol:     tol,
	}, nil
}
