// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
)

type EntryOptions struct {
	doesExp bool
	doesInv bool
	expTime time.Time
	invTime time.Time
	tol     float64
}

func ExpireTime(time time.Time) func(*EntryOptions) error {
	return func(e *EntryOptions) error {
		e.doesExp = true
		e.expTime = time
		return nil
	}
}

func InvalidTime(time time.Time) func(*EntryOptions) error {
	return func(e *EntryOptions) error {
		e.doesInv = true
		e.invTime = time
		return nil
	}
}

func Tol(t float64) func(*EntryOptions) error {
	return func(e *EntryOptions) error {
		e.tol = t
		return nil
	}
}

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
		cbuf = append(cbuf, ':')
		cbuf = append(cbuf, strconv.FormatFloat(e.tol, 'f', -1, 64)...)
	} else {
		cbuf = append(cbuf, strconv.Itoa(boolToInt(false))...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.Itoa(boolToInt(false))...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.FormatInt(0, 10)...)
		cbuf = append(cbuf, '~')
		cbuf = append(cbuf, strconv.FormatInt(0, 10)...)
		cbuf = append(cbuf, ':')
		cbuf = append(cbuf, strconv.FormatFloat(0.01, 'f', -1, 64)...)
	}
	return cbuf
}

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
	tol, err := strconv.ParseFloat(stmt[6], 64)
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
