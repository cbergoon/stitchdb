// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"testing"
	"time"
)

func TestExpireTime(t *testing.T) {
	now := time.Now()
	entryOptions, err := NewEntryOptions(ExpireTime(now))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(now)) returned error \"%v\"", err)
	}
	if entryOptions == nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(now)) returned nil entry options")
	}
	if entryOptions.expTime != now {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(now)) expected entryOptions.expTime == %v got entryOptions.expTime == %v", now, entryOptions.expTime)
	}
}

func TestInvalidTime(t *testing.T) {
	now := time.Now()
	entryOptions, err := NewEntryOptions(InvalidTime(now))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(InvalidTime(now)) returned error \"%v\"", err)
	}
	if entryOptions == nil {
		t.Errorf("Failure: NewEntryOptions(InvalidTime(now)) returned nil entry options")
	}
	if entryOptions.invTime != now {
		t.Errorf("Failure: NewEntryOptions(InvalidTime(now)) expected entryOptions.invTime == %v got entryOptions.invTime == %v", now, entryOptions.invTime)
	}
}

func TestTol(t *testing.T) {
	entryOptions, err := NewEntryOptions(Tol(9.99))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(Tol(9.99)) returned error \"%v\"", err)
	}
	if entryOptions == nil {
		t.Errorf("Failure: NewEntryOptions(Tol(9.99)) returned nil entry options")
	}
	if entryOptions.tol != 9.99 {
		t.Errorf("Failure: NewEntryOptions(Tol(9.99)) expected entryOptions.tol == %v got entryOptions.tol == %v", 9.99, entryOptions.tol)
	}
}

func TestNewEntryOptions(t *testing.T) {

}

func TestEntryOptionsCreateStmt(t *testing.T) {

}

func TestNewEntryOptionsFromStmt(t *testing.T) {

}
