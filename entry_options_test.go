// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"strings"
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
	now := time.Now()
	entryOptions, err := NewEntryOptions(ExpireTime(now), InvalidTime(now), Tol(9.99))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) returned error \"%v\"", err)
	}
	if entryOptions == nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) returned nil entry options")
	}
	if entryOptions.expTime != now {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) expected entryOptions.expTime == %v got entryOptions.expTime == %v", now, entryOptions.expTime)
	}
	if entryOptions.invTime != now {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) expected entryOptions.invTime == %v got entryOptions.invTime == %v", now, entryOptions.invTime)
	}
	if entryOptions.tol != 9.99 {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) expected entryOptions.tol == %v got entryOptions.tol == %v", 9.99, entryOptions.tol)
	}
}

func TestEntryOptionsCreateStmt(t *testing.T) {
	now := time.Now()
	entryOptions, err := NewEntryOptions(ExpireTime(now), InvalidTime(now), Tol(9.99))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) returned error \"%v\"", err)
	}
	opts := entryOptions.entryOptionsCreateStmt()
	if len(opts) != 30 {
		t.Errorf("Failure: entryOptionsCreateStmt() statement expected length 30 got length %v", len(opts))
	}
}

func TestNewEntryOptionsFromStmt(t *testing.T) {
	now := time.Now()
	entryOptions, err := NewEntryOptions(ExpireTime(now), InvalidTime(now), Tol(9.99))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) returned error \"%v\"", err)
	}
	opts := entryOptions.entryOptionsCreateStmt()
	if len(opts) != 30 {
		t.Errorf("Failure: entryOptionsCreateStmt() statement expected length 30 got length %v", len(opts))
	}
	parts := strings.Split(string(opts), "~")
	newEntryOptions, err := NewEntryOptionsFromStmt(parts)
	if err != nil {
		t.Errorf("Failure: NewEntryOptionsFromStmt(parts) returned error \"%v\"", err)
	}
	//Todo (cbergoon): Improve resolution of the time.
	//if !newEntryOptions.expTime.Equal(now) {
	//	t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) expected newEntryOptions.expTime == %v got newEntryOptions.expTime == %v", now, newEntryOptions.expTime)
	//}
	//if !newEntryOptions.invTime.Equal(now) {
	//	t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) expected newEntryOptions.invTime == %v got newEntryOptions.invTime == %v", now, newEntryOptions.invTime)
	//}
	if newEntryOptions.tol != 9.99 {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(), InvalidTime(now), Tol(9.99)) expected newEntryOptions.tol == %v got newEntryOptions.tol == %v", 9.99, newEntryOptions.tol)
	}
}
