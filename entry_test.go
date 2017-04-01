// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"strings"
	"testing"
	"time"
)

func TestNewEntry(t *testing.T) {
	options, err := NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntry("Test01", "{\"coords\": [1.0, 3.0, 4.0]}", true, options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test01\", \"\", false, options), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	if entry1.k != "Test01" {
		t.Errorf("Failure: Expected entry1.k == \"Test01\" got entry1.k == %v", entry1.k)
	}
	if entry1.v != "{\"coords\": [1.0, 3.0, 4.0]}" {
		t.Errorf("Failure: Expected entry1.v == \"{\"coords\": [1.0, 3.0, 4.0]}\" got entry1.v == %v", entry1.v)
	}
	if entry1.location[0] != 1.0 {
		t.Errorf("Failure: Expected entry1.location[0] == 1.0 got entry1.location[0] == %v", entry1.location[0])
	}
	if entry1.location[1] != 3.0 {
		t.Errorf("Failure: Expected entry1.location[1] == 3.0 got entry1.location[1] == %v", entry1.location[1])
	}
	if entry1.location[2] != 4.0 {
		t.Errorf("Failure: Expected entry1.location[2] == 4.0 got entry1.location[2] == %v", entry1.location[2])
	}
	entry2, err := NewEntry("Test02", "{\"coords\": [2.0, 4.0, 5.0]}", true, options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test02\", \"\", false, options), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	if entry2.k != "Test02" {
		t.Errorf("Failure: Expected entry2.k == \"Test02\" got entry2.k == %v", entry2.k)
	}
	if entry2.v != "{\"coords\": [2.0, 4.0, 5.0]}" {
		t.Errorf("Failure: Expected entry2.v == \"{\"coords\": [2.0, 4.0, 5.0]}\" got entry2.v == %v", entry2.v)
	}
	if entry2.location[0] != 2.0 {
		t.Errorf("Failure: Expected entry2.location[0] == 2.0 got entry2.location[0] == %v", entry2.location[0])
	}
	if entry2.location[1] != 4.0 {
		t.Errorf("Failure: Expected entry2.location[1] == 4.0 got entry2.location[1] == %v", entry2.location[1])
	}
	if entry2.location[2] != 5.0 {
		t.Errorf("Failure: Expected entry2.location[2] == 5.0 got entry2.location[2] == %v", entry2.location[2])
	}
}

func TestNewEntryWithGeo(t *testing.T) {
	options, err := NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "{\"coords\": [1.0, 3.0, 4.0]}", options)
	if err != nil {
		t.Errorf("Failure: NewEntryWithGeo(\"Test01\", \"{\"coords\": [1.0, 3.0, 4.0]}\", options) returned error \"%v\"", err)
	}
	if entry1.k != "Test01" {
		t.Errorf("Failure: Expected entry1.k == \"Test01\" got entry1.k == %v", entry1.k)
	}
	if entry1.v != "{\"coords\": [1.0, 3.0, 4.0]}" {
		t.Errorf("Failure: Expected entry1.v == \"{\"coords\": [1.0, 3.0, 4.0]}\" got entry1.v == %v", entry1.v)
	}
	if entry1.location[0] != 1.0 {
		t.Errorf("Failure: Expected entry1.location[0] == 1.0 got entry1.location[0] == %v", entry1.location[0])
	}
	if entry1.location[1] != 3.0 {
		t.Errorf("Failure: Expected entry1.location[1] == 3.0 got entry1.location[1] == %v", entry1.location[1])
	}
	if entry1.location[2] != 4.0 {
		t.Errorf("Failure: Expected entry1.location[2] == 4.0 got entry1.location[2] == %v", entry1.location[2])
	}
}

func TestEntry_Less(t *testing.T) {
	options, err := NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "", options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test01\", \"\", false, options), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	entry2, err := NewEntry("Test02", "", false, options)
	if err != nil {
		t.Errorf("Failure: Expected entry1.k == \"Test01\" got entry1.k == %v", entry1.k)
	}
	entry3, err := NewEntry("Test03", "", true, options)
	if err != nil {
		t.Errorf("Failure: Expected entry1.v == \"\" got entry1.v == %v", entry1.v)
	}
	if !entry1.Less(entry2, nil) {
		t.Errorf("Failure: Expected entry1 < entry2")
	}
	if entry2.Less(entry1, nil) {
		t.Errorf("Failure: Expected entry2 > entry1")
	}
	if !entry2.Less(entry3, nil) {
		t.Errorf("Failure: Expected entry2 < entry3")
	}
	if entry3.Less(entry2, nil) {
		t.Errorf("Failure: Expected entry3 > entry2")
	}
	if !entry1.Less(entry3, nil) {
		t.Errorf("Failure: Expected entry1 < entry3")
	}
}

func TestEntry_IsExpired(t *testing.T) {
	options1, err := NewEntryOptions(ExpireTime(time.Now().Add(-1*time.Second)), InvalidTime(time.Now().Add(-1*time.Second)), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now().Add(-1 * time.Second)), InvalidTime(time.Now().Add(-1 * time.Second)), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "", options1)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test01\", \"\", true, options1) returned error \"%v\"", err)
	}
	options2, err := NewEntryOptions(ExpireTime(time.Now().Add(time.Second)), InvalidTime(time.Now().Add(time.Second)), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now().Add(time.Second)), InvalidTime(time.Now().Add(time.Second)), Tol(9.9)) returned error \"%v\"", err)
	}
	entry2, err := NewEntry("Test02", "", false, options2)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test02\", \"\", true, options2) returned error \"%v\"", err)
	}
	options3, err := NewEntryOptions(ExpireTime(time.Now().Add(time.Minute)), InvalidTime(time.Now().Add(time.Minute)), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now().Add(time.Minute)), InvalidTime(time.Now().Add(time.Minute)), Tol(9.9)) returned error \"%v\"", err)
	}
	entry3, err := NewEntry("Test03", "", true, options3)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test03\", \"\", true, options3) returned error \"%v\"", err)
	}
	// Entry 1 Pre-Sleep
	if !entry1.IsExpired() {
		t.Errorf("Failure: Expected entry1 to be expired")
	}
	// Entry 2 Pre-Sleep
	if entry2.IsExpired() {
		t.Errorf("Failure: Expected entry2 to be not expired")
	}
	// Entry 3 Pre-Sleep
	if entry3.IsExpired() {
		t.Errorf("Failure: Expected entry3 to be not expired")
	}
	time.Sleep(time.Second * 2)
	// Entry 1 Post-Sleep
	if !entry1.IsExpired() {
		t.Errorf("Failure: Expected entry1 to be expired")
	}
	// Entry 2 Post-Sleep
	if !entry2.IsExpired() {
		t.Errorf("Failure: Expected entry2 to be expired")
	}
	// Entry 3 Post-Sleep
	if entry3.IsExpired() {
		t.Errorf("Failure: Expected entry3 to be not expired")
	}
}

func TestEntry_IsInvalid(t *testing.T) {
	options1, err := NewEntryOptions(ExpireTime(time.Now().Add(-1*time.Second)), InvalidTime(time.Now().Add(-1*time.Second)), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now().Add(-1 * time.Second)), InvalidTime(time.Now().Add(-1 * time.Second)), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "", options1)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test01\", \"\", true, options1) returned error \"%v\"", err)
	}
	options2, err := NewEntryOptions(ExpireTime(time.Now().Add(time.Second)), InvalidTime(time.Now().Add(time.Second)), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now().Add(time.Second)), InvalidTime(time.Now().Add(time.Second)), Tol(9.9)) returned error \"%v\"", err)
	}
	entry2, err := NewEntry("Test02", "", false, options2)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test02\", \"\", true, options2) returned error \"%v\"", err)
	}
	options3, err := NewEntryOptions(ExpireTime(time.Now().Add(time.Minute)), InvalidTime(time.Now().Add(time.Minute)), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now().Add(time.Minute)), InvalidTime(time.Now().Add(time.Minute)), Tol(9.9)) returned error \"%v\"", err)
	}
	entry3, err := NewEntry("Test03", "", true, options3)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test03\", \"\", true, options3) returned error \"%v\"", err)
	}
	// Entry 1 Pre-Sleep
	if !entry1.IsInvalid() {
		t.Errorf("Failure: Expected entry1 to be invalid")
	}
	// Entry 2 Pre-Sleep
	if entry2.IsInvalid() {
		t.Errorf("Failure: Expected entry2 to be not invalid")
	}
	// Entry 3 Pre-Sleep
	if entry3.IsInvalid() {
		t.Errorf("Failure: Expected entry3 to be not invalid")
	}
	time.Sleep(time.Second * 2)
	// Entry 1 Post-Sleep
	if !entry1.IsInvalid() {
		t.Errorf("Failure: Expected entry1 to be invalid")
	}
	// Entry 2 Post-Sleep
	if !entry2.IsInvalid() {
		t.Errorf("Failure: Expected entry2 to be invalid")
	}
	// Entry 3 Post-Sleep
	if entry3.IsInvalid() {
		t.Errorf("Failure: Expected entry3 to be not invalid")
	}
}

func TestEntry_ExpiresAt(t *testing.T) {
	past := time.Now().Add(-1 * time.Second)
	options1, err := NewEntryOptions(ExpireTime(past), InvalidTime(past), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(past), InvalidTime(past), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "", options1)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test01\", \"\", true, options1) returned error \"%v\"", err)
	}
	if entry1.ExpiresAt() != past {
		t.Errorf("Failure: Expected entry1.ExpiresAt() == %v got entry1.ExpiresAt() == %v", past, entry1.ExpiresAt())
	}
}

func TestEntry_InvalidatesAt(t *testing.T) {
	past := time.Now().Add(-1 * time.Second)
	options1, err := NewEntryOptions(ExpireTime(past), InvalidTime(past), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(past), InvalidTime(past), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "", options1)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test01\", \"\", true, options1) returned error \"%v\"", err)
	}
	if entry1.InvalidatesAt() != past {
		t.Errorf("Failure: Expected entry1.InvalidatesAt() == %v got entry1.InvalidatesAt() == %v", past, entry1.InvalidatesAt())
	}
}

func TestEntry_Bounds(t *testing.T) {
	options, err := NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(0))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(0)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "{\"coords\": [1.0, 3.0, 4.0]}", options)
	if err != nil {
		t.Errorf("Failure: NewEntryWithGeo(\"Test01\", \"{\"coords\": [1.0, 3.0, 4.0]}\", options) returned error \"%v\"", err)
	}
	if entry1.Bounds().PointCoord(0) != 1.0 || entry1.Bounds().LengthsCoord(0) != 0 {
		t.Errorf("Failure: Invalid resulting rectangle")
	}
	if entry1.Bounds().PointCoord(1) != 3.0 || entry1.Bounds().LengthsCoord(1) != 0 {
		t.Errorf("Failure: Invalid resulting rectangle")
	}
	if entry1.Bounds().PointCoord(2) != 4.0 || entry1.Bounds().LengthsCoord(2) != 0 {
		t.Errorf("Failure: Invalid resulting rectangle")
	}
}

func TestEntry_EntryInsertStmt(t *testing.T) {
	options, err := NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "", options)
	if err != nil {
		t.Errorf("Failure: NewEntryWithGeo(\"Test01\", \"\", options) returned error \"%v\"", err)
	}
	entry2, err := NewEntry("Test02", "{\"coords\": [1.0, 3.0, 4.0]}", false, options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test02\", \"{\"coords\": [1.0, 3.0, 4.0]}\", false, options) returned error \"%v\"", err)
	}
	entry3, err := NewEntry("Test03", "{\"coords\": [1.0, 3.0]}", true, options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test03\", \"{\"coords\": [1.0, 3.0]}\", true, options) returned error \"%v\"", err)
	}
	stmt1i := entry1.EntryInsertStmt()
	if len(stmt1i) != 48 {
		t.Errorf("Failure: Expected statement length 10 got %v", len(stmt1i))
	}
	stmt2i := entry2.EntryInsertStmt()
	if len(stmt2i) != 75 {
		t.Errorf("Failure: Expected statement length 10 got %v", len(stmt2i))
	}
	stmt3i := entry3.EntryInsertStmt()
	if len(stmt3i) != 70 {
		t.Errorf("Failure: Expected statement length 10 got %v", len(stmt3i))
	}
}

func TestEntry_EntryDeleteStmt(t *testing.T) {
	options, err := NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "", options)
	if err != nil {
		t.Errorf("Failure: NewEntryWithGeo(\"Test01\", \"\", options) returned error \"%v\"", err)
	}
	entry2, err := NewEntry("Test02", "{\"coords\": [1.0, 3.0, 4.0]}", false, options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test02\", \"{\"coords\": [1.0, 3.0, 4.0]}\", false, options) returned error \"%v\"", err)
	}
	entry3, err := NewEntry("Test03", "{\"coords\": [1.0, 3.0]}", true, options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test03\", \"{\"coords\": [1.0, 3.0]}\", true, options) returned error \"%v\"", err)
	}
	stmt1d := entry1.EntryDeleteStmt()
	if len(stmt1d) != 48 {
		t.Errorf("Failure: Expected statement length 10 got %v", len(stmt1d))
	}
	stmt2d := entry2.EntryDeleteStmt()
	if len(stmt2d) != 75 {
		t.Errorf("Failure: Expected statement length 10 got %v", len(stmt2d))
	}
	stmt3d := entry3.EntryDeleteStmt()
	if len(stmt3d) != 70 {
		t.Errorf("Failure: Expected statement length 10 got %v", len(stmt3d))
	}
}

func TestNewEntryFromStmt(t *testing.T) {
	options, err := NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9))
	if err != nil {
		t.Errorf("Failure: NewEntryOptions(ExpireTime(time.Now()), InvalidTime(time.Now()), Tol(9.9)) returned error \"%v\"", err)
	}
	entry1, err := NewEntryWithGeo("Test01", "", options)
	if err != nil {
		t.Errorf("Failure: NewEntryWithGeo(\"Test01\", \"\", options) returned error \"%v\"", err)
	}
	entry2, err := NewEntry("Test02", "", false, options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test02\", \"\", false, options) returned error \"%v\"", err)
	}
	entry3, err := NewEntry("Test03", "", true, options)
	if err != nil {
		t.Errorf("Failure: NewEntry(\"Test03\", \"\", true, options) returned error \"%v\"", err)
	}

	stmt1d := entry1.EntryDeleteStmt()
	stmt1dp := strings.Split(string(stmt1d), "~")
	stmt1i := entry1.EntryInsertStmt()
	stmt1ip := strings.Split(string(stmt1i), "~")

	stmt2d := entry2.EntryDeleteStmt()
	stmt2dp := strings.Split(string(stmt2d), "~")
	stmt2i := entry2.EntryInsertStmt()
	stmt2ip := strings.Split(string(stmt2i), "~")

	stmt3d := entry3.EntryDeleteStmt()
	stmt3dp := strings.Split(string(stmt3d), "~")
	stmt3i := entry3.EntryInsertStmt()
	stmt3ip := strings.Split(string(stmt3i), "~")

	entry1d, err := NewEntryFromStmt(stmt1dp)
	if err != nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt1dp) returned error \"%v\"", err)
	}
	if entry1d == nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt1dp) returned nil entry")
	}
	entry1i, err := NewEntryFromStmt(stmt1ip)
	if err != nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt1ip) returned error \"%v\"", err)
	}
	if entry1i == nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt1ip) returned nil entry")
	}
	entry2d, err := NewEntryFromStmt(stmt2dp)
	if err != nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt2dp) returned error \"%v\"", err)
	}
	if entry2d == nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt2dp) returned nil entry")
	}
	entry2i, err := NewEntryFromStmt(stmt2ip)
	if err != nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt2ip) returned error \"%v\"", err)
	}
	if entry2i == nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt2ip) returned nil entry")
	}
	entry3d, err := NewEntryFromStmt(stmt3dp)
	if err != nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt3dp) returned error \"%v\"", err)
	}
	if entry3d == nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt3dp) returned nil entry")
	}
	entry3i, err := NewEntryFromStmt(stmt3ip)
	if err != nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt3ip) returned error \"%v\"", err)
	}
	if entry3i == nil {
		t.Errorf("Failure: NewEntryFromStmt(stmt3ip) returned nil entry")
	}
}

func TestGetEntryComparator(t *testing.T) {
	comp := GetEntryComparator()
	if comp == nil {
		t.Errorf("Failure: Expected non-nil comparator got nil")
	}
}
