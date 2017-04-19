// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"math"
	"strings"

	"github.com/cbergoon/btree"
	"github.com/dhconnelly/rtreego"
	"github.com/juju/errors"
)

//RbCtx preserves the state of the tree during a transaction representing the changes made to allow for commits/rollbacks.
type RbCtx struct {
	//Holds the backward changes made during the transaction. Keys with a nil value were inserted
	//during the transaction and should be deleted. Keys with a non-nil value were deleted
	//during the transaction and should be inserted.
	backward map[string]*Entry
	//Holds the backward index changes made during the transaction. Keys with a nil value were created
	//and need to be deleted on rollback. Keys with a non-nil value were dropped and need to be replaced
	//with the value on rollback.
	backwardIndex map[string]*Index
	//Holds the forward changes made during the transaction. Keys with a nil value were deleted during
	//the transaction and should be deleted. Keys with a non-nil value were inserted during the transaction
	//and should be inserted.
	forward map[string]*Entry
}

//Tx represents the a transaction including rollback information.
type Tx struct {
	db        *StitchDB                 //DB that bucket is contained in. See bkt.
	bkt       *Bucket                   //Bucket that the this tx is operating on
	mode      RWMode                    //Describes if tx is read-only or read-write
	rbctx     *RbCtx                    //Context containing changes to bucket
	iterating bool                      //True if iterating over tree; used to prevent effects of updates while iterating.
	sysperf   []*SystemPerformanceEntry //Slice of entries to be committed; contains matrics on tx operations
}

//newTx creates a new transaction for the DB and bucket provided with the RW specified modifier.
func newTx(db *StitchDB, bkt *Bucket, mode RWMode) (*Tx, error) {
	return &Tx{
		db:   db,
		bkt:  bkt,
		mode: mode,
		rbctx: &RbCtx{
			backward:      make(map[string]*Entry), //Changes to main tree during tx to rollback (backward).
			backwardIndex: make(map[string]*Index), //Changes to the index trees during tx to rollback (backward).
			forward:       make(map[string]*Entry), //Changes to main tree during tx to commit (forward).
		},
	}, nil
}

//rollbackTx iterates over backward changes stored in rollback context rbctx and returns the bucket to a state
//equivalent to the state of the bucket pre-transaction.
func (t *Tx) rollbackTx() error {
	for key, entry := range t.rbctx.backward {
		if entry == nil { //Entry was inserted during transaction; delete
			t.bkt.delete(&Entry{k: key})
			for _, ind := range t.bkt.indexes {
				ind.t.Delete(&Entry{k: key})
			}
		} else { //Entry was deleted or overwritten during transaction; insert
			t.bkt.insert(entry)
			for _, ind := range t.bkt.indexes {
				ind.t.ReplaceOrInsert(entry)
			}
		}
	}
	t.unlock()
	return nil
}

//commitTx iterates over forward changes to the bucket and persists changes to the AOF.
func (t *Tx) commitTx() error {
	if !t.db.open {
		return errors.New("error: tx: db is closed")
	}
	if t.mode == MODE_READ {
		return errors.New("error: tx: cannot commit read only transaction")
	}
	if t.mode == MODE_READ_WRITE {
		for key, entry := range t.rbctx.forward {
			if entry == nil { //Entry was deleted or overwritten during transaction; delete/overwrite
				t.bkt.writeDeleteEntry(&Entry{k: key})
			} else { //Entry was inserted during transaction; insert
				t.bkt.writeInsertEntry(entry)
			}
		}
		t.bkt.writeAOFBuf()
	}
	t.unlock()
	return nil
}

//lock is a helper function to obtain a lock on the bucket appropriately based on the RW modifier of the transaction.
func (t *Tx) lock() {
	if t.mode == MODE_READ {
		t.bkt.bktlock.RLock()
	} else if t.mode == MODE_READ_WRITE {
		t.bkt.bktlock.Lock()
	}
}

//unlock is a helper function to release the lock on the bucket appropriately based on the RW modifier of the transaction.
func (t *Tx) unlock() {
	if t.mode == MODE_READ {
		t.bkt.bktlock.RUnlock()
	} else if t.mode == MODE_READ_WRITE {
		t.bkt.bktlock.Unlock()
	}
}

//setIterating sets the iterating flag to the specified value.
func (t *Tx) setIterating(i bool) {
	t.iterating = i
}

//Ascend iterates over the items in the bucket using the specified index for each item calling the provided function f
//terminating only when there are no more entries in the bucket or the provided function returns false. An empty string
//represents no index in which case entries will use the default key ordering.
//Note: only the portion of the entry that the index is built with needs to be populated.
func (t *Tx) Ascend(index string, f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.setIterating(true)
	defer t.setIterating(false)
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		t.bkt.indexes[index].t.Ascend(i)
	} else {
		t.bkt.data.Ascend(i)
	}
	return nil
}

//AscendGreaterOrEqual iterates over the items in the bucket using the specified index for each item greater than or equal to the
//pivot entry calling the provided function f terminating only when there are no more entries in the bucket or the
//provided function returns false. An empty string represents no index in which case entries will use the default key
//ordering.
//Note: only the portion of the entry that the index is built with needs to be populated.
func (t *Tx) AscendGreaterOrEqual(index string, pivot *Entry, f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.setIterating(true)
	defer t.setIterating(false)
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		t.bkt.indexes[index].t.AscendGreaterOrEqual(pivot, i)
	} else {
		t.bkt.data.AscendGreaterOrEqual(pivot, i)
	}
	return nil
}

//AscendLessThan iterates over the items in the bucket using the specified index for each item less than the pivot entry
//calling the provided function f. Iteration terminates only when there are no more entries less than pivot in the bucket
//or the provided function returns false. An empty string represents no index in which case entries will use the default
//key ordering.
//Note: only the portion of the entry that the index is built with needs to be populated.
func (t *Tx) AscendLessThan(index string, pivot *Entry, f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.setIterating(true)
	defer t.setIterating(false)
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		t.bkt.indexes[index].t.AscendLessThan(pivot, i)
	} else {
		t.bkt.data.AscendLessThan(pivot, i)
	}
	return nil
}

//AscendRange iterates over the items in the bucket that are greater than or equal to greaterOrEqual and less than
//lessThan calling the provided function f. Iteration terminates only when there are no more entries in the range or
//the provided function returns false. An empty string represents no index in which case entries will use the default
//key ordering.
//Note: only the portion of the entry that the index is built with needs to be populated.
func (t *Tx) AscendRange(index string, greaterOrEqual *Entry, lessThan *Entry, f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.setIterating(true)
	defer t.setIterating(false)
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		t.bkt.indexes[index].t.AscendRange(greaterOrEqual, lessThan, i)
	} else {
		t.bkt.data.AscendRange(greaterOrEqual, lessThan, i)
	}
	return nil
}

//Descend iterates over the items in the bucket using the specified index for each item calling the provided function f
//terminating only when there are no more entries in the bucket or the provided function returns false. An empty string
//represents no index in which case entries will use the default key ordering.
//Note: only the portion of the entry that the index is built with needs to be populated.
func (t *Tx) Descend(index string, f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.setIterating(true)
	defer t.setIterating(false)
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		t.bkt.indexes[index].t.Descend(i)
	} else {
		t.bkt.data.Descend(i)
	}
	return nil
}

//DescendGreaterThan iterates over the items in the bucket using the specified index for each item greater than to the
//pivot entry calling the provided function f terminating only when there are no more entries greater than pivot in the
//bucket or the provided function returns false. An empty string represents no index in which case entries will use the
//default key ordering.
//Note: only the portion of the entry that the index is built with needs to be populated.
func (t *Tx) DescendGreaterThan(index string, pivot *Entry, f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.setIterating(true)
	defer t.setIterating(false)
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		t.bkt.indexes[index].t.DescendGreaterThan(pivot, i)
	} else {
		t.bkt.data.DescendGreaterThan(pivot, i)
	}
	return nil
}

//DescendLessOrEqual iterates over the items in the bucket using the specified index for each item less than the pivot entry
//calling the provided function f. Iteration terminates only when there are no more entries less than or equal to pivot
//in the bucket or the provided function returns false. An empty string represents no index in which case entries will
//use the default key ordering.
//Note: only the portion of the entry that the index is built with needs to be populated.
func (t *Tx) DescendLessOrEqual(index string, pivot *Entry, f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.setIterating(true)
	defer t.setIterating(false)
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		t.bkt.indexes[index].t.DescendLessOrEqual(pivot, i)
	} else {
		t.bkt.data.DescendLessOrEqual(pivot, i)
	}
	return nil
}

//DescendRange iterates over the items in the bucket that are less than or equal to lessOrEqual and greater than
//greaterThan calling the provided function f. Iteration terminates only when there are no more entries in the range or
//the provided function returns false. An empty string represents no index in which case entries will use the default
//key ordering.
//Note: only the portion of the entry that the index is built with needs to be populated.
func (t *Tx) DescendRange(index string, lessOrEqual *Entry, greaterThan *Entry, f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.setIterating(true)
	defer t.setIterating(false)
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		t.bkt.indexes[index].t.DescendRange(lessOrEqual, greaterThan, i)
	} else {
		t.bkt.data.DescendRange(lessOrEqual, greaterThan, i)
	}
	return nil
}

//Get returns an entry from the bucket using the default tree to search (i.e. searches on entry key). Returns nil if the
//the entry is invalid, expired, or not found in the bucket. Returns an error if the db or bucket is closed.
func (t *Tx) Get(e *Entry) (*Entry, error) {
	if !t.db.open || t.bkt == nil || !t.bkt.open {
		return nil, errors.New("error: tx: cannot get entry; db is in invalid state")
	}
	res := t.bkt.get(e)
	if res != nil {
		if res.IsExpired() || res.IsInvalid() {
			return nil, nil
		}
	}
	return res, nil
}

//Set inserts an entry into the bucket. If the key of the entry to insert already exists in the tree the old entry is
//replaced and returned otherwise returns nil. Returns an error if the transaction is iterating and if the the db or bucket
//is closed.
func (t *Tx) Set(e *Entry) (*Entry, error) {
	if t.iterating {
		return nil, errors.New("error: tx: transaction is iterating; cannot set entry")
	}
	if !t.db.open || t.bkt == nil || !t.bkt.open {
		return nil, errors.New("error: tx: cannot set entry; db is in invalid state")
	}
	pres := t.bkt.insert(e)
	t.rbctx.backward[e.k] = pres
	t.rbctx.forward[e.k] = e
	return pres, nil
}

//Delete removes an entry from the bucket. If an entry is removed returns the removed entry otherwise returns nil. Returns
//an error if the db or bucket is closed.
func (t *Tx) Delete(e *Entry) (*Entry, error) {
	if t.iterating {
		return nil, errors.New("error: tx: transaction is iterating; cannot set entry")
	}
	if !t.db.open || t.bkt == nil || !t.bkt.open {
		return nil, errors.New("error: tx: cannot delete entry; db is in invalid state")
	}
	dres := t.bkt.delete(e)
	if dres != nil {
		t.rbctx.backward[e.k] = dres
		t.rbctx.forward[e.k] = nil
	}
	return dres, nil
}

//CreateIndex builds an index over a field of the value of the entry. The field is identified by pattern and its type is
//described by vtype. Returns an error if the db or bucket is closed, the index already exists, or if an error occurred
//while populating the index.
func (t *Tx) CreateIndex(pattern string, vtype IndexValueType) error {
	if !t.db.open || t.bkt == nil || !t.bkt.open {
		return errors.New("error: tx: cannot create index; db is in invalid state")
	}
	curr, ok := t.bkt.indexes[pattern]
	if ok && curr != nil {
		return errors.New("error: tx: cannot create index; index already exists")
	}
	//Create Index
	index, err := NewIndex(pattern, vtype, t.bkt)
	if err != nil {
		return errors.Annotate(err, "error: tx: could not create index")
	}
	t.bkt.indexes[pattern] = index
	//Add to backward indexes with nil value
	t.rbctx.backwardIndex[pattern] = nil
	//Rebuild Index
	t.bkt.indexes[pattern].build()
	return nil
}

//DropIndex removes an index specified by pattern. Returns an error if the db or bucket is closed or if the index does
//not exist.
func (t *Tx) DropIndex(pattern string) error {
	if !t.db.open || t.bkt == nil || !t.bkt.open {
		return errors.New("error: tx: cannot drop index; db is in invalid state")
	}
	//Add to backward indexes with pointer to index value
	index, ok := t.bkt.indexes[pattern]
	if !ok || index == nil {
		return errors.New("error: tx: cannot drop; index does not exist")
	}
	t.rbctx.backwardIndex[pattern] = index
	//Set map pointer to nil, Delete entry from index map
	t.bkt.indexes[pattern] = nil
	delete(t.bkt.indexes, pattern)
	return nil
}

//Indexes returns a slice of strings containing the names (patterns) of all indexes in the bucket.
func (t *Tx) Indexes() ([]string, error) {
	var idxs []string
	for i := range t.bkt.indexes {
		idxs = append(idxs, i)
	}
	return idxs, nil
}

//Min returns the minimum value entry inthe bucket for a given index. An empty string represents no index in which case
//the entry with the minimum key will be found.
func (t *Tx) Min(index string) (*Entry, error) {
	var item btree.Item
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		item = t.bkt.indexes[index].t.Min()

	} else {
		item = t.bkt.data.Min()
	}
	return item.(*Entry), nil
}

//Max returns the maximum value entry inthe bucket for a given index. An empty string represents no index in which case
//the entry with the maximum key will be found.
func (t *Tx) Max(index string) (*Entry, error) {
	var item btree.Item
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		item = t.bkt.indexes[index].t.Max()

	} else {
		item = t.bkt.data.Max()
	}
	return item.(*Entry), nil
}

//Has chacks if an entry exists in the bucket for a given index. An empty string represents no index in which case
//entries will use the default key ordering.
func (t *Tx) Has(index string, e *Entry) (bool, error) {
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		return t.bkt.indexes[index].t.Has(e), nil
	}
	return t.bkt.data.Has(e), nil
}

//Size returns the number of entries in the bucket.
func (t *Tx) Size(index string) (int, error) {
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		return t.bkt.indexes[index].t.Len(), nil
	}
	return t.bkt.data.Len(), nil
}

//SearchIntersect finds entries of the bucket that fall within the bounds of the provided rectangle. Bucket must be
//configured for geolocation. Returns a slice containing pointers to the entries that are within the bounds of the rectangle.
//Returns an error if the bucket is not geo enabled.
func (t *Tx) SearchIntersect(rbb *Rect) ([]*Entry, error) {
	if !t.bkt.options.geo {
		return nil, errors.New("error: tx: bucket is not geo")
	}
	bb, _ := rtreegoRect(rbb)
	var res []*Entry
	e := t.bkt.rtree.SearchIntersect(bb)
	for _, s := range e {
		res = append(res, s.(*Entry))
	}
	return res, nil
}

//SearchWithinRadius finds entries that are within an n-dimensional sphere centered at point pt with a radius of radius.
//This condition is determined by finding the Euclidean distance between the center point of the n-sphere and the point
//in question. The result is then determined by comparing the radius of the n-sphere and the distance between the two
//points. If the GeoRangeIsInclusive option is set for the bucket then the point is found the be within the n-sphere if
//the distance between the two points is less than the specified radius. If the GeoRangeIsInclusive option is not set
//then the point is found to be within the n-sphere if the distance between the two points is less than or equal to the
//specified radius. Returns an error if the bucket is not geo enabled.
func (t *Tx) SearchWithinRadius(pt Point, radius float64) ([]*Entry, error) {
	//if len(p) != t.bkt.options.dims {
	//	fmt.Println(t.bkt.options.dims)
	//	return nil, errors.New("error: tx: invalid dimension for bucket")
	//}
	p := rtreegoPoint(pt)
	d := 2 * radius
	var dls []float64
	for i := 0; i < len(p); i++ {
		dls = append(dls, d)
	}
	var pmod rtreego.Point
	for i := 0; i < len(p); i++ {
		pmod = append(pmod, p[i]-radius)
	}
	rrect, _ := rtreego.NewRect(pmod, dls)
	radiusFilter := func(xp rtreego.Point, r float64) func(results []rtreego.Spatial, object rtreego.Spatial) (refuse, abort bool) {
		return func(results []rtreego.Spatial, object rtreego.Spatial) (refuse, abort bool) {
			var xpmod rtreego.Point
			for i := 0; i < len(xp); i++ {
				xpmod = append(xpmod, xp[i]+radius)
			}
			entry := object.(*Entry)
			if len(xpmod) != len(entry.location) {
				return true, true
			}
			var midp rtreego.Point
			midp = entry.location
			//for i := 0; i < len(entry.location); i++ {
			//	midp = append(midp, entry.location[i]/2)
			//}
			//Euclidean distance: sqrt((q1-p1)^2 + (q2-p2)^2 + ... + (qn-pn)^2)
			var dsub, d float64
			for i := 0; i < len(xpmod); i++ {
				dsub = dsub + ((midp[i] - xpmod[i]) * (midp[i] - xpmod[i]))
			}
			d = math.Sqrt(dsub)
			if d < r {
				return false, false
			} else if d == r {
				return !t.bkt.options.georincl, false
			} else {
				return true, false
			}
		}
	}
	e := t.bkt.rtree.SearchIntersect(rrect, radiusFilter(pmod, radius))
	var res []*Entry
	for _, s := range e {
		res = append(res, s.(*Entry))
	}
	return res, nil
}

//NearestNeighbor returns the closest neighbor to a given point pt. Returns an error if the bucket is not geo enabled.
func (t *Tx) NearestNeighbor(pt Point) (*Entry, error) {
	p := rtreegoPoint(pt)
	if !t.bkt.options.geo {
		return nil, errors.New("error: tx: bucket is not geo")
	}
	e := t.bkt.rtree.NearestNeighbor(p)
	return e.(*Entry), nil
}

//NearestNeighbors returns a slice of the k closest entries to a given point pt. Returns an error if the bucket is not
//geo enabled.
func (t *Tx) NearestNeighbors(k int, pt Point) ([]*Entry, error) {
	if !t.bkt.options.geo {
		return nil, errors.New("error: tx: bucket is not geo")
	}
	p := rtreegoPoint(pt)
	var res []*Entry
	e := t.bkt.rtree.NearestNeighbors(k, p)
	for _, s := range e {
		res = append(res, s.(*Entry))
	}
	return res, nil
}
