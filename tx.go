package main

import (
	"strings"

	"math"

	"github.com/cbergoon/btree"
	"github.com/dhconnelly/rtreego"
	"github.com/juju/errors"
)

type RbCtx struct {
	backward      map[string]*Entry
	forward       map[string]*Entry
	backwardIndex map[string]*Index
}

type Tx struct {
	db        *StitchDB
	bkt       *Bucket
	mode      RWMode
	rbctx     *RbCtx
	iterating bool
}

func NewTx(db *StitchDB, bkt *Bucket, mode RWMode) (*Tx, error) {
	return &Tx{
		db:   db,
		bkt:  bkt,
		mode: mode,
		rbctx: &RbCtx{
			//Holds the backward changes made during the transaction. Keys with a nil value were inserted
			//during the transaction and should be deleted. Keys with a non-nil value were deleted
			//during the transaction and should be inserted.
			backward: make(map[string]*Entry),
			//Holds the backward index changes made during the transaction. Keys with a nil value were created
			//and need to be deleted on rollback. Keys with a non-nil value were dropped and need to be replaced
			//with the value on rollback.
			backwardIndex: make(map[string]*Index),
			//Holds the forward changes made during the transaction. Keys with a nil value were deleted during
			//the transaction and should be deleted. Keys with a non-nil value were inserted during the transaction
			//and should be inserted.
			forward: make(map[string]*Entry),
		},
	}, nil
}

func (t *Tx) RollbackTx() error {
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

func (t *Tx) CommitTx() error {
	if !t.db.open {
		return errors.New("error: tx: db is closed")
	}
	if t.mode == MODE_READ {
		return errors.New("error: tx: cannot commit read only transaction")
	}
	if t.mode == MODE_READ_WRITE {
		for key, entry := range t.rbctx.forward {
			if entry == nil { //Entry was deleted or overwritten during transaction; delete/overwrite
				t.bkt.WriteDeleteEntry(&Entry{k: key})
			} else { //Entry was inserted during transaction; insert
				t.bkt.WriteInsertEntry(entry)
			}
		}
		t.bkt.WriteAOFBuf()
	}
	t.unlock()
	return nil
}

func (t *Tx) lock() {
	if t.mode == MODE_READ {
		t.bkt.bktlock.RLock()
	} else if t.mode == MODE_READ_WRITE {
		t.bkt.bktlock.Lock()
	}
}

func (t *Tx) unlock() {
	if t.mode == MODE_READ {
		t.bkt.bktlock.RUnlock()
	} else if t.mode == MODE_READ_WRITE {
		t.bkt.bktlock.Unlock()
	}
}

func (t *Tx) setIterating(i bool) {
	t.iterating = i
}

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
	t.bkt.data.Descend(i)
	return nil
}

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

func (t *Tx) Indexes() ([]string, error) {
	var idxs []string
	for i := range t.bkt.indexes {
		idxs = append(idxs, i)
	}
	return idxs, nil
}

func (t *Tx) Min(index string) (*Entry, error) {
	var item btree.Item
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		item = t.bkt.indexes[index].t.Min()

	} else {
		item = t.bkt.data.Min()
	}
	return item.(*Entry), nil
}

func (t *Tx) Max(index string) (*Entry, error) {
	var item btree.Item
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		item = t.bkt.indexes[index].t.Max()

	} else {
		item = t.bkt.data.Max()
	}
	return item.(*Entry), nil
}

func (t *Tx) Has(index string, e *Entry) (bool, error) {
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		return t.bkt.indexes[index].t.Has(e), nil
	} else {
		return t.bkt.data.Has(e), nil
	}
}

func (t *Tx) Size(index string) (int, error) {
	if strings.TrimSpace(index) != "" && t.bkt.indexExists(index) {
		return t.bkt.indexes[index].t.Len(), nil
	} else {
		return t.bkt.data.Len(), nil
	}
}

func (t *Tx) SearchIntersect(bb *rtreego.Rect, filters ...rtreego.Filter) ([]*Entry, error) {
	if !t.bkt.options.geo {
		return nil, errors.New("error: tx: bucket is not geo")
	}
	var res []*Entry
	e := t.bkt.rtree.SearchIntersect(bb, filters...)
	for _, s := range e {
		res = append(res, s.(*Entry))
	}
	return res, nil
}

//func (t *Tx) SearchIntersectWithLimit(k int, bb *rtreego.Rect) ([]*Entry, error) {
//	if !t.bkt.options.geo {
//		return nil, errors.New("error: tx: bucket is not geo")
//	}
//	var res []*Entry
//	e := t.bkt.rtree.SearchIntersectWithLimit(k, bb)
//	for _, s := range e {
//		res = append(res, s.(*Entry))
//	}
//	return res, nil
//}

func (t *Tx) SearchWithinRadius(p rtreego.Point, radius float64) ([]*Entry, error) {
	//if len(p) != t.bkt.options.dims {
	//	fmt.Println(t.bkt.options.dims)
	//	return nil, errors.New("error: tx: invalid dimension for bucket")
	//}
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
			var dsub, d float64
			for i := 0; i < len(xpmod); i++ {
				dsub = dsub + ((midp[i] - xpmod[i]) * (midp[i] - xpmod[i]))
			}
			//d = math.Sqrt(dsub)
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

func (t *Tx) NearestNeighbor(p rtreego.Point) (*Entry, error) {
	if !t.bkt.options.geo {
		return nil, errors.New("error: tx: bucket is not geo")
	}
	e := t.bkt.rtree.NearestNeighbor(p)
	return e.(*Entry), nil
}

func (t *Tx) NearestNeighbors(k int, p rtreego.Point, filters ...rtreego.Filter) ([]*Entry, error) {
	if !t.bkt.options.geo {
		return nil, errors.New("error: tx: bucket is not geo")
	}
	var res []*Entry
	e := t.bkt.rtree.NearestNeighbors(k, p, filters...)
	for _, s := range e {
		res = append(res, s.(*Entry))
	}
	return res, nil
}

func (t *Tx) GetAllBoundingBoxes() ([]*rtreego.Rect, error) {
	if !t.bkt.options.geo {
		return nil, errors.New("error: tx: bucket is not geo")
	}
	return t.bkt.rtree.GetAllBoundingBoxes(), nil
}
