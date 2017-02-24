package main

import (
	"errors"
	"time"

	"github.com/cbergoon/btree"
)

type RbCtx struct {
	changes map[string]*Entry
}

type Tx struct {
	db    *StitchDB
	bkt   *Bucket
	mode  RWMode
	rbctx *RbCtx
}

func NewTx(db *StitchDB, bkt *Bucket, mode RWMode) (*Tx, error) {
	return &Tx{
		db:   db,
		bkt:  bkt,
		mode: mode,
		rbctx: &RbCtx{
			//Holds the changes made during the transaction. Keys with a nil value were inserted
			//during the transaction and should be deleted. Keys with a non-nil value were deleted
			//durign the transaction and should be inserted.
			changes: make(map[string]*Entry),
		},
	}, nil
}

func (t *Tx) RollbackTx() error {
	for key, entry := range t.rbctx.changes {
		if entry == nil { //Entry was inserted during transaction; delete
			t.bkt.delete(&Entry{k: key})
		} else { //Entry was deleted or overwritten during transaction; insert
			t.bkt.insert(entry)
		}
	}
	//Todo: Indexes
	t.unlock()
	return nil
}

func (t *Tx) CommitTx() error {
	if !t.db.open {
		return errors.New("error: db is closed")
	}
	if t.mode == MODE_READ {
		return errors.New("error: cannot commit read only transaction")
	}
	if t.mode == MODE_READ_WRITE {
		//Commit changes
		//write set write delete
		//sync file
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

func (t *Tx) Ascend(f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.bkt.data.Ascend(i)
	return nil
}

func (t *Tx) Descend(f func(e *Entry) bool) error {
	i := func(i btree.Item) bool {
		eItem := i.(*Entry)
		return f(eItem)
	}
	t.bkt.data.Descend(i)
	return nil
}

func (t *Tx) AscendIndex(index string, f func(e *Entry) bool) error {
	return nil
}

func (t *Tx) DescendIndex(index string, f func(e *Entry) bool) error {
	return nil
}

func (t *Tx) Get(e *Entry) (*Entry, error) {
	if !t.db.open || t.bkt == nil || !t.bkt.open {
		return nil, errors.New("error: cannot get entry; db is in invalid state")
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
	if !t.db.open || t.bkt == nil || !t.bkt.open {
		return nil, errors.New("error: cannot set entry; db is in invalid state")
	}
	pres := t.bkt.insert(e)
	t.rbctx.changes[e.k] = pres
	return pres, nil
}

func (t *Tx) Delete(e *Entry) (*Entry, error) {
	if !t.db.open || t.bkt == nil || !t.bkt.open {
		return nil, errors.New("error: cannot delete entry; db is in invalid state")
	}
	dres := t.bkt.delete(e)
	if dres != nil {
		t.rbctx.changes[e.k] = dres
	}
	return dres, nil
}

func (t *Tx) CreateIndex(index string, pattern string) error {
	return nil
}

func (t *Tx) DropIndex(index string) error {
	return nil
}

func (t *Tx) Indexes() ([]string, error) {
	return nil, nil
}

func (t *Tx) Min() (*Entry, error) {
	return nil, nil
}

func (t *Tx) Max() (*Entry, error) {
	return nil, nil
}

func (t *Tx) Has(e *Entry) (bool, error) {
	return false, nil
}

func (t *Tx) Degree() (int, error) {
	return 0, nil
}

func (t *Tx) ExpiresIn(key string) (time.Duration, error) {
	return time.Second, nil
}
