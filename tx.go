package main

import "time"

type RbCtx struct {
	added   []*Entry
	deleted []*Entry
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
			added:   make([]*Entry, 0, 100),
			deleted: make([]*Entry, 0, 100),
		},
	}, nil
}

func (t *Tx) RollbackTx() error {
	//Rollback changes
	t.unlock()
	return nil
}

func (t *Tx) CommitTx() error {
	if !t.db.open {
		//Todo: return error
	}
	//tx is write tx
	if t.mode == MODE_READ_WRITE {

	}
	//Commit changes
	//write set write delete
	//sync file
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

func (t *Tx) Ascend(f func()) error {
	return nil
}

func (t *Tx) Descend() error {
	return nil
}

func (t *Tx) AscendIndex() error {
	return nil
}

func (t *Tx) DescendIndex() error {
	return nil
}

func (t *Tx) Get() error {
	return nil
}

func (t *Tx) Set() error {
	return nil
}

func (t *Tx) Delete() error {
	return nil
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
