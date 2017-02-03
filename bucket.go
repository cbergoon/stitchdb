package main

import (
	"github.com/cbergoon/btree"
	"os"
	"sync"
)

type Bucket struct {
	Name     string
	Lock     sync.RWMutex
	Data     *btree.BTree
	Eviction *btree.BTree
	Indexes  map[string]*btree.BTree
	file     *os.File
	Options  *BucketOptions
}

func NewBucket(bucketOptions *BucketOptions) (*Bucket, error) {
	return nil, nil
}

func (b *Bucket) StartTx() (*Tx, error) {
	//if db is not open close
	//lock db
	//create new tx
	//populate rollback

	return nil, nil
}

func (b *Bucket) handleTx(mode RWMode, f func(t *Tx) error) error {
	tx, err := b.StartTx()
	if err != nil {
		//Todo: error could not start transaction
	}
	err = f(tx)
	//if err != nil -> rollback return
	//if writable -> commit
	//if ! writable -> rollback
	return err
}

func (b *Bucket) manger() error {
	//Remove expires
	//if sync 1 second and persist file sync
	//future geo location call backs
	return nil
}
