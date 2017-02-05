package main

import (
	"os"
	"sync"
	"time"

	//Allow both implementations
	"github.com/cbergoon/btree"
)

type Bucket struct {
	Name     string
	Db       *StitchDB
	Lock     sync.RWMutex
	Data     *btree.BTree
	Eviction *btree.BTree
	Indexes  map[string]*btree.BTree
	File     *os.File
	Options  *BucketOptions
}

func NewBucket(db *StitchDB, bucketOptions *BucketOptions) (*Bucket, error) {
	return nil, nil
}

func (b *Bucket) InstantiateBucket(bucket, file string) error {
	return nil
}

func (b *Bucket) Close() error {
	//call sync
	//close files
	//set open false
	//set all refs to nil
	return nil
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

func (b *Bucket) manager() error {
	mngct := time.NewTicker(b.Db.config.ManageFrequency)
	defer mngct.Stop()
	for range mngct.C {
		//if on "second" frequency write bucket file
		//for each bucket call bucket manager
		//Remove expires
		//if sync 1 second and persist file sync
		//future geo location call backs
	}
	return nil
}

//Add insert, delete
