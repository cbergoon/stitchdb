package main

import (
	"os"
	"sync"
	"time"

	//Allow both implementations
	"github.com/cbergoon/btree"
)

type Bucket struct {
	name         string
	db           *StitchDB
	lock         sync.RWMutex
	data         *btree.BTree
	eviction     *btree.BTree
	invalidation *btree.BTree
	indexes      map[string]*Index
	file         *os.File
	options      *BucketOptions
	aofbuf       []byte
}

type eItype struct {
	db *StitchDB
}

type iItype struct {
	db *StitchDB
}

func NewBucket(db *StitchDB, bucketOptions *BucketOptions, name string) (*Bucket, error) {
	return &Bucket{
		name:         name,
		db:           db,
		options:      bucketOptions,
		data:         btree.New(bucketOptions.btdeg, nil),
		eviction:     btree.New(bucketOptions.btdeg, &eItype{db: db}),
		invalidation: btree.New(bucketOptions.btdeg, &iItype{db: db}),
		indexes:      make(map[string]*Index),
	}, nil
}

func (b *Bucket) OpenBucket(file string) error {
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
	mngct := time.NewTicker(b.db.config.ManageFrequency)
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

func (b *Bucket) bucketCreateStmt() []byte {
	var cbuf []byte
	cbuf = append(cbuf, "CREATE"...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.name...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.options.bucketOptionsCreateStmt()...)
	return append(cbuf, '\n')
}

func (b *Bucket) bucketDropStmt() []byte {
	var cbuf []byte
	cbuf = append(cbuf, "DROP"...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.name...)
	return append(cbuf, '\n')
}

func NewBucketFromStmt(db *StitchDB, stmtParts []string) (*Bucket, error) {
	opts, err := NewBucketOptionsFromStmt(stmtParts)
	if err != nil {
		//Todo: error here
	}
	return NewBucket(db, opts, stmtParts[0])
}

type Index struct {
	t *btree.BTree
}

//Add insert, delete
