package main

import (
	"os"
	"sync"
	"time"

	//Allow both implementations
	"github.com/cbergoon/btree"
)

type Bucket struct {
	Name         string
	Db           *StitchDB
	Lock         sync.RWMutex
	Data         *btree.BTree
	Eviction     *btree.BTree
	Invalidation *btree.BTree
	Indexes      map[string]*Index
	File         *os.File
	Options      *BucketOptions
	aofbuf       []byte
}

type eItype struct {
	db *StitchDB
}

type iItype struct {
	db *StitchDB
}

type Index struct {
}

func NewBucket(db *StitchDB, bucketOptions *BucketOptions, name string) (*Bucket, error) {
	return &Bucket{
		Name:         name,
		Db:           db,
		Options:      bucketOptions,
		Data:         btree.New(bucketOptions.btdeg, nil),
		Eviction:     btree.New(bucketOptions.btdeg, &eItype{db: db}),
		Invalidation: btree.New(bucketOptions.btdeg, &iItype{db: db}),
		Indexes:      make(map[string]*Index),
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

func (b *Bucket) bucketCreateStmt() []byte {
	var cbuf []byte
	cbuf = append(cbuf, "CREATE"...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.Name...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.Options.bucketOptionsCreateStmt()...)
	return append(cbuf, '\n')
}

func (b *Bucket) bucketDropStmt() []byte {
	var cbuf []byte
	cbuf = append(cbuf, "DROP"...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.Name...)
	return append(cbuf, '\n')
}

func NewBucketFromStmt(db *StitchDB, stmtParts []string) (*Bucket, error) {
	opts, err := NewBucketOptionsFromStmt(stmtParts)
	if err != nil {
		//Todo: error here
	}
	return NewBucket(db, opts, stmtParts[0])
}

//Add insert, delete
