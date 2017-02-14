package main

import (
	"os"
	"sync"
	"time"

	"fmt"

	"github.com/cbergoon/btree"
)

type Bucket struct {
	name         string
	db           *StitchDB
	bktlock      sync.RWMutex
	data         *btree.BTree
	eviction     *btree.BTree
	invalidation *btree.BTree
	indexes      map[string]*Index
	file         *os.File
	open         bool
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
	b.lock(MODE_READ_WRITE)
	defer b.unlock(MODE_READ_WRITE)
	if b.db.config.persist {
		var err error
		fmt.Println(file)
		b.file, err = os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			//Todo: error
		}
		//Todo: Populate bucket, eviction and, invalidation
	}
	return nil
}

func (b *Bucket) Close() error {
	b.lock(MODE_READ_WRITE)
	defer b.unlock(MODE_READ_WRITE)
	if b.db.config.persist {
		if len(b.aofbuf) > 0 {
			_, err := b.file.Write(b.aofbuf)
			if err != nil {
				//Todo: error
			}
		}
		if err := b.file.Sync(); err != nil {
			//Todo: error
		}
		b.file.Close()
	}
	b.open = false
	b.aofbuf, b.data, b.eviction, b.invalidation, b.indexes = nil, nil, nil, nil, nil
	return nil
}

func (b *Bucket) get(key *Entry) *Entry {
	if e := b.data.Get(key); e != nil {
		return e.(*Entry)
	}
	return nil
}

func (b *Bucket) insert(entry *Entry) *Entry {
	var pentry *Entry = nil
	fmt.Println(b)
	fmt.Println(b.data)
	if p := b.data.ReplaceOrInsert(entry); p != nil {
		pentry = p.(*Entry)
	}
	if pentry != nil {
		if pentry.opts.doesExp {
			b.eviction.Delete(pentry)
		}
		if pentry.opts.doesInv {
			b.invalidation.Delete(pentry)
		}
		//Todo: Iterate through indexes delete pentry
	}
	if entry.opts.doesExp {
		b.eviction.ReplaceOrInsert(entry)
	}
	if entry.opts.doesInv {
		b.invalidation.ReplaceOrInsert(entry)
	}
	//Todo: Iterate through indexes insert pentry
	return pentry
}

func (b *Bucket) delete(key *Entry) *Entry {
	var pentry *Entry
	if p := b.data.Delete(key); p != nil {
		pentry = p.(*Entry)
	}
	if pentry != nil {
		if pentry.opts.doesExp {
			b.eviction.Delete(pentry)
		}
		if pentry.opts.doesInv {
			b.invalidation.Delete(pentry)
		}
		//Todo: Iterate through indexes delete pentry
	}
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
	mngct := time.NewTicker(b.db.config.manageFrequency)
	defer mngct.Stop()
	for range mngct.C {
		if !b.db.open {
			break
		}
		if b.db.config.persist {
			if b.db.config.writeFreq == MNGFREQ {
				if len(b.aofbuf) > 0 {
					b.file.Write(b.aofbuf)
					if b.db.config.syncFreq == EACH {
						b.file.Sync()
					}
				}
			}
			if b.db.config.syncFreq == MNGFREQ {
				b.file.Sync()
			}
		}
		//Remove expires
		//invalidate invalid
		//future geo location call backs
	}
	return nil
}

func (b *Bucket) lock(mode RWMode) {
	if mode == MODE_READ {
		b.bktlock.RLock()
	} else if mode == MODE_READ_WRITE {
		b.bktlock.Lock()
	}
}

func (b *Bucket) unlock(mode RWMode) {
	if mode == MODE_READ {
		b.bktlock.RUnlock()
	} else if mode == MODE_READ_WRITE {
		b.bktlock.Unlock()
	}
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
