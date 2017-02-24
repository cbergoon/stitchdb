package main

import (
	"os"
	"sync"
	"time"
	"bufio"
	"io"

	"github.com/cbergoon/btree"
	"github.com/pkg/errors"
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

func (b *Bucket) loadBucketFile() error {
	lines := make([]string, 0)
	r := bufio.NewReader(b.file)
	var err error
	var line []byte
	for {
		for i := 0; i < 128; i++ {
			line, err = r.ReadBytes('\n')
			if err == io.EOF && len(line) <= 0 {
				break //Read is complete
			} else if err != nil {
				return errors.New("error: failed to read file")
			}
			lines = append(lines, string(line))
		}
		//Todo: Process to record bruh.
		//Todo: Populate bucket, eviction and, invalidation
		if err == io.EOF && len(line) <= 0 {
			break //Read is complete
		} else if err != nil {
			return errors.New("error: failed to read file")
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (b *Bucket) OpenBucket(file string) error {
	b.lock(MODE_READ_WRITE)
	defer b.unlock(MODE_READ_WRITE)
	if b.db.config.persist {
		var err error
		b.file, err = os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return errors.New("error: failed to open bucket file")
		}
		err = b.loadBucketFile()
		if err != nil {
			return errors.New("error failed to load from file")
		}
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
				return errors.New("error: failed to write to bucket")
			}
		}
		if err := b.file.Sync(); err != nil {
			return errors.New("error: failed to sync bucket file")
		}
		b.open = false
		b.aofbuf, b.data, b.eviction, b.invalidation, b.indexes = nil, nil, nil, nil, nil
		err := b.file.Close()
		if err != nil {
			return errors.New("error: failed to close bucket file")
		}
	}
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

func (b *Bucket) StartTx(mode RWMode) (*Tx, error) {
	//if db is not open close
	if !b.db.open {
		//Todo: Error
	}
	//create new tx
	tx, err := NewTx(b.db, b, mode)
	if err != nil {
		//Todo: Error
	}
	tx.lock()
	return tx, nil
}

func (b *Bucket) handleTx(mode RWMode, f func(t *Tx) error) error {
	tx, err := b.StartTx(mode)
	if err != nil {
		return err
	}
	err = f(tx)
	if err != nil {
		err := tx.RollbackTx()
		return err //May need to check
	}
	if tx.mode == MODE_READ_WRITE {
		err := tx.CommitTx()
		return err //May need to check
	} else if mode == MODE_READ {
		err := tx.RollbackTx()
		return err //May need to check
	} else {
		err := tx.RollbackTx()
		return err //May need to check
	}
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

func (b *Bucket) needCompactLog() error {
	return nil
}

func (b *Bucket) compactLog() error {
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
