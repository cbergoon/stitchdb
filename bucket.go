package main

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cbergoon/btree"
)

//Todo: Implement Writes
//Todo: Implement Indexes
//Todo: Implement Process in Load
//Todo: Finish Start Tx and Handle Tx
//Todo: juju/errors
//Todo: Finish Manager; invalidate, expire, callbacks
//Todo: Implement Log Compaction

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
	entries := make([]string, 0)
	r := bufio.NewReader(b.file)
	var err error
	var iline []byte
	for {
		for i := 0; i < 128; i++ {
			iline, err = r.ReadBytes('\n')
			if err == io.EOF && len(iline) <= 0 {
				break //Read is complete
			} else if err != nil {
				return errors.New("error: failed to read bucket file")
			}
			var size int = 0
			size, err = strconv.Atoi(strings.TrimSpace(string(iline)))
			if err != nil {
				return errors.New("error: bucket file data is corrupt; missing or unusable entry length")
			}
			if size > 0 {
				entry := make([]byte, size)
				var readlen int = 0
				readlen, err = io.ReadFull(r, entry)
				if err == io.ErrUnexpectedEOF || err == io.EOF {
					break
				} else if err != nil {
					return errors.New("error: failed to read bucket file")
				}
				if readlen != size {
					return errors.New("error: bucket file data is corrupt; entry length is invalid")
				}
				entries = append(entries, string(entry))
			}
		}

		for _, e := range entries {
			stype, sparts, err := parseEntryStmtTypeName(e)
			if err != nil {
				return errors.New("error: failed to parse statement")
			}
			if stype == "INSERT" {
				nentry, err := NewEntryFromStmt(sparts)
				if err != nil {
					return errors.New("error: failed to parse statement")
				}
				b.insert(nentry)
			} else if stype == "DELETE" {
				nentry, err := NewEntryFromStmt(sparts)
				if err != nil {
					return errors.New("error: failed to parse statement")
				}
				b.delete(nentry)
			}
		}

		entries = nil

		if err == io.EOF {
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

func parseEntryStmtTypeName(stmt string) (string, []string, error) {
	parts := strings.Split(stmt, "~")
	if parts[0] == "INSERT" || parts[0] == "DELETE" {
		return strings.TrimSpace(parts[0]), parts, nil
	} else {
		return "", nil, errors.New("error: invalid or unrecognized statement")
	}
}

//Called from tx which has a RW lock
func (b *Bucket) WriteAOFBuf() error {
	if b.db.config.persist {
		if b.db.config.writeFreq == MNGFREQ {
			if len(b.aofbuf) > 0 {
				written, err := b.file.Write(b.aofbuf)
				if err != nil || written != len(b.aofbuf) {
					return errors.New("error: failed to write bucket file")
				}
				if b.db.config.writeFreq == EACH {
					err := b.file.Sync()
					if err != nil {
						errors.New("error: failed to sync file")
					}
				}
				b.aofbuf = nil
			}
		}
	}
	return nil
}

func (b *Bucket) WriteDeleteEntry(e *Entry) {
	stmt := e.EntryDeleteStmt()
	b.aofbuf = append(b.aofbuf, stmt...)
}

func (b *Bucket) WriteInsertEntry(e *Entry) {
	stmt := e.EntryInsertStmt()
	b.aofbuf = append(b.aofbuf, stmt...)
}

func (b *Bucket) OpenBucket(file string) error {
	b.lock(MODE_READ_WRITE)
	defer b.unlock(MODE_READ_WRITE)
	b.open = true
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
			b.aofbuf = nil
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
	if b.db == nil || !b.db.open || b == nil || !b.open {
		return nil, errors.New("error: resource is not open")
	}
	tx, err := NewTx(b.db, b, mode)
	if err != nil {
		return nil, errors.New("error: failed to create transaction")
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
		return err
	}
	if tx.mode == MODE_READ_WRITE {
		err := tx.CommitTx()
		return err
	} else if mode == MODE_READ {
		err := tx.RollbackTx()
		return err
	} else {
		err := tx.RollbackTx()
		return err
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
				b.lock(MODE_READ_WRITE)
				if len(b.aofbuf) > 0 {
					b.file.Write(b.aofbuf)
					b.aofbuf = nil
					if b.db.config.syncFreq == EACH {
						b.file.Sync()
					}
				}
				b.unlock(MODE_READ_WRITE)
			}
			if b.db.config.syncFreq == MNGFREQ {
				b.lock(MODE_READ_WRITE)
				b.file.Sync()
				b.unlock(MODE_READ_WRITE)
			}
		}

		//Todo: Remove expires
		//Todo: Invalidate invalid
		//Todo: Future: Add geo location call backs
	}
	return nil
}

func (b *Bucket) needCompactLog() error {
	//Todo: Implement
	return nil
}

func (b *Bucket) compactLog() error {
	//Todo: Implement
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
		return nil, errors.New("error: failed to parse statement")
	}
	return NewBucket(db, opts, stmtParts[0])
}
