// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cbergoon/btree"
	"github.com/dhconnelly/rtreego"
	"github.com/juju/errors"
	"github.com/tidwall/gjson"
)

const COMPACT_FACTOR int = 10

type Bucket struct {
	name         string            //Name of the bucket.
	db           *StitchDB         //Reference to containing DB.
	bktlock      sync.RWMutex      //Lock for bucket.
	data         *btree.BTree      //Primary tree for bucket.
	eviction     *btree.BTree      //Data for bucket ordered by eviction time.
	invalidation *btree.BTree      //Data for bucket ordered by invalidation time.
	rtree        *rtreego.Rtree    //Rtree of data for geolocation.
	indexes      map[string]*Index //Map of indexes built over data.
	file         *os.File          //Bucket Append Only File.
	rct          uint64            //AOF row count.
	open         bool              //Indicated the status of the bucket.
	options      *BucketOptions    //Options for the bucket.
	aofbuf       []byte            //AOF write buffer.
}

//eItype provides a basic context via type for tree iType.
type eItype struct {
	db *StitchDB
}

//iItype provides a basic context via type for tree iType.
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
		rtree:        rtreego.NewTree(bucketOptions.dims, bucketOptions.btdeg, bucketOptions.btdeg*2),
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
				return errors.Annotate(err, "error: bucket: failed to read bucket file")
			}
			var size int = 0
			size, err = strconv.Atoi(strings.TrimSpace(string(iline)))
			if err != nil {
				return errors.Annotate(err, "error: bucket: bucket file data is corrupt; missing or unusable entry length")
			}
			if size > 0 {
				entry := make([]byte, size)
				var readlen int = 0
				readlen, err = io.ReadFull(r, entry)
				if err == io.ErrUnexpectedEOF || err == io.EOF {
					break
				} else if err != nil {
					return errors.Annotate(err, "error: bucket: failed to read bucket file")
				}
				if readlen != size {
					return errors.Annotate(err, "error: bucket: bucket file data is corrupt; entry length is invalid")
				}
				entries = append(entries, string(entry))
			}
		}

		for _, e := range entries {
			stype, sparts, err := parseEntryStmtTypeName(e)
			if err != nil {
				return errors.Annotate(err, "error: bucket: failed to parse statement")
			}
			if stype == "INSERT" {
				nentry, err := NewEntryFromStmt(sparts)
				if err != nil {
					return errors.Annotate(err, "error: bucket: failed to parse statement")
				}
				b.insert(nentry)
			} else if stype == "DELETE" {
				nentry, err := NewEntryFromStmt(sparts)
				if err != nil {
					return errors.Annotate(err, "error: bucket: failed to parse statement")
				}
				b.delete(nentry)
			}
		}

		entries = nil

		if err == io.EOF {
			break //Read is complete
		} else if err != nil {
			return errors.Annotate(err, "error: bucket: failed to read file")
		}
	}

	//Rebuild Indexes
	for _, ind := range b.indexes {
		ind.rebuild()
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
		return "", nil, errors.New("error: bucket: invalid or unrecognized statement")
	}
}

//Called from tx which has a RW lock
func (b *Bucket) WriteAOFBuf() error {
	if b.db.config.persist {
		if b.db.config.writeFreq == MNGFREQ {
			if len(b.aofbuf) > 0 {
				written, err := b.file.Write(b.aofbuf)
				if err != nil {
					return errors.Annotate(err, "error: bucket: failed to write bucket file")
				}
				if written != len(b.aofbuf) {
					return errors.New("error: bucket: failed to write bucket file")
				}
				if b.db.config.writeFreq == EACH {
					err := b.file.Sync()
					if err != nil {
						errors.Annotate(err, "error: bucket: failed to sync file")
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
			return errors.Annotate(err, "error: bucket: failed to open bucket file")
		}
		err = b.loadBucketFile()
		if err != nil {
			return errors.Annotate(err, "error bucket: failed to load from file")
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
				return errors.Annotate(err, "error: bucket: failed to write to bucket")
			}
			b.aofbuf = nil
		}
		if err := b.file.Sync(); err != nil {
			return errors.Annotate(err, "error: bucket: failed to sync bucket file")
		}
		b.open = false
		b.aofbuf, b.data, b.eviction, b.invalidation, b.indexes = nil, nil, nil, nil, nil
		err := b.file.Close()
		if err != nil {
			return errors.Annotate(err, "error: bucket: failed to close bucket file")
		}
	}
	return nil
}

func (b *Bucket) indexExists(index string) bool {
	idx, ok := b.indexes[index]
	if ok && idx != nil {
		return true
	}
	return false
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
	b.rct++
	if pentry != nil {
		if pentry.opts.doesExp {
			b.eviction.Delete(pentry)
		}
		if pentry.opts.doesInv {
			b.invalidation.Delete(pentry)
		}
		//Iterate through indexes delete pentry
		for _, ind := range b.indexes {
			ind.delete(pentry)
		}
		//Delete from Rtree
		if b.options.geo {
			ljson := gjson.Get(pentry.v, "coords")
			if ljson.Exists() {
				b.rtree.DeleteWithComparator(pentry, GetEntryComparator())
			}
		}
	}
	if entry.opts.doesExp {
		b.eviction.ReplaceOrInsert(entry)
	}
	if entry.opts.doesInv {
		b.invalidation.ReplaceOrInsert(entry)
	}
	//Iterate through indexes insert entry
	for _, ind := range b.indexes {
		ind.insert(entry)
	}
	//Insert into Rtree
	if b.options.geo {
		ljson := gjson.Get(entry.v, "coords")
		if ljson.Exists() {
			b.rtree.Insert(entry)
		}
	}
	return pentry
}

func (b *Bucket) delete(key *Entry) *Entry {
	var pentry *Entry
	if p := b.data.Delete(key); p != nil {
		pentry = p.(*Entry)
	}
	b.rct++
	if pentry != nil {
		if pentry.opts.doesExp {
			b.eviction.Delete(pentry)
		}
		if pentry.opts.doesInv {
			b.invalidation.Delete(pentry)
		}
		//Iterate through indexes delete pentry
		for _, ind := range b.indexes {
			ind.delete(pentry)
		}
		//Delete from Rtree
		if b.options.geo {
			if b.options.geo {
				ljson := gjson.Get(pentry.v, "coords")
				if ljson.Exists() {
					b.rtree.DeleteWithComparator(pentry, GetEntryComparator())
				}
			}
		}
	}
	return nil
}

func (b *Bucket) StartTx(mode RWMode) (*Tx, error) {
	if b.db == nil || !b.db.open || b == nil || !b.open {
		return nil, errors.New("error: bucket: resource is not open")
	}
	tx, err := NewTx(b.db, b, mode)
	if err != nil {
		return nil, errors.Annotate(err, "error: bucket: failed to create transaction")
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
		b.lock(MODE_READ_WRITE)
		if !b.db.open {
			break
		}
		if b.db.config.persist {
			if b.db.config.writeFreq == MNGFREQ {
				if len(b.aofbuf) > 0 {
					_, err := b.file.Write(b.aofbuf)
					if err != nil {
						fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: bucket: failed to write to bucket file")))
					}
					b.aofbuf = nil
					if b.db.config.syncFreq == EACH {
						err := b.file.Sync()
						if err != nil {
							fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: bucket: failed to sync1 bucket file")))
						}
					} else if b.db.config.syncFreq == MNGFREQ {
						err := b.file.Sync()
						if err != nil {
							fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: bucket: failed to sync2 bucket file")))
						}
					}
				}
			}
			if b != nil && b.data != nil {
				if b.rct > uint64(b.data.Len()*COMPACT_FACTOR) {
					err := b.compactLog()
					if err != nil {
						fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: bucket: failed to compact bucket file")))
					}
				}
			}
		}

		if b != nil && b.data != nil {
			for i := 0; i < b.eviction.Len(); i++ {
				var eitem *Entry
				mitem := b.eviction.Min()
				if mitem != nil {
					eitem = mitem.(*Entry)
				}
				if eitem.IsExpired() {
					b.delete(eitem)
					//callback
				}
			}
		}

		if b != nil && b.data != nil {
			for i := 0; i < b.invalidation.Len(); i++ {
				var eitem *Entry
				mitem := b.invalidation.Min()
				if mitem != nil {
					eitem = mitem.(*Entry)
				}
				if eitem.IsInvalid() {
					eitem.invalid = true
					//callback
				}
			}
		}

		b.unlock(MODE_READ_WRITE)
	}
	return nil
}

func (b *Bucket) compactLog() error {
	//open new tmp file
	var err error
	tmpFile, err := os.OpenFile(b.db.getDBFilePath(b.name+BUCKET_TMP_FILE_EXTENSION), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return errors.Annotate(err, "error: bucket: failed to open temporary bucket file")
	}
	var buf []byte
	b.data.Ascend(func(item btree.Item) bool {
		eItem := item.(*Entry)
		return func(e *Entry) bool {
			buf = append(buf, e.EntryInsertStmt()...)
			if len(buf) > 1024*1024 {
				tmpFile.Write(buf)
				buf = nil
			}
			return true
		}(eItem)
	})
	err = tmpFile.Sync()
	if err != nil {
		return errors.Annotate(err, "error: bucket: failed to sync temporary bucket file")
	}
	err = b.file.Close()
	if err != nil {
		return errors.Annotate(err, "error: bucket: failed to close bucket file")
	}
	err = os.Remove(b.db.getDBFilePath(b.name + BUCKET_FILE_EXTENSION))
	if err != nil {
		return errors.Annotate(err, "error: bucket: failed to delete bucket file")
	}
	err = tmpFile.Close()
	if err != nil {
		return errors.Annotate(err, "error: bucket: failed to close temporary bucket file")
	}
	err = os.Rename(b.db.getDBFilePath(b.name+BUCKET_TMP_FILE_EXTENSION), b.db.getDBFilePath(b.name+BUCKET_FILE_EXTENSION))
	if err != nil {
		return errors.Annotate(err, "error: bucket: failed to rename bucket file")
	}
	b.file, err = os.OpenFile(b.db.getDBFilePath(b.name+BUCKET_FILE_EXTENSION), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return errors.Annotate(err, "error: bucket: failed to open bucket file")
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
		return nil, errors.Annotate(err, "error: bucket: failed to parse statement")
	}
	return NewBucket(db, opts, stmtParts[0])
}
