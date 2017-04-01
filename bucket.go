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

//Todo: Replace with the StitchDB config value.

//COMPACT_FACTOR is the Multiplier factor for to determine when to compact log.
const COMPACT_FACTOR int = 10

//Bucket represents a bucket in the database. Think 'table' but for key-value store.
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

//NewBucket creates a new bucket for the specified db with the provided options.
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

//loadBucketFile reads the entire bucket file and inserts the entries into the bucket. Populates the main, invalidation,
//expiration, and index trees.
func (b *Bucket) loadBucketFile() error {
	entries := make([]string, 0)
	r := bufio.NewReader(b.file)
	var err error
	var iline []byte
	for {
		for i := 0; i < 1024; i++ {
			iline, err = r.ReadBytes('\n')
			if err == io.EOF && len(iline) <= 0 {
				break //Read is complete
			} else if err != nil {
				return errors.Annotate(err, "error: bucket: failed to read bucket file")
			}
			var size int
			size, err = strconv.Atoi(strings.TrimSpace(string(iline)))
			if err != nil {
				return errors.Annotate(err, "error: bucket: bucket file data is corrupt; missing or unusable entry length")
			}
			if size > 0 {
				entry := make([]byte, size)
				var readlen int
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

//parseEntryStmtTypeName returns the entry name and slice of the remaining parts of the tree.
func parseEntryStmtTypeName(stmt string) (string, []string, error) {
	parts := strings.Split(stmt, "~")
	if parts[0] == "INSERT" || parts[0] == "DELETE" {
		return strings.TrimSpace(parts[0]), parts, nil
	}
	return "", nil, errors.New("error: bucket: invalid or unrecognized statement")
}

//writeAOFBuf writes the db file buffer to disk performing a file sync when if required.
//Called from tx which has a RW lock.
func (b *Bucket) writeAOFBuf() error {
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

//writeDeleteEntry generates and appends an insert entry to the write buffer.
func (b *Bucket) writeDeleteEntry(e *Entry) {
	stmt := e.EntryDeleteStmt()
	b.aofbuf = append(b.aofbuf, stmt...)
}

//writeInsertEntry generates and appends a delete entry to the write buffer.
func (b *Bucket) writeInsertEntry(e *Entry) {
	stmt := e.EntryInsertStmt()
	b.aofbuf = append(b.aofbuf, stmt...)
}

//OpenBucket opens and loads a bucket. It is expected that the manager is started by the caller of this function after
//bucket is open. Returns an error if the file could not be opened or created or if the bucket file could not be loaded.
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

//Close closes the bucket flushing the write buffer to disk. Performs a sync regardless of frequency setting as it is not
//guarenteed that the manager will execute again before exiting. Returns an error if the write to the bucket file failed,
//the file sync failed, or if the file fails to close.
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

//indexExists returns true if the index exists for the provided index name.
func (b *Bucket) indexExists(index string) bool {
	idx, ok := b.indexes[index]
	if ok && idx != nil {
		return true
	}
	return false
}

//get retrieves an entry from the data tree using the default (key) comparator for the entry. It is assumed the the caller
//obtains a lock on the db.
func (b *Bucket) get(key *Entry) *Entry {
	if e := b.data.Get(key); e != nil {
		return e.(*Entry)
	}
	return nil
}

//insert adds an entry to the bucket populating the expires, invalidation, and index trees. It is assumed the the caller
//obtains a lock on the db.
func (b *Bucket) insert(entry *Entry) *Entry {
	var pentry *Entry
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

//delete removes an entry from the bucket if it exists removing from the expires, invalidation, and all index trees. It
//is assumed the the caller obtains a lock on the db.
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

//startTx returns a new transaction with the specified RW mode and obtains the lock on the bucket. Returns an error if
//the db or bucket is closed or if the transactions fails to be created.
func (b *Bucket) startTx(mode RWMode) (*Tx, error) {
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

//handleTx executes the provided function against the transaction. The transaction will be committed if and only if the
//transaction is a Read/Write transaction and the provided function returns a nil error otherwise the transaction will be
//rolled back.
func (b *Bucket) handleTx(mode RWMode, f func(t *Tx) error) error {
	tx, err := b.startTx(mode)
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
}

//manager is the main execution loop for the bucket. The manager loop is executed at the specified frequency of the database.
func (b *Bucket) manager() error {
	if b == nil || b.db == nil || b.db.config == nil {
		return nil
	}
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

//compactLog rewrites the log resulting in a condensed form containing only insert/update statements.
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

//lock is a helper function to obtain a lock on the bucket appropriately based on the provided RW modifier.
func (b *Bucket) lock(mode RWMode) {
	if mode == MODE_READ {
		b.bktlock.RLock()
	} else if mode == MODE_READ_WRITE {
		b.bktlock.Lock()
	}
}

//unlock is a helper function to release the lock on the bucket appropriately based on the provided RW modifier.
func (b *Bucket) unlock(mode RWMode) {
	if mode == MODE_READ {
		b.bktlock.RUnlock()
	} else if mode == MODE_READ_WRITE {
		b.bktlock.Unlock()
	}
}

//bucketCreateStmt builds the statement representing the provided entry.
func (b *Bucket) bucketCreateStmt() []byte {
	var cbuf []byte
	cbuf = append(cbuf, "CREATE"...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.name...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.options.bucketOptionsCreateStmt()...)
	return append(cbuf, '\n')
}

//bucketDropStmt builds the statement representing the provided entry.
func (b *Bucket) bucketDropStmt() []byte {
	var cbuf []byte
	cbuf = append(cbuf, "DROP"...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, b.name...)
	return append(cbuf, '\n')
}

//NewBucketFromStmt creates a bucket for a given bucket statement.
func NewBucketFromStmt(db *StitchDB, stmtParts []string) (*Bucket, error) {
	opts, err := NewBucketOptionsFromStmt(stmtParts)
	if err != nil {
		return nil, errors.Annotate(err, "error: bucket: failed to parse statement")
	}
	return NewBucket(db, opts, stmtParts[0])
}
