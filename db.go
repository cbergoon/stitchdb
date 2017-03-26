// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
)

const (
	BUCKET_CONFIG_FILE        string = "sbkt.conf"   //Main DB AOF
	BUCKET_FILE_EXTENSION     string = ".stitch"     //Bucket AOF file extension
	BUCKET_TMP_FILE_EXTENSION string = ".stitch.tmp" //Bucket AOF file extension used when replacing file
)

//StitchDB represents the database object. All operations on the database originate from this object.
type StitchDB struct {
	config    *Config
	dblock    sync.RWMutex
	open      bool
	buckets   map[string]*Bucket
	system    *Bucket
	bktcfgf   *os.File
	bktcfgfrc int
}

//NewStitchDB returns a new StitchDB with the specified configuration. Note: this function only creates the representation
//of the DB and does not open or start the db.
func NewStitchDB(config *Config) (*StitchDB, error) {
	stitch := &StitchDB{
		config:  config,
		buckets: make(map[string]*Bucket),
	}
	sysbktopts, err := NewBucketOptions(BTreeDegree(32), System)
	if err != nil {
		return nil, errors.Annotate(err, "error: db: failed to create system bucket options")
	}
	sysbkt, err := NewBucket(stitch, sysbktopts, "_sys")
	if err != nil {
		return nil, errors.Annotate(err, "error: db: failed to create system bucket")
	}
	stitch.system = sysbkt
	return stitch, nil
}

//readConfigFileBuckets opens/creates/reads the BUCKET_CONFIG_FILE into a modified map representation of the file. Returns
//a map[string][]string with key equal to names of the buckets and values representing the remaining statement.
func (db *StitchDB) readConfigFileBuckets() (map[string][]string, error) {
	lines := make([]string, 0)
	var err error
	db.bktcfgf, err = os.OpenFile(db.getDBFilePath(BUCKET_CONFIG_FILE), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(db.bktcfgf)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		db.bktcfgfrc++
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	stmtMap := make(map[string][]string)
	for _, line := range lines {
		name, detail, err := parseStmtTypeName(line)
		if err != nil {
			return nil, errors.Annotate(err, "error: db: failed to parse bucket statement")
		}
		stmtMap[name] = detail
	}
	return stmtMap, nil
}

//parseStmtTypeName given a bucket statement returns the name of the bucket and a slice of strings containing the parts
//of the statement.
func parseStmtTypeName(stmt string) (string, []string, error) {
	parts := strings.Split(stmt, ":")
	if len(parts) == 8 && parts[0] == "CREATE" {
		return strings.TrimSpace(parts[1]), parts[1:], nil
	} else if len(parts) == 2 && parts[0] == "DROP" {
		return strings.TrimSpace(parts[1]), nil, nil
	} else {
		return "", nil, errors.New("error: db: invalid or unrecognized statement")
	}
}

//getDBFilePath builds the path to the StitchDB resource at the specified base directory.
func (db *StitchDB) getDBFilePath(fileName string) string {
	return strings.TrimSpace(db.config.dirPath) + strings.TrimSpace(fileName)
}

//Open initializes the db for use and starts the manager routine. Open opens/creates the main db append only file, parses
//the statements within, creates the buckets stored in the file, and opens each bucket. Returns an error if the process was
//not able to create the directory, failed to read the stitch db
func (db *StitchDB) Open() error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if db.config.persist {
		err := os.MkdirAll(db.config.dirPath, os.ModePerm)
		if err != nil {
			return errors.Annotate(err, "error: db: failed to create stitch directory")
		}
		bktStmts, err := db.readConfigFileBuckets()
		if err != nil {
			return errors.Annotate(err, "error: db: failed to read stitch file")
		}
		for bktName, bktStmtParts := range bktStmts {
			if bktStmtParts != nil && len(bktStmtParts) > 0 {
				bucket, err := NewBucketFromStmt(db, bktStmts[bktName])
				if err != nil {
					return errors.Annotate(err, "error: db: failed to create bucket from statement")
				}
				db.buckets[bktName] = bucket
				db.buckets[bktName].OpenBucket(db.getDBFilePath(bktName + BUCKET_FILE_EXTENSION))
				fmt.Println(db.getDBFilePath(bktName + BUCKET_FILE_EXTENSION))
			}
		}
	}
	db.open = true
	go db.runManager()
	return nil
}

//Close closes each bucket including system, flushes bucket config file, and closes the file. Waits until all bucket
//managers have exited.
func (db *StitchDB) Close() error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if !db.open {
		return errors.New("error: db: db is closed")
	}
	for key := range db.buckets {
		err := db.buckets[key].Close()
		if err != nil {
			return errors.Annotate(err, "error: db: failed to close bucket")
		}
		db.buckets[key] = nil
	}
	db.system.Close()
	if db.config.persist && db.bktcfgf != nil {
		err := db.bktcfgf.Sync()
		if err != nil {
			return errors.Annotate(err, "errors: db: failed to sync bucket config file")
		}
		err = db.bktcfgf.Close()
		if err != nil {
			return errors.Annotate(err, "errors: db: failed to close bucket config file")
		}
	}
	db.open = false
	db.buckets = nil
	db.system = nil
	db.bktcfgf = nil
	// Pause for manageFrequency * 2 to allow bucket managers to exit gracefully.
	//time.Sleep(db.config.manageFrequency * 2)
	return nil
}

//runManager is the main manager loop for the database manager. Writes entries to AOF, compaction, and file flushes.
//runManager also starts bucket managers for each bucket in the db.
func (db *StitchDB) runManager() error {
	go func() {
		mngct := time.NewTicker(db.config.manageFrequency)
		defer mngct.Stop()
		for range mngct.C {
			if !db.open {
				break
			}
			db.lock(MODE_READ_WRITE)
			if db.config.persist {
				if len(db.buckets)*db.config.bucketFileMultLimit > db.bktcfgfrc {
					//Clear file
					err := db.bktcfgf.Truncate(0)
					if err != nil {
						fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: db: failed to truncate bucket config file")))
						continue
					}
					_, err = db.bktcfgf.Seek(0, 0)
					if err != nil {
						fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: db: failed to seek to bucket config file")))
						continue
					}
					//Rewrite file
					for key := range db.buckets {
						stmt := db.buckets[key].bucketCreateStmt()
						_, err := db.bktcfgf.Write(stmt)
						if err != nil {
							fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: db: failed to write bucket config file")))
							continue
						}
					}
					db.bktcfgfrc = len(db.buckets)
					if db.config.syncFreq == EACH {
						err := db.bktcfgf.Sync()
						if err != nil {
							fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: db: failed to sync bucket config file")))
							continue
						}
					}
				}
				if db.config.syncFreq == MNGFREQ {
					err := db.bktcfgf.Sync()
					if err != nil {
						fmt.Println(errors.ErrorStack(errors.Annotate(err, "error: db: failed to sync bucket config file")))
						continue
					}
				}
			}
			db.unlock(MODE_READ_WRITE)
		}
	}()
	if db.system != nil {
		go db.system.manager()
	}
	for key := range db.buckets {
		go db.buckets[key].manager()
	}
	return nil
}

//GetConfig returns a the configuration for the db.
func (db *StitchDB) GetConfig() *Config {
	db.lock(MODE_READ)
	defer db.unlock(MODE_READ)
	return db.config
}

//SetConfig sets the configuration for the db.
func (db *StitchDB) SetConfig(config *Config) {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	db.config = config
}

//getBucket returns the bucket with the provided name. Returns an error if the bucket name is invalid.
func (db *StitchDB) getBucket(name string) (*Bucket, error) {
	var b *Bucket
	var ok bool
	bktName := strings.TrimSpace(name)
	if name == "_sys" {
		b = db.system
	} else {
		b, ok = db.buckets[bktName]
	}
	if !ok {
		return nil, errors.New("error: db: invalid bucket")
	}
	return b, nil
}

//View creates a read only transaction and passes the open transaction to the provided function. The created transaction
//will provide read only access to the bucket specified by the bucket name provided. Returns an error if the db is closed
//or the bucket is invalid.
func (db *StitchDB) View(bucket string, f func(t *Tx) error) error {
	db.lock(MODE_READ)
	defer db.unlock(MODE_READ)
	if !db.open {
		return errors.New("error: db: db is closed")
	}
	b, err := db.getBucket(bucket)
	if err != nil {
		return errors.Annotate(err, "error: db: invalid bucket")
	}
	if b == nil {
		return errors.New("error: db: invalid bucket")
	}
	err = b.handleTx(MODE_READ, f)
	return err
}

//Update creates a read only transaction and passes the open transaction to the provided function. The created transaction
//will provide read/write access to the bucket specified by the bucket name provided. Returns an error if the db is closed
//or the bucket is invalid.
func (db *StitchDB) Update(bucket string, f func(t *Tx) error) error {
	db.lock(MODE_READ)
	defer db.unlock(MODE_READ)
	if !db.open {
		return errors.New("error: db: db is closed")
	}
	b, err := db.getBucket(bucket)
	if err != nil {
		return errors.Annotate(err, "error: db: invalid bucket")
	}
	if b == nil {
		return errors.New("error: db: invalid bucket")
	}
	err = b.handleTx(MODE_READ_WRITE, f)
	return err
}

//CreateBucket creates and opens a new bucket.
func (db *StitchDB) CreateBucket(name string, options *BucketOptions) error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if !db.open {
		return errors.New("error: db: db is closed")
	}

	bkt, err := db.getBucket(name)
	if err == nil {
		return errors.Annotate(err, "error: db: bucket already exists")
	}
	if bkt != nil {
		return errors.New("error: db: bucket already exists")
	}
	bktName := strings.TrimSpace(name)
	bktFilePath := db.getDBFilePath(bktName + BUCKET_FILE_EXTENSION)
	bucket, err := NewBucket(db, options, bktName)
	if err != nil {
		return errors.Annotate(err, "error: db: failed to create bucket")
	}

	db.buckets[bktName] = bucket
	err = db.buckets[bktName].OpenBucket(bktFilePath)
	if err != nil {
		return errors.Annotate(err, "error: db: failed to open bucket")
	}

	if db.config.persist && db.bktcfgf != nil {
		stmt := bucket.bucketCreateStmt()
		_, err := db.bktcfgf.Write(stmt)
		if err != nil {
			return errors.Annotate(err, "errors: db: failed to write to bucket config file")
		}
		db.bktcfgfrc++
		if db.config.syncFreq == EACH {
			err := db.bktcfgf.Sync()
			if err != nil {
				return errors.Annotate(err, "errors: db: failed to write bucket config file")
			}
		}
	}

	go db.buckets[bktName].manager()
	return nil
}

//DropBucket closes bucket and removes the bucket from the db.
func (db *StitchDB) DropBucket(name string) error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if !db.open {
		return errors.New("error: db: db is closed")
	}
	bktName := strings.TrimSpace(name)
	bucket, err := db.getBucket(bktName)
	if err != nil {
		return errors.Annotate(err, "error: db: invalid bucket")
	}
	stmt := bucket.bucketDropStmt()
	bucket.Close()
	bucket = nil
	delete(db.buckets, bktName)

	if db.config.persist && db.bktcfgf != nil {
		_, err := db.bktcfgf.Write(stmt)
		if err != nil {
			return errors.Annotate(err, "errors: db: failed to write bucket config file")
		}
		db.bktcfgfrc++
		if db.config.syncFreq == EACH {
			err := db.bktcfgf.Sync()
			if err != nil {
				return errors.Annotate(err, "errors: db: failed to sync bucket config file")
			}
		}
	}
	return nil
}

//lock is a helper function to obtain a lock on the db appropriately based on the RW modifier of the transaction.
func (db *StitchDB) lock(mode RWMode) {
	if mode == MODE_READ {
		db.dblock.RLock()
	} else if mode == MODE_READ_WRITE {
		db.dblock.Lock()
	}
}

//unlock is a helper function to release the lock on the db appropriately based on the RW modifier of the transaction.
func (db *StitchDB) unlock(mode RWMode) {
	if mode == MODE_READ {
		db.dblock.RUnlock()
	} else if mode == MODE_READ_WRITE {
		db.dblock.Unlock()
	}
}
