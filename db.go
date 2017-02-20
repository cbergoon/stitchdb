package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	//"github.com/pkg/errors"
)

const (
	BUCKET_CONFIG_FILE    string = "sbkt.conf"
	BUCKET_FILE_EXTENSION string = ".stitch"
)

type StitchDB struct {
	config    *Config
	dblock    sync.RWMutex
	open      bool
	buckets   map[string]*Bucket
	system    *Bucket
	bktcfgf   *os.File
	bktcfgfrc int
}

func NewStitchDB(config *Config) (*StitchDB, error) {
	stitch := &StitchDB{
		config:  config,
		buckets: make(map[string]*Bucket),
	}
	sysbktopts, err := NewBucketOptions(BTreeDegree(32)) //Todo: set appropriate bucket options for sys
	if err != nil {
		//Todo: Error
		return nil, errors.New("error: failed to create system bucket options")
	}
	sysbkt, err := NewBucket(stitch, sysbktopts, "_sys")
	if err != nil {
		//Todo: Error
		return nil, errors.New("error: failed to create system bucket")
	}
	stitch.system = sysbkt
	return stitch, nil
}

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
	// check for errors
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	stmtMap := make(map[string][]string)
	for _, line := range lines {
		name, detail, err := parseStmtTypeName(line)
		if err != nil {
			//Todo: Error
			return nil, errors.New("error: failed to ...")
		}
		stmtMap[name] = detail
	}
	return stmtMap, nil
}

func parseStmtTypeName(stmt string) (string, []string, error) {
	parts := strings.Split(stmt, ":")
	if len(parts) == 7 && parts[0] == "CREATE" {
		return strings.TrimSpace(parts[1]), parts[1:], nil
	} else if len(parts) == 2 && parts[0] == "DROP" {
		return strings.TrimSpace(parts[1]), nil, nil
	} else {
		//Todo: Error invalid stmt
		return "", nil, nil
	}
}

func (db *StitchDB) getDBFilePath(fileName string) string {
	return strings.TrimSpace(db.config.dirPath) + strings.TrimSpace(fileName)
}

func (db *StitchDB) Open() error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if db.config.persist {
		err := os.MkdirAll(db.config.dirPath, os.ModePerm)
		if err != nil {
			//Todo: error
		}
		bktStmts, err := db.readConfigFileBuckets()
		if err != nil {
			//Todo: error could not read file
		}
		for bktName, bktStmtParts := range bktStmts {
			if bktStmtParts != nil && len(bktStmtParts) > 0 {
				bucket, err := NewBucketFromStmt(db, bktStmts[bktName])
				if err != nil {
					//Todo: error
				}
				db.buckets[bktName] = bucket
				db.buckets[bktName].OpenBucket(db.getDBFilePath(bktName + BUCKET_FILE_EXTENSION))
			}
		}
	}
	db.open = true
	go db.runManager()
	return nil
}

func (db *StitchDB) Close() error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if !db.open {
		//Todo: return error db is closed
	}
	for key := range db.buckets {
		err := db.buckets[key].Close()
		if err != nil {
			//Todo: log error
		}
		db.buckets[key] = nil
	}
	db.system.Close()
	if db.config.persist && db.bktcfgf != nil {
		err := db.bktcfgf.Sync()
		if err != nil {
			//Todo: log error
		}
		err = db.bktcfgf.Close()
		if err != nil {
			//Todo: log error
		}
	}
	db.open = false
	db.buckets = nil
	db.system = nil
	db.bktcfgf = nil
	// Pause for manageFrequency * 2 to allow bucket managers to exit gracefully.
	time.Sleep(db.config.manageFrequency * 2)
	return nil
}

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
					db.bktcfgf.Truncate(0)
					db.bktcfgf.Seek(0, 0)
					//Rewrite file
					for key := range db.buckets {
						stmt := db.buckets[key].bucketCreateStmt()
						db.bktcfgf.Write(stmt)
					}
					db.bktcfgfrc = len(db.buckets)
					if db.config.syncFreq == EACH {
						db.bktcfgf.Sync()
					}
				}
				if db.config.syncFreq == MNGFREQ {
					db.bktcfgf.Sync()
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

func (db *StitchDB) GetConfig() *Config {
	db.lock(MODE_READ)
	defer db.unlock(MODE_READ)
	return db.config
}

func (db *StitchDB) SetConfig(config *Config) {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	db.config = config
}

func (db *StitchDB) getBucket(name string) (*Bucket, error) {
	//db.lock(MODE_READ)
	//defer db.unlock(MODE_READ)
	var b *Bucket
	var ok bool
	bktName := strings.TrimSpace(name)
	if name == "_sys" {
		b = db.system
	} else {
		b, ok = db.buckets[bktName]
	}
	if !ok {
		//Todo: Error bucket does not exist
		return nil, errors.New("Bucket does not exist")
	}
	return b, nil
}

func (db *StitchDB) View(bucket string, f func(t *Tx) error) error {
	db.lock(MODE_READ)
	defer db.unlock(MODE_READ)
	if !db.open {
		//Todo: return error db is closed
	}
	b, err := db.getBucket(bucket)
	if b == nil || err != nil {
		//Todo: Error invalid bucket
	}
	err = b.handleTx(MODE_READ, f)
	return err
}

func (db *StitchDB) Update(bucket string, f func(t *Tx) error) error {
	db.lock(MODE_READ)
	defer db.unlock(MODE_READ)
	if !db.open {
		//Todo: return error db is closed
	}
	b, err := db.getBucket(bucket)
	if b == nil || err != nil {
		//Todo: Error invalid bucket
	}
	err = b.handleTx(MODE_READ_WRITE, f)
	return err
}

func (db *StitchDB) CreateBucket(name string, options *BucketOptions) error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if !db.open {
		//Todo: error
	}

	bkt, err := db.getBucket(name)
	if bkt != nil || err == nil {
		//Todo: error bucket already exists
	}
	fmt.Println("here")
	bktName := strings.TrimSpace(name)
	bktFilePath := db.getDBFilePath(bktName + BUCKET_FILE_EXTENSION)
	bucket, err := NewBucket(db, options, bktName)
	if err != nil {
		//Todo: error
	}

	db.buckets[bktName] = bucket
	db.buckets[bktName].OpenBucket(bktFilePath)

	if db.config.persist && db.bktcfgf != nil {
		stmt := bucket.bucketCreateStmt()
		db.bktcfgf.Write(stmt)
		db.bktcfgfrc++
		if db.config.syncFreq == EACH {
			db.bktcfgf.Sync()
		}
	}

	go db.buckets[bktName].manager()
	return nil
}

func (db *StitchDB) DropBucket(name string) error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if !db.open {
		//Todo: error
	}
	bktName := strings.TrimSpace(name)
	bucket, err := db.getBucket(bktName)
	if err != nil {
		//Todo: error
	}
	stmt := bucket.bucketDropStmt()
	bucket.Close()
	bucket = nil
	delete(db.buckets, bktName)

	if db.config.persist && db.bktcfgf != nil {
		db.bktcfgf.Write(stmt)
		db.bktcfgfrc++
		if db.config.syncFreq == EACH {
			db.bktcfgf.Sync()
		}
	}
	return nil
}

func (db *StitchDB) lock(mode RWMode) {
	if mode == MODE_READ {
		db.dblock.RLock()
	} else if mode == MODE_READ_WRITE {
		db.dblock.Lock()
	}
}

func (db *StitchDB) unlock(mode RWMode) {
	if mode == MODE_READ {
		db.dblock.RUnlock()
	} else if mode == MODE_READ_WRITE {
		db.dblock.Unlock()
	}
}
