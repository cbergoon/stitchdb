package main

import (
	"bufio"
	"errors"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	BUCKET_CONFIG_FILE    string = "sbkt.conf"
	BUCKET_FILE_EXTENSION string = ".stitch"
)

type StitchDB struct {
	config       *Config
	dblock       sync.RWMutex
	open         bool
	buckets      map[string]*Bucket
	system       *Bucket
	bktcfgfile   *os.File
	bktcfgfilerc int
}

func NewStitchDB(config *Config) (*StitchDB, error) {
	stitch := &StitchDB{
		config:  config,
		buckets: make(map[string]*Bucket),
	}
	sysbktopts, err := NewBucketOptions() //Todo: set appropriate bucket options for sys
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

func (db *StitchDB) readConfigFileBuckets() ([]string, error) {
	db.lock(MODE_READ)
	defer db.unlock(MODE_READ)
	lines := make([]string, 0)
	var err error
	db.bktcfgfile, err = os.OpenFile(db.getDBFilePath(BUCKET_CONFIG_FILE), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(db.bktcfgfile)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		db.bktcfgfilerc++
	}
	// check for errors
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func (db *StitchDB) getDBFilePath(fileName string) string {
	return strings.TrimSpace(db.config.DirPath) + strings.TrimSpace(fileName)
}

func (db *StitchDB) Open() error {
	db.lock(MODE_READ_WRITE)
	defer db.unlock(MODE_READ_WRITE)
	if db.config.Persist {
		bkts, err := db.readConfigFileBuckets()
		if err != nil {
			//Todo: error could not read file
		}
		for _, bkt := range bkts {
			//Read and load bucket
			bktName := strings.TrimSpace(bkt)
			bktFilePath := db.getDBFilePath(bktName + BUCKET_FILE_EXTENSION)
			opts, err := NewBucketOptions()
			if err != nil {
				//Todo: error
			}
			bucket, err := NewBucket(db, opts, bktName)
			if err != nil {
				//Todo: error
			}
			db.buckets[bktName] = bucket
			db.buckets[bktName].OpenBucket(bktName, bktFilePath)
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
	if db.config.Persist && db.bktcfgfile != nil {
		err := db.bktcfgfile.Sync()
		if err != nil {
			//Todo: log error
		}
		err = db.bktcfgfile.Close()
		if err != nil {
			//Todo: log error
		}
	}
	db.open = false
	db.buckets = nil
	db.system = nil
	db.bktcfgfile = nil
	// Pause for ManageFrequency * 2 to allow bucket managers to exit gracefully.
	time.Sleep(db.config.ManageFrequency * 2)
	return nil
}

func (db *StitchDB) runManager() error {
	go func() {
		mngct := time.NewTicker(db.config.ManageFrequency)
		defer mngct.Stop()
		for range mngct.C {
			if !db.open {
				break
			}
			db.lock(MODE_READ_WRITE)
			if db.config.Persist {
				if len(db.buckets)*10 > db.bktcfgfilerc {
					//Clear File
					db.bktcfgfile.Truncate(0)
					db.bktcfgfile.Seek(0, 0)
					//Rewrite File
					for key := range db.buckets {
						stmt := db.buckets[key].bucketCreateStmt()
						db.bktcfgfile.Write(stmt)
					}
					db.bktcfgfilerc = len(db.buckets)
					if db.config.SyncFreq == EACH {
						db.bktcfgfile.Sync()
					}
				}
				if db.config.SyncFreq == MNGFREQ {
					db.bktcfgfile.Sync()
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
	db.lock(MODE_READ)
	defer db.unlock(MODE_READ)
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
	bktName := strings.TrimSpace(name)
	bktFilePath := db.getDBFilePath(bktName + BUCKET_FILE_EXTENSION)
	bucket, err := NewBucket(db, options, bktName)
	if err != nil {
		//Todo: error
	}
	db.buckets[bktName] = bucket
	db.buckets[bktName].OpenBucket(bktName, bktFilePath)

	if db.config.Persist && db.bktcfgfile != nil {
		stmt := bucket.bucketCreateStmt()
		db.bktcfgfile.Write(stmt)
		db.bktcfgfilerc++
		if db.config.SyncFreq == EACH {
			bucket.File.Sync()
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

	if db.config.Persist && db.bktcfgfile != nil {
		db.bktcfgfile.Write(stmt)
		db.bktcfgfilerc++
		if db.config.SyncFreq == EACH {
			bucket.File.Sync()
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
