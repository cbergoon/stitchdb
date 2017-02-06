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

type DBContext struct {
	bucketAdd    []string
	bucketRemove []string
}

type StitchDB struct {
	config     *Config
	dblock     sync.RWMutex
	open       bool
	buckets    map[string]*Bucket
	system     *Bucket
	filelock   sync.Mutex
	bktcfgfile *os.File
	context    *DBContext
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
	sysbkt, err := NewBucket(stitch, sysbktopts)
	if err != nil {
		//Todo: Error
		return nil, errors.New("error: failed to create system bucket")
	}
	stitch.system = sysbkt
	return stitch, nil
}

func (db *StitchDB) readConfigFileBuckets() ([]string, error) {
	lines := make([]string, 0)
	var err error
	db.bktcfgfile, err = os.OpenFile(db.getDBFilePath(BUCKET_CONFIG_FILE), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(db.bktcfgfile)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
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
			bucket, err := NewBucket(db, opts)
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
	if !db.open {
		//Todo: return error db is closed
	}
	db.lock(MODE_READ_WRITE)
	for key := range db.buckets {
		err := db.buckets[key].Close()
		if err != nil {
			//Todo: log error
		}
		db.buckets[key] = nil
	}
	db.system.Close()
	if db.config.Persist {
		if db.bktcfgfile != nil {
			err := db.bktcfgfile.Sync()
			if err != nil {
				//Todo: log error
			}
			err = db.bktcfgfile.Close()
			if err != nil {
				//Todo: log error
			}
		}
	}
	db.open = false
	db.buckets = nil
	db.system = nil
	db.bktcfgfile = nil
	// Pause for ManageFrequency * 2 to allow bucket managers to exit gracefully.
	time.Sleep(db.config.ManageFrequency * 2)
	db.unlock(MODE_READ_WRITE)
	return nil
}

func (db *StitchDB) persistBucketConfig() error {
	//Rewrite file since it will be very short
	//call sync/flush
	return nil
}

func (db *StitchDB) runManager() error {
	go func() {
		mngct := time.NewTicker(db.config.ManageFrequency)
		defer mngct.Stop()
		for range mngct.C {
			db.lock(MODE_READ_WRITE)
			if db.config.Persist && db.config.Sync == SECOND {
				if len(db.context.bucketAdd) > 0 || len(db.context.bucketRemove) > 0 {
					db.persistBucketConfig()
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
	if !db.open {
		//Todo: return error db is closed
	}
	b, err := db.getBucket(bucket)
	if b == nil || err != nil {
		//Todo: Error invalid bucket
	}
	return b.handleTx(MODE_READ, f)
}

func (db *StitchDB) Update(bucket string, f func(t *Tx) error) error {
	if !db.open {
		//Todo: return error db is closed
	}
	b, err := db.getBucket(bucket)
	if b == nil || err != nil {
		//Todo: Error invalid bucket
	}
	return b.handleTx(MODE_READ_WRITE, f)
}

func (db *StitchDB) CreateBucket(name string, options *BucketOptions) error {
	db.lock(MODE_READ_WRITE)
	if !db.open {
		//Todo: error
	}
	bkt, err := db.getBucket(name)
	if bkt != nil || err == nil {
		//Todo: error bucket already exists
	}

	bktName := strings.TrimSpace(name)
	bktFilePath := db.getDBFilePath(bktName + BUCKET_FILE_EXTENSION)
	bucket, err := NewBucket(db, options)
	if err != nil {
		//Todo: error
	}

	db.buckets[bktName] = bucket
	db.buckets[bktName].OpenBucket(bktName, bktFilePath)
	db.unlock(MODE_READ_WRITE)
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
	bkt, err := db.getBucket(bktName)
	if err != nil {
		//Todo: error
	}
	bkt.Close()
	bkt = nil
	delete(db.buckets, bktName)
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
