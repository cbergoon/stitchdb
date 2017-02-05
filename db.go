package main

import (
	"bufio"
	"errors"
	"os"
	"strings"
	"sync"
)

// Notes: The name came from a function in a legacy codebase called StitchGeo which took sets time series geo
// information and and coalesced them together intelligently.

const (
	BUCKET_CONFIG_FILE    string = "sbkt.conf"
	BUCKET_FILE_EXTENSION string = ".stitch"
)

type StitchDB struct {
	config     *Config
	dblock     sync.RWMutex
	open       bool
	buckets    map[string]*Bucket
	system     *Bucket
	filelock   sync.Mutex
	bktcfgfile *os.File
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
			db.buckets[bktName].InstantiateBucket(bktName, bktFilePath)
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
	//Todo: Lock DB?
	for key := range db.buckets {
		err := db.buckets[key].Close()
		if err != nil {
			//Todo: log error
		}
		db.buckets[key] = nil
	}
	db.system.Close()
	err := db.bktcfgfile.Sync()
	if err != nil {
		//Todo: log error
	}
	err = db.bktcfgfile.Close()
	if err != nil {
		//Todo: log error
	}
	db.open = false
	db.buckets = nil
	db.system = nil
	db.bktcfgfile = nil
	//Todo: Unlock DB?
	return nil
}

func (db *StitchDB) runManager() error {
	//if on "second" frequency write bucket config file
	//for each bucket call bucket manager
	return nil
}

func (db *StitchDB) GetConfig() *Config {
	return db.config
}

func (db *StitchDB) SetConfig(config *Config) {
	db.config = config
}

func (db *StitchDB) getBucket(name string) (*Bucket, error) {
	var b *Bucket
	var ok bool
	if name == "_sys" {
		b = db.system
	} else {
		b, ok = db.buckets[name]
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

//func (db *StitchDB) runTx(bucket string, write bool, f func(t *Tx) error) error {
//	return nil
//}

func (db *StitchDB) CreateBucket(name string, options *BucketOptions) error {
	return nil
}

func (db *StitchDB) DropBucket(name string) error {
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
