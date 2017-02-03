package main

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// Notes: The name came from a function in a legacy codebase called StitchGeo which took sets time series geo
// information and and coalesced them together intelligently.

type StitchDB struct {
	config     *Config
	lock       sync.RWMutex
	open       bool
	buckets    map[string]*Bucket
	system     *Bucket
	filelock   sync.Mutex
	bktcfgfile *os.File
}

func NewStitchDb(config *Config) (*StitchDB, error) {
	sysbktopts, err := NewBucketOptions()
	if err != nil {
		//Todo: Error
		return nil, errors.New("error: failed to create system bucket options")
	}
	sysbkt, err := NewBucket(sysbktopts)
	if err != nil {
		//Todo: Error
		return nil, errors.New("error: failed to create system bucket")
	}
	stitch := &StitchDB{
		config:  config,
		buckets: make(map[string]*Bucket),
		system:  sysbkt,
	}
	return stitch, nil
}

func (db *StitchDB) Open() error {
	if db.config.Persist {
		//Open the Config File
		//For each listed bucket
		//	open bucket file
		//	create and load bucket
	}
	go db.runManager()
	db.open = true
	return nil
}

func (db *StitchDB) Close() error {
	//sync files
	//close files
	//set all refs to nil
	return nil
}

func (db *StitchDB) Export(writer io.Writer) error {
	//write items in file format to buffer
	return nil
}

func (db *StitchDB) runManager() error {
	mngct := time.NewTicker(db.config.ManageFrequency)
	defer mngct.Stop()
	for range mngct.C {
		//if on "second" frequency write bucket config file
		//for each bucket call bucket manager
	}
	return nil
}

func (db *StitchDB) GetConfig() *Config {
	return db.config
}

func (db *StitchDB) SetConfig(config *Config) {
	db.config = config
}

func (db *StitchDB) View(bucket string, f func(t *Tx) error) error {
	var b *Bucket
	var ok bool
	if bucket == "_sys" {
		b = db.system
	} else {
		b, ok = db.buckets[bucket]
	}
	if !ok {
		//Todo: Error bucket does not exist
	}
	return b.handleTx(MODE_READ, f)
}

func (db *StitchDB) Update(bucket string, f func(t *Tx) error) error {
	var b *Bucket
	var ok bool
	if bucket == "_sys" {
		b = db.system
	} else {
		b, ok = db.buckets[bucket]
	}
	if !ok {
		//Todo: Error bucket does not exist
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
