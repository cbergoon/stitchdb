// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"time"

	"github.com/juju/errors"
)

//IOFrequency represents the frequency in which management operations will be executed.
type IOFrequency int

const (
	//EACH action will take place at each commit/manage cycle
	EACH IOFrequency = iota
	//MNGFREQ action will take place at each manage cycle
	MNGFREQ
	//NONE action will never take place
	NONE
)

//Config holds StitchDB metadata.
type Config struct {
	persist             bool          //Indicates if the db should be persisted to disk.
	dirPath             string        //Path where db files should be stored.
	syncFreq            IOFrequency   //Interval at which the db files should be sync'd.
	writeFreq           IOFrequency   //Interval at which writes to the db should happen.
	manageFrequency     time.Duration //Interval at which db's manager should execute.
	developer           bool          //Enable developer mode.
	performanceMonitor  bool          //Enable performance monitor.
	bucketFileMultLimit int           //Compaction factor of the the bucket file.
}

//Persist enables the db to persist to disk.
func Persist(c *Config) error {
	c.persist = true
	return nil
}

//DirPath sets the path where the db should be stored.
func DirPath(path string) func(*Config) error {
	return func(c *Config) error {
		c.dirPath = path
		return nil
	}
}

//Sync sets the frequency at which the db file should be sync'd.
func Sync(frequency IOFrequency) func(*Config) error {
	return func(c *Config) error {
		c.syncFreq = frequency
		return nil
	}
}

//ManageFrequency sets the frequency at which the the db manager should run.
func ManageFrequency(frequency time.Duration) func(*Config) error {
	return func(c *Config) error {
		c.manageFrequency = frequency
		return nil
	}
}

//Developer enables developer mode.
func Developer(c *Config) error {
	c.developer = true
	return nil
}

//PerformanceMonitor enables the performance monitor.
func PerformanceMonitor(c *Config) error {
	c.performanceMonitor = true
	return nil
}

//BucketFileMultLimit sets the file compaction factor.
func BucketFileMultLimit(limit int) func(*Config) error {
	return func(c *Config) error {
		c.bucketFileMultLimit = limit
		return nil
	}
}

//NewConfig creates a new config using the provided option modifiers.
func NewConfig(options ...func(*Config) error) (*Config, error) {
	// Defaults for required values
	c := &Config{
		syncFreq:            EACH,
		dirPath:             "stitch.db",
		manageFrequency:     time.Second * time.Duration(1*time.Second),
		bucketFileMultLimit: 10,
	}
	for _, option := range options {
		err := option(c)
		if err != nil {
			return nil, errors.Annotate(err, "error: config: could not create configuration")
		}
	}
	return c, nil
}
