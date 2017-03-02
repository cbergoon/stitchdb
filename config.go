package main

import (
	"time"

	"github.com/juju/errors"
)

type IOFrequency int

const (
	EACH IOFrequency = iota
	MNGFREQ
	NONE
)

type Config struct {
	persist             bool
	dirPath             string
	syncFreq            IOFrequency
	writeFreq           IOFrequency
	manageFrequency     time.Duration
	developer           bool
	performanceMonitor  bool
	bucketFileMultLimit int
}

func Persist(c *Config) error {
	c.persist = true
	return nil
}

func DirPath(path string) func(*Config) error {
	return func(c *Config) error {
		c.dirPath = path
		return nil
	}
}

func Sync(frequency IOFrequency) func(*Config) error {
	return func(c *Config) error {
		c.syncFreq = frequency
		return nil
	}
}

func ManageFrequency(frequency time.Duration) func(*Config) error {
	return func(c *Config) error {
		c.manageFrequency = frequency
		return nil
	}
}

func Developer(c *Config) error {
	c.developer = true
	return nil
}

func PerformanceMonitor(c *Config) error {
	c.performanceMonitor = true
	return nil
}

func BucketFileMultLimit(limit int) func(*Config) error {
	return func(c *Config) error {
		c.bucketFileMultLimit = limit
		return nil
	}
}

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
			return nil, errors.Annotate(err, "config: could not create configuration")
		}
	}
	return c, nil
}
