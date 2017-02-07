package main

import (
	"errors"
	"time"
)

var (
	ErrCouldNotCreateConfig = errors.New("config: could not create configuration")
)

type IOFrequency int

const (
	EACH    IOFrequency = iota
	MNGFREQ
	NONE
)

type Config struct {
	Persist             bool
	DirPath             string
	SyncFreq            IOFrequency
	WriteFreq           IOFrequency
	ManageFrequency     time.Duration
	Developer           bool
	PerformanceMonitor  bool
	BucketFileMultLimit int
}

func Persist(c *Config) error {
	c.Persist = true
	return nil
}

func DirPath(path string) func(*Config) error {
	return func(c *Config) error {
		c.DirPath = path
		return nil
	}
}

func Sync(frequency IOFrequency) func(*Config) error {
	return func(c *Config) error {
		c.SyncFreq = frequency
		return nil
	}
}

func ManageFrequency(frequency time.Duration) func(*Config) error {
	return func(c *Config) error {
		c.ManageFrequency = frequency
		return nil
	}
}

func Developer(c *Config) error {
	c.Developer = true
	return nil
}

func PerformanceMonitor(c *Config) error {
	c.PerformanceMonitor = true
	return nil
}

func BucketFileMultLimit(limit int) func(*Config) error {
	return func(c *Config) error {
		c.BucketFileMultLimit = limit
		return nil
	}
}

func NewConfig(options ...func(*Config) error) (*Config, error) {
	// Defaults for required values
	c := &Config{
		SyncFreq:            EACH,
		DirPath:             "stitch.db",
		ManageFrequency:     time.Second * time.Duration(1*time.Second),
		BucketFileMultLimit: 10,
	}
	for _, option := range options {
		err := option(c)
		if err != nil {
			return nil, ErrCouldNotCreateConfig
		}
	}
	return c, nil
}
