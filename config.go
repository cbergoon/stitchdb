package main

import (
	"errors"
	"time"
)

var (
	ErrCouldNotCreateConfig = errors.New("config: could not create configuration")
)

type SyncFrequency int

const (
	EACH SyncFrequency = iota
	SECOND
	NONE
)

type Config struct {
	Persist            bool
	DirPath            string
	Sync               SyncFrequency
	ManageFrequency    time.Duration
	Developer          bool
	PerformanceMonitor bool
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

func Sync(frequency SyncFrequency) func(*Config) error {
	return func(c *Config) error {
		c.Sync = frequency
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

func NewConfig(options ...func(*Config) error) (*Config, error) {
	// Defaults for required values
	c := &Config{
		Sync: EACH,
		DirPath: "stitch.db",
		ManageFrequency: time.Second * time.Duration(1 * time.Second),
	}
	for _, option := range options {
		err := option(c)
		if err != nil {
			return nil, ErrCouldNotCreateConfig
		}
	}
	return c, nil
}
