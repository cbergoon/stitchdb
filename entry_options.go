package main

import "time"

type EntryOptions struct {
	doesExp bool
	doesInv bool
	expTime time.Time
	invTime time.Time
}

func ExpireTime(time time.Time) func(*EntryOptions) error {
	return func(e *EntryOptions) error {
		e.expTime = time
		return nil
	}
}

func InvalidTime(time time.Time) func(*EntryOptions) error {
	return func(e *EntryOptions) error {
		e.invTime = time
		return nil
	}
}

func NewEntryOptions(options ...func(*EntryOptions) error) (*EntryOptions, error) {
	c := &EntryOptions{}
	for _, option := range options {
		err := option(c)
		if err != nil {
			return nil, ErrCouldNotCreateConfig
		}
	}
	return c, nil
}
