package main

import (
	"strconv"

	"github.com/juju/errors"
)

type BucketOptions struct {
	system   bool
	btdeg    int
	geo      bool
	georincl bool
	time     bool
}

func System(b *BucketOptions) error {
	b.system = true
	return nil
}

func Geo(b *BucketOptions) error {
	b.geo = true
	return nil
}

func GeoRangeIsInclusive(b *BucketOptions) error {
	b.georincl = true
	return nil
}

func Time(b *BucketOptions) error {
	b.time = true
	return nil
}

func BTreeDegree(degree int) func(*BucketOptions) error {
	return func(b *BucketOptions) error {
		b.btdeg = degree
		return nil
	}
}

func NewBucketOptions(options ...func(*BucketOptions) error) (*BucketOptions, error) {
	c := &BucketOptions{}
	for _, option := range options {
		err := option(c)
		if err != nil {
			return nil, ErrCouldNotCreateConfig
		}
	}
	return c, nil
}

func (b *BucketOptions) bucketOptionsCreateStmt() []byte {
	var cbuf []byte
	cbuf = append(cbuf, strconv.Itoa(b.btdeg)...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, strconv.Itoa(boolToInt(b.system))...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, strconv.Itoa(boolToInt(b.geo))...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, strconv.Itoa(boolToInt(b.georincl))...)
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, strconv.Itoa(boolToInt(b.time))...)
	return cbuf
}

func NewBucketOptionsFromStmt(stmt []string) (*BucketOptions, error) {
	btdeg, err := strconv.ParseInt(stmt[1], 10, 64)
	if err != nil {
		return nil, errors.Annotate(err, "error: failed to parse bucket options")
	}
	system, err := strconv.ParseBool(stmt[2])
	if err != nil {
		return nil, errors.Annotate(err, "error: failed to parse bucket options")
	}
	geo, err := strconv.ParseBool(stmt[3])
	if err != nil {
		return nil, errors.Annotate(err, "error: failed to parse bucket options")
	}
	georincl, err := strconv.ParseBool(stmt[4])
	if err != nil {
		return nil, errors.Annotate(err, "error: failed to parse bucket options")
	}
	time, err := strconv.ParseBool(stmt[5])
	if err != nil {
		return nil, errors.Annotate(err, "error: failed to parse bucket options")
	}
	opts := &BucketOptions{
		btdeg:    int(btdeg),
		system:   system,
		geo:      geo,
		georincl: georincl,
		time:     time,
	}
	return opts, nil
}

//On eviction Function Move to Item
//On invalidation Function Move to Item
//Is Geo?
//On Geo Beacon is in Target Range Function Move to Item
//Geo Range is inclusive?
//Time series?
//Is System Bucket?
