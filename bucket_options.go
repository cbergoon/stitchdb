package main

import (
	"strconv"
)

type BucketOptions struct {
	system   bool
	btdeg    int
	geo      bool
	georincl bool
	time     bool
}

//Implement Bucket options Here

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
		//Todo: error...
	}
	system, err := strconv.ParseBool(stmt[2])
	if err != nil {
		//Todo: error...
	}
	geo, err := strconv.ParseBool(stmt[3])
	if err != nil {
		//Todo: error...
	}
	georincl, err := strconv.ParseBool(stmt[4])
	if err != nil {
		//Todo: error...
	}
	time, err := strconv.ParseBool(stmt[5])
	if err != nil {
		//Todo: error...
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

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

//On eviction Function Move to Item
//On invalidation Function Move to Item
//Is Geo?
//On Geo Beacon is in Target Range Function Move to Item
//Geo Range is inclusive?
//Time series?
//Is System Bucket?
