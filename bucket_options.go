// Copyright 2017 Cameron Bergoon
// Licensed under the LGPLv3, see LICENCE file for details.

package stitchdb

import (
	"strconv"

	"github.com/juju/errors"
)

//BucketOptions holds bucket metadata.
type BucketOptions struct {
	system   bool //Indicates that this bucket is the system bucket.
	btdeg    int  //Dergee of the B-Tree; used to optimize performance based on use case.
	geo      bool //Indicates if the bucket is geo enabled or not.
	georincl bool //Indicates if the range of radius searches are inclusive or exclusive.
	time     bool //Indicates if the bucket is time series enabled. Todo: Implement
	dims     int  //Number of dimensions the geo functionality will utilize.
}

//System sets the system option.
func System(b *BucketOptions) error {
	b.system = true
	return nil
}

//Geo enables geo-location functionality.
func Geo(b *BucketOptions) error {
	b.geo = true
	return nil
}

//GeoRangeIsInclusive enables inclusive range checks.
func GeoRangeIsInclusive(b *BucketOptions) error {
	b.georincl = true
	return nil
}

//Time enable the time series functionality.
func Time(b *BucketOptions) error {
	b.time = true
	return nil
}

//Dims sets the number of dimensions that will be utilized.
func Dims(dims int) func(*BucketOptions) error {
	return func(b *BucketOptions) error {
		b.dims = dims
		return nil
	}
}

//BTreeDegree sets the degree of the trees ued for the bucket.
func BTreeDegree(degree int) func(*BucketOptions) error {
	return func(b *BucketOptions) error {
		b.btdeg = degree
		return nil
	}
}

//NewBucketOptions creates a new bucket options using the provided option modifiers.
func NewBucketOptions(options ...func(*BucketOptions) error) (*BucketOptions, error) {
	c := &BucketOptions{}
	for _, option := range options {
		err := option(c)
		if err != nil {
			return nil, errors.New("error: bucket_options: could not create bucket options")
		}
	}
	return c, nil
}

//bucketOptionsCreateStmt returns the statement that represents the bucket options.
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
	cbuf = append(cbuf, ':')
	cbuf = append(cbuf, strconv.Itoa(b.dims)...)
	return cbuf
}

//NewBucketOptionsFromStmt returns bucket options representing the options portion of the statement. Returns an error if the
//bucket statement could not be parsed.
func NewBucketOptionsFromStmt(stmt []string) (*BucketOptions, error) {
	btdeg, err := strconv.ParseInt(stmt[1], 10, 64)
	if err != nil {
		return nil, errors.Annotate(err, "error: bucket_optiona: failed to parse bucket options")
	}
	system, err := strconv.ParseBool(stmt[2])
	if err != nil {
		return nil, errors.Annotate(err, "error: bucket_optiona: failed to parse bucket options")
	}
	geo, err := strconv.ParseBool(stmt[3])
	if err != nil {
		return nil, errors.Annotate(err, "error: bucket_optiona: failed to parse bucket options")
	}
	georincl, err := strconv.ParseBool(stmt[4])
	if err != nil {
		return nil, errors.Annotate(err, "error: bucket_optiona: failed to parse bucket options")
	}
	time, err := strconv.ParseBool(stmt[5])
	if err != nil {
		return nil, errors.Annotate(err, "error: bucket_optiona: failed to parse bucket options")
	}
	dims, err := strconv.ParseInt(stmt[6], 10, 64)
	if err != nil {
		return nil, errors.Annotate(err, "error: bucket_optiona: failed to parse bucket options")
	}
	opts := &BucketOptions{
		btdeg:    int(btdeg),
		system:   system,
		geo:      geo,
		georincl: georincl,
		time:     time,
		dims:     int(dims),
	}
	return opts, nil
}
