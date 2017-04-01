<p align="center">
<img 
    src="logo.png" 
    width="338" height="63" border="0" alt="StitchDB">
<br>
<a href="https://travis-ci.org/cbergoon/stitchdb"><img src="https://travis-ci.org/cbergoon/stitchdb.svg?branch=master" alt="Build"></a>
<a href="https://goreportcard.com/report/github.com/cbergoon/stitchdb"><img src="https://goreportcard.com/badge/github.com/cbergoon/stitchdb" alt="Report"></a>
<a href="http://gocover.io/github.com/cbergoon/stitchdb"><img src="https://img.shields.io/badge/coverage-76%25-brightgreen.svg" alt="Coverage"></a>
<a href="https://godoc.org/github.com/cbergoon/stitchdb"><img src="https://img.shields.io/badge/godoc-reference-brightgreen.svg" alt="Docs"></a>
<a href="http://gocover.io/github.com/cbergoon/stitchdb"><img src="https://img.shields.io/badge/version-0.1.0-brightgreen.svg" alt="Version"></a>
</p>

Yet another key value store - StitchDB is an in memory key-value store persisted with an append only log with support for 
geo-location and time series data. StitchDB is accompanied by [StitchQL](https://github.com/cbergoon/stitchql) and [StitchServer](https://github.com/cbergoon/stitchserver). Together these projects 
result in an entire database ecosystem complete with query language and standalone server daemon. 

StitchDB's API is inspired by boltdb/bolt and tidwall/buntdband making use of their elegant API design. StitchDB strives 
to add a feature set that is tailored to a high throughput and less rigidly persistent use case with builtin multidimensional
geo-location and time-series data support. 

All contributions, ideas, and criticisms are welcome.
 
### Coming Soon
* StitchQL: A Query Language for StitchDB
  * Interpreted Language of Some Sort
  * Verb-like Syntax Tailored to Geo/Time Use Case
* StitchServer: An HTTP API Around Stitch DB With Management System 
  * DB Viewer/Editor
  * Users/Authentication
  * Performance Monitor/System Info
  * R-Tree Viewer 
* Built in Time Series Support
  
### Goals
StitchDB was born out of a need to replace a legacy timeseries/geolocation package with a more robust real-time solution 
that could stand alone as a separate service with little work. It needed to have separation of data or buckets, searchable 
indexes, invalidation, expiration, and custom event callbacks. Additionally, we wanted the operation and code to remain as
light weight and manipulable as possible.
 
### Tradeoffs and Consideraitons
* Fast operation and real-time snapshot of data over hardened and optimized data persistence.
* Geographical functionality built in.
* Easily extensible API and feature set.
* Definable triggers for items, and buckets.
* Ability for items to be expired and invalidated. 
* Minimal "stop-the-world" db/bucket manager executions.
 
### Documentation

API documentation is available at [stitchdb Godoc](https://godoc.org/github.com/cbergoon/stitchdb)

The Wiki is full of explanations and examples:

[https://github.com/cbergoon/stitchdb/wiki](https://github.com/cbergoon/stitchdb/wiki)

### Usage

There are more complete examples and how-to's in the resources above but to get started all you need to do is install StitchDB. 

The dependencies for StitchDB are available by running the following:

```bash
go get github.com/cbergoon/btree
go get github.com/tidwall/gjson
go get github.com/dhconnelly/rtreego
go get github.com/juju/errors
```

```bash 
go get github.com/cbergoon/StitchDB
```

Here is some boiler plate code to get started with:  

```go
package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/cbergoon/stitchdb"
)

func main() {

	c, _ := stitchdb.NewConfig(stitchdb.Persist, stitchdb.DirPath("path/to/loc/"), stitchdb.Sync(stitchdb.MNGFREQ), stitchdb.ManageFrequency(1*time.Second), stitchdb.Developer, stitchdb.PerformanceMonitor, stitchdb.BucketFileMultLimit(10))
	s, _ := stitchdb.NewStitchDB(c)

	s.Open()

	opts, _ := stitchdb.NewBucketOptions(stitchdb.BTreeDegree(32), stitchdb.Geo)
	s.CreateBucket("test", opts)

	s.Update("test", func(t *stitchdb.Tx) error {
		t.CreateIndex("user", stitchdb.INT_INDEX)
		for i := 0; i < 10; i++ {
			eopt, _ := stitchdb.NewEntryOptions()
			e, _ := stitchdb.NewEntry("k"+strconv.Itoa(i), "{ \"user\":\""+strconv.Itoa(10-i)+"\", \"coords\": ["+strconv.Itoa(i)+", 3.0]}", true, eopt)
			t.Set(e)
		}
		return nil
	})

	s.View("test", func(t *stitchdb.Tx) error {
		sz, _ := t.Size("")
		fmt.Println("Size: ", sz)

		t.Ascend("", func(e *stitchdb.Entry) bool {
			fmt.Println("Ascend Entries: ", e)
			return true
		})
		rect, _ := stitchdb.NewRect(stitchdb.Point{0.0, 0.0}, []float64{10, 10})
		fmt.Print("Nearest Neighbor: ")
		fmt.Print(t.NearestNeighbor(stitchdb.Point{5.2, 3.0}))
		fmt.Print("\n")
		fmt.Print("Search Within Radius: ")
		fmt.Print(t.SearchWithinRadius(stitchdb.Point{0.0, 0.0}, 5))
		fmt.Print("\n")
		fmt.Print("Search Intersect: ")
		fmt.Print(t.SearchIntersect(rect))
		fmt.Print("\n")
		return nil
	})

	time.Sleep(time.Second * 4)
	s.Close()
}

```

Then run it with:
```bash
go run <filename>.go
```

### Performance

Benchmarks are ran on a MacBook Pro 3.1Ghz, 16GB RAM, SSD 

#### Insertion

#### Retrieval  

### StitchDB Ecosystem (Future Work) 
* [stitchserver](https://github.com/cbergoon/stitchserver) - Builds a HTTP and RPC API layer over StitchDB allowing it to operate as a standalone service.
* [stitchraft](https://github.com/cbergoon/stitchraft) - An distributed and consistent service that adds RAFT to stitchserver (work in progress name).
* [stitchql](https://github.com/cbergoon/stitchql) - A query language that interpreter that provides implements a simple language to access/manipulate StitchDB.

### License 

This project is licensed un the GNU Lesser General Public License. See the [LICENSE](https://github.com/cbergoon/stitchdb/blob/master/LICENSE) file. 

For license information on included libraries see [LICENSE-3RD-PARTY](https://github.com/cbergoon/stitchdb/blob/master/LICENSE-3RD=PARTY) file. 
