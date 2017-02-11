package main

import (
	"fmt"
	"time"
)

func main() {

	c, _ := NewConfig(Persist, DirPath("path/to/loc/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	s, _ := NewStitchDB(c)
	s.Open()
	//opts, _ := NewBucketOptions(BTreeDegree(32))
	//s.CreateBucket("test", opts)
	//s.CreateBucket("test1", opts)
	//s.CreateBucket("test2", opts)
	//s.DropBucket("test1")
	fmt.Println(s)

}
