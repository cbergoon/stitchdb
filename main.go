package main

import (
	"fmt"
	"time"
)

func main() {

	c, _ := NewConfig(Persist, DirPath("path/to/loc/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	s, _ := NewStitchDB(c)

	s.Open()

	opts, _ := NewBucketOptions(BTreeDegree(32))
	s.CreateBucket("test", opts)
	s.CreateBucket("test1", opts)
	s.CreateBucket("test2", opts)
	s.DropBucket("test1")

	//fmt.Println(s)

	b, _ := s.getBucket("test")
	for i := 0; i < 100; i++ {
		e, _ := NewEntry("k"+string(i), "v"+string(i), nil)
		b.insert(e)
	}

	fmt.Println("after insert")

	s.View("test", func(t *Tx) error {
		t.Ascend(func(e *Entry) bool {
			fmt.Println("Here in the Ascend: ", e)
			return true
		})
		return nil
	})

	fmt.Println("after iterate")

	//fmt.Println(b.get(&Entry{k: "k2"}))

	time.Sleep(time.Second * 4)
	s.Close()
}
