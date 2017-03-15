package main

import (
	"fmt"
	"time"
	//"strconv"
	"strconv"

	"github.com/dhconnelly/rtreego"
)

func main() {

	c, _ := NewConfig(Persist, DirPath("path/to/loc/"), Sync(MNGFREQ), ManageFrequency(1*time.Second), Developer, PerformanceMonitor, BucketFileMultLimit(10))
	s, _ := NewStitchDB(c)

	s.Open()

	opts, _ := NewBucketOptions(BTreeDegree(32), Geo)
	s.CreateBucket("test", opts)
	s.CreateBucket("test1", opts)
	s.CreateBucket("test2", opts)
	s.DropBucket("test1")

	//fmt.Println(s)

	//b, _ := s.getBucket("test")
	//for i := 0; i < 10000; i++ {
	//	e, _ := NewEntry("k"+strconv.Itoa(i), "v"+strconv.Itoa(i), nil)
	//	b.insert(e)
	//}

	fmt.Println("after insert")

	tis := time.Now().UnixNano()
	s.Update("test", func(t *Tx) error {
		t.CreateIndex("hey", INT_INDEX)
		for i := 0; i < 1000; i++ {
			eopt, _ := NewEntryOptions()
			e, _ := NewEntry("kr2"+strconv.Itoa(i), "{ \"hey\":\""+strconv.Itoa(1000-i)+"\", \"coords\": ["+strconv.Itoa(i)+", 3.0]}", true, eopt)
			t.Set(e)
		}
		//t.Ascend(func(e *Entry) bool {
		//	//fmt.Println("Here in the Ascend: ", e)
		//	return true
		//})
		//eopt, _ := NewEntryOptions()
		//e, _ := NewEntry("k99", "vvvvvvvv99999999999999", eopt)
		//t.Set(e)
		//t.Descend(func(e *Entry) bool {
		//	//fmt.Println("Here in the Ascend: ", e)
		//	return true
		//})
		//return errors.New("")
		return nil
	})
	tie := time.Now().UnixNano() - tis
	fmt.Println("time: ", tie)
	fmt.Println(s.open)
	tis1 := time.Now().UnixNano()
	s.Update("test", func(t *Tx) error {
		sz, _ := t.Size("")
		fmt.Println("s: ", sz)
		err := t.Ascend("", func(e *Entry) bool {
			//if e.k == "k784" {
			fmt.Println("Here in the Ascend1: ", e)
			//}
			return true
		})
		fmt.Println(t.NearestNeighbor(rtreego.Point{54.2, 3.0}))
		fmt.Println("err: ", err)
		//t.Ascend("hey", func(e *Entry) bool {
		//	//if e.k == "k784" {
		//	fmt.Println("Here in the Ascend2: ", e)
		//	//}
		//	return true
		//})
		return nil
	})
	tie1 := time.Now().UnixNano() - tis1
	fmt.Println("time: ", tie1)

	fmt.Println("after iterate")

	//fmt.Println(b.get(&Entry{k: "k2"}))

	time.Sleep(time.Second * 4)
	s.Close()
}
