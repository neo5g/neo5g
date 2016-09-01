package main

//package memdb

import (
	"bytes"
	//"encoding/binary"
	//"errors"
	"fmt"
	//"hash"
	//"hash/fnv"
	//"os"
	//"sort"
	"strconv"
	//"sync"
	"time"
	"github.com/google/btree"
	//"github.com/willf/bloom"
	//"github.com/tylertreat/BoomFilters"
	//"launchpad.net/gommap"
	//"github.com/edsrzf/mmap-go"
)


type iKey struct{
	iData []byte
	}

func (i *iKey) To(key,value []byte) int {
	buf := new(bytes.Buffer)
	buf.Write(int2byte(len(key)))
	buf.Write(int2byte(len(value)))
	buf.Write(int2byte(int(float64(len(value)) * 0.3)))
	buf.Write(key)
	buf.Write(value)
	i.iData = buf.Bytes()
	//fmt.Println("iData:",i.iData)
	return buf.Len()
	}

func (i *iKey) From() (int,int,int,[]byte,[]byte) {
	k := byte2int(i.iData[:4])
	v := byte2int(i.iData[4:8])
	r := byte2int(i.iData[8:12])
	key := i.iData[12:12+k]
	value := i.iData[12+k:12+k+v]
	return k,v,r,key,value
	}

func (i *iKey) Key() []byte {
	//fmt.Println("Key:",i.iData)
	k := byte2int(i.iData[:4])
	return i.iData[12:12+k]
	}

func (i *iKey) Value() []byte {
	k := byte2int(i.iData[:4])
	v := byte2int(i.iData[4:8])
	return i.iData[12+k:12+k+v]
	}


func (i *iKey) Reset(){
	i.iData=i.iData[:0]
	}

func byte2int(b []byte) int {
	_ = b[3]
	return int(b[0]) | int(b[1])<<8 | int(b[2])<<15 | int(b[3])<<24
}

func int2byte(i int) []byte {
	b := make([]byte, 4)
	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[2] = byte(i >> 16)
	b[3] = byte(i >> 24)
	return b
}

func (a *iKey) Less(b btree.Item) bool {
	//fmt.Println("Less:",a.Key(),b)
	return bytes.Compare(a.Key(),b.(*iKey).Key()) < 0
	}

type DB struct{
	tr *btree.BTree
	ikey *iKey
	}

func (db *DB) Delete(key []byte) []byte {
	db.ikey.To(key,[]byte(""))
	return db.tr.Delete(db.ikey).(*iKey).Key()
	}

func (db *DB) Has(key []byte) bool {
	db.ikey.To(key,[]byte(""))
	return db.tr.Get(db.ikey) != nil
	}


func (db *DB) Get(key []byte) []byte {
	db.ikey.To(key,[]byte(""))
	val := db.tr.Get(db.ikey)
	if val != nil {
		return val.(*iKey).Value()
		}
	return nil
	}

func (db *DB) Put(key,value []byte) {
	db.ikey.To(key,value)
	db.tr.ReplaceOrInsert(db.ikey)
	}

func NewDB() *DB {
	freeList := btree.NewFreeList(32)
	return &DB{tr:btree.NewWithFreeList(1<<16,freeList),ikey:new(iKey)}
	
	}

func main() {

	rc := 3
	db := NewDB()
	tp0 := time.Now()
	d := new(iKey)
	for i := 0; i < rc; i++ {
		s := strconv.Itoa(i)
		a, b := []byte("Iiiiiiiiiiii-"+s), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+s)
		db.Put(a, b)
		a, b = []byte("Jjjjjjjjjjjjj-"+s), []byte("Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-"+s)
		db.Put(a, b)
		//fmt.Println("A,B:",d.iData)
		a, b = []byte("Kkkkkkkkkkkkk-"+s), []byte("Yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-"+s)
		db.Put(a, b)
	}
	m := make([][2]string, 0)
	m = append(m, [2]string{"abc2", "1234+2babc"})
	m = append(m, [2]string{"abc21", "1234_12babc"})
	m = append(m, [2]string{"abc", "1234"})
	m = append(m, [2]string{"abc85", "1234+85"})
	m = append(m, [2]string{"abc3", "1234+3"})
	m = append(m, [2]string{"abc7", "1234+7"})
	for i := range m {
		a := []byte(m[i][0])
		b := []byte(m[i][1])
		db.Put(a, b)
	}
	d.Reset()
	tp1 := time.Now()
	fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
	//sort.Sort(&ByKey{*n});
	a,b := []byte("Zzzzzzzzzzzzzzzzzzzz-" + strconv.Itoa(1)),[]byte("")
	db.Put(a,b)
	v := db.Get(a)
	fmt.Println("V:k", v)
	db.Delete(a)
	v = db.Get(a)
	fmt.Println("V:kd", v)
	a, b = []byte("abc2"), []byte("1234+2wwwwwwwwwwwwwwwwww")
	db.Put(a,b)
	v = db.Get(a)
	fmt.Println("V:a", v)
	a, b = []byte("abc2"), []byte("1234+23")
	db.Put(a,b)
	v = db.Get(a)
	fmt.Println("V:a", v)
}
