package main

//package memdb

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
	"github.com/Workiva/go-datastructures/common"
	"github.com/Workiva/go-datastructures/btree/palm"
	
)

type iKey []byte

type iKV struct{
	k,v []byte
}


func NewKey(b... []byte) *iKey{
	buf := new(bytes.Buffer)
	buf.Write(int2byte(len(b[0])));
	if len(b) == 2 {
		buf.Write(int2byte(len(b[1])));
		}
	buf.Write(b[0]);
	if len(b) == 2 {
		buf.Write(b[1]);
		}
	ikey := iKey(buf.Bytes()) 
	return &ikey
}


func (i *iKey) To(key,value []byte) int {
	buf := new(bytes.Buffer)
	buf.Write(int2byte(len(key)))
	buf.Write(int2byte(len(value)))
	buf.Write(key)
	buf.Write(value)
	return buf.Len();
	}

func (i *iKey) From() (int,int,[]byte,[]byte) {
	k := byte2int((*i)[:4])
	v := byte2int((*i)[4:8])
	key := (*i)[8:8+k]
	value := (*i)[8+k:8+k+v]
	return k,v, key,value
	}

func (i *iKey) Key() []byte {
	//fmt.Println("Key:",i.iData)
	k := byte2int((*i)[:4])
	return (*i)[8:8+k]
	}

func (i *iKey) Value() []byte {
	k := byte2int((*i)[:4])
	v := byte2int((*i)[4:8])
	return (*i)[8+k:8+k+v]
	}


func (i iKey) Reset(){
	i=i[:0]
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


func (a *iKey) Compare(b common.Comparator) int {
	return bytes.Compare(a.Key(),b.(*iKey).Key())
	}

type iKeys []iKey

type DB struct{
	tr palm.BTree
	file *os.File
	}

func (db *DB) Open(path string) (error) {
	return nil
	}

func (db *DB) Close() {
	}

func (db *DB) load() (error) {
	return nil
	}

func (db *DB) save() (error) {
	return nil
	}

func (db *DB) sync() (error) {
	return nil
	}



func (db *DB) Delete(r ...iKV) {
	l := len(r)
	k := make([]common.Comparator,l);
	for i := 0; i < l; i++ {
		d := r[i]
		k[i] = NewKey(d.k,d.v)
		}
	db.tr.Delete(k...)
	}


func (db *DB) Get(r ...iKV) common.Comparators {
	l := len(r)
	k := make([]common.Comparator,l);
	for i := 0; i < l; i++ {
		d := r[i]
		k[i] = NewKey(d.k,d.v)
		}
	return db.tr.Get(k...)
	}

func (db *DB) Put(r ...iKV) {
	l := len(r)
	k := make([]common.Comparator,l);
	for i := 0; i < l; i++ {
		d := r[i]
		k[i] = NewKey(d.k,d.v)
		}
	db.tr.Insert(k...)
	}


type Options struct {
	bufSize uint64
	ary uint64
}

func defaultOptions() *Options {
	return &Options{bufSize:uint64(os.Getpagesize()),ary:uint64(runtime.NumCPU() << 2)}
}

func NewDB(opt *Options) *DB {
	return &DB{tr:palm.New(opt.bufSize,opt.ary)}
	
	}

func roundUp(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}


func main() {
	rc := 333
	opt := defaultOptions()
	//opt.bufSize = 1<<24
	fmt.Println("ROUNDUP",roundUp(1<<12),*opt);
	db := NewDB(opt)
	keys := make([]iKV,0);
	for i := 0; i < rc; i++ {
		s := strconv.Itoa(i)
		a, b := []byte("Iiiiiiiiiiii-"+s), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+s)
		keys = append(keys,iKV{a,b});
		a, b = []byte("Jjjjjjjjjjjjj-"+s), []byte("Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-"+s)
		keys = append(keys,iKV{a,b});
		//fmt.Println("A,B:",d.iData)
		a, b = []byte("Kkkkkkkkkkkkk-"+s), []byte("Yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-"+s)
		keys = append(keys,iKV{a,b});
	}
	tp0 := time.Now()
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
		keys = append(keys,iKV{a,b});
	}
	db.Put(keys...)
	tp1 := time.Now()
	fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
	//sort.Sort(&ByKey{*n});
	a,b := []byte("Iiiiiiiiiiii-" + strconv.Itoa(1)),[]byte("1234567890")
	v := db.Get(iKV{a,b})
	fmt.Println("V:k", v)
	db.Delete(iKV{a,b})
	v = db.Get(iKV{a,b})
	fmt.Println("V:kd", v)
	a, b = []byte("abc2"), []byte("1234+2wwwwwwwwwwwwwwwwww")
	keys = append(keys,iKV{a,b});
	v = db.Get(iKV{a,b})
	fmt.Println("V:a", string(v[0].(*iKey).Value()))
	a, b = []byte("abc2"), []byte("1234+23")
	keys = append(keys,iKV{a,b});
	v = db.Get(iKV{a,b})
	fmt.Println("V:a", string(v[0].(*iKey).Value()))
	ln:=db.tr.Len()
	fmt.Println("Len():",ln);
	_=db.tr.Query(NewKey([]byte("abc")),NewKey([]byte("Zzzzzzzzzzzzzzzzzzzz-" + strconv.Itoa(1))));
	for i,j := 0,int(ln); i < j; i++ {
		//fmt.Println("Q:",string(q[i].(*iKey).Key()),string(q[i].(*iKey).Value()));
		}
}
