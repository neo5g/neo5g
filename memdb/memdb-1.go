package main

//package memdb

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
	"github.com/Workiva/go-datastructures/common"
	"github.com/Workiva/go-datastructures/btree/palm"
	
)

type iKey []byte


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
	}

func (db *DB) Delete(key []byte) []byte {
	db.tr.Delete(NewKey(key))
	return key;
	}

func (db *DB) Has(key []byte) bool {
	return db.tr.Get(NewKey(key))[0] != nil
	}


func (db *DB) Get(key []byte) []byte {
	fmt.Println("Get:",string(key));
	val := db.tr.Get(NewKey(key))
	fmt.Println("VAL:",val);
	if val[0] != nil {
		return val[0].(*iKey).Value()
		}
	return nil
	}

func (db *DB) Put(key,value []byte) {
	db.tr.Insert(NewKey(key,value))
	}

func NewDB() *DB {
	return &DB{tr:palm.New(1 << 12,1<<8)}
	
	}

func main() {

	rc := 333333
	db := NewDB()
	tp0 := time.Now()
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
	tp1 := time.Now()
	fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
	//sort.Sort(&ByKey{*n});
	a,b := []byte("Zzzzzzzzzzzzzzzzzzzz-" + strconv.Itoa(1)),[]byte("1234567890")
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
	ln:=db.tr.Len()
	fmt.Println("Len():",ln);
	_=db.tr.Query(NewKey([]byte("abc")),NewKey([]byte("Zzzzzzzzzzzzzzzzzzzz-" + strconv.Itoa(1))));
	for i,j := 0,int(ln); i < j; i++ {
		//fmt.Println("Q:",string(q[i].(*iKey).Key()),string(q[i].(*iKey).Value()));
		}
}
