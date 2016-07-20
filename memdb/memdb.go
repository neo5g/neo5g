package main
//package memdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Key []byte

type Value []byte

type kv struct {
	offset	int
	klen	int
	vlen	int
}

type kvs []kv

type kvData []byte

type ns struct{
	mu sync.RWMutex
	Comparator func(a,b []byte) int
	index []int
	kvs kvs
	kvData kvData
	kvSize int
}

type Nss []ns

//func (ns *ns) Len() int { return len(ns.kvs)}
func (ns *ns) Len() int { return len(ns.index)}

//func (ns *ns) Swap(i,j int) {ns.kvs[i],ns.kvs[j] = ns.kvs[j],ns.kvs[i]}
func (ns *ns) Swap(i,j int) {ns.index[i],ns.index[j] = ns.index[j],ns.index[i]}


type ByKey struct {ns}
/*
func (ns *ByKey) Less(i,j int) bool {
	o1 := ns.kvs[i]
	o2 := ns.kvs[j]
	k1 := Key(ns.kvData[o1.offset:o1.offset+o1.klen])
	k2 := Key(ns.kvData[o2.offset:o2.offset+o2.klen])
	return ns.compare(k2,k1) >= 0
	}
*/
func (ns *ByKey) Less(i,j int) bool {
	o1 := ns.kvs[ns.index[i]]
	o2 := ns.kvs[ns.index[j]]
	k1 := Key(ns.kvData[o1.offset:o1.offset+o1.klen])
	k2 := Key(ns.kvData[o2.offset:o2.offset+o2.klen])
	return ns.compare(k2,k1) >= 0
	}

func (ns *ns) compare(key1,key2 Key) int {
	return ns.Comparator([]byte(key1),[]byte(key2));
}

func (ns *ns) delete(key Key) {
	i,err := ns.find(key);
	if err == nil {ns.deleteKey(i);}
}

func (ns *ns) deleteKey(i int) {
	idx := ns.index[i]
	r := ns.kvs[idx]
	ns.kvData = append(ns.kvData[:r.offset],ns.kvData[r.offset+r.klen+r.vlen:]...)
	ns.kvs = append(ns.kvs[:idx], ns.kvs[idx:]...);
	ns.index = append(ns.index[:i],ns.index[i:]...);
	ns.kvSize = ns.kvSize - r.klen - r.vlen;
}

func (ns *ns) find(key Key) (int,error){
	s := sort.Search(len(ns.index), func(i int) bool {
		//fmt.Println("Bytes compare:",string(key),string(ns.getKey(i)),bytes.Compare([]byte(key),[]byte(ns.getKey(i))) == 0);
		return ns.compare(ns.getKey(i),key) >=0 });
	//fmt.Println("Search:",string(key),len(ns.kvs),s);
	if s < len(ns.kvs) && ns.compare(ns.getKey(s),key) == 0 {return s,nil}
	return -1,errors.New("<Not found>");
}

func (ns *ns) getKey(i int) Key {
	r := ns.kvs[ns.index[i]]
	k := Key(ns.kvData[r.offset:r.offset + r.klen])
	//fmt.Println("getKey:",i,string(k));
	return k
}

func (ns *ns) getValue(i int) Value {
	//fmt.Println("getValue:",i,ns.kvs[i]);
	r := ns.kvs[ns.index[i]]
	return Value(ns.kvData[r.offset + r.klen:r.offset + r.klen + r.vlen])
}

func (ns *ns) get(key Key) (Value,error) {
	k,err := ns.find(key);
	fmt.Println("get:",k,err);
	if err != nil {
		return nil,err;
	}
	return ns.getValue(k),nil;
}

func (ns *ns) has(key Key) bool {
	_, err := ns.find(key)
	return err == nil;
}

func NewKV(offset,lkey,lval int) kv {
	return kv{offset,lkey,lval}
}

func (ns *ns) put(key Key, value Value) error {
	ns.delete(key);
	ns.kvs = append(ns.kvs,NewKV(ns.kvSize,len(key),len(value)));
	ns.kvData = append(ns.kvData,[]byte(key)...);
	ns.kvData = append(ns.kvData,[]byte(value)...);
	ns.kvSize = ns.kvSize + len(key) + len(value);
	ns.index = append(ns.index,len(ns.kvs)-1);
	//fmt.Println("kvData:",string(ns.kvData));
	return nil
}

func (ns *ns) Delete(key Key) {
	ns.delete(key);
	}

func (ns *ns) Has(key Key) bool {
	return ns.has(key);
}

func (ns *ns) Get(key Key) (Value,error) {
	return ns.get(key);
}

func (ns *ns) Put(key Key, value Value) error {
	err := ns.put(key,value);
	if err == nil {sort.Sort(&ByKey{*ns});}
	return err
}

func (ns *ns) Seek(key []byte) Value {
	return Value("key")
}

func NewNs() *ns {return new(ns)}


type MemDB struct{
	nss		[]ns
	nsSize	uint64
}

func (db *MemDB) getNS(parent, namespace []byte) int64 {
	val,err := db.nss[0].Get(append(parent,namespace...));
	if err != nil { return -1}
	v,c := binary.Varint(val);
	if c > 0 {return v;}
	return -1
}

func (db *MemDB) putNS(parent, namespace,value []byte) int64 {
	val,err := db.nss[0].Get(append(parent,namespace...));
	if err != nil { return -1}
	v,c := binary.Varint(val);
	if c > 0 {return v;}
	return -1
}


/* 

func (db *memDB) Get(ns uint64,key []byte) Value{
	return ns[ns].Get(key);
}

func (db *memDB) Delete(ns, key []byte) Value{
	id := getNS(ns);
	return ns[id].Get(key);
}

func (db *memDB) Has(ns, key []byte) Value{
	id := getNS(ns);
	return ns[id].Get(key);
}

func (db *memDB) Get(ns, key []byte) Value{
	id := getNS(ns);
	return ns[id].Get(key);
}

func (db *emDB) Put(ns,key,value []byte) Value{
	id := getNS(ns);
	return ns[id].Put(key,value);
}

func (db *Memdb) Seek(ns, key []byte) Value{
	id := getNS(ns);
	return ns[id].Seek(key);
}
*/
func main(){

 n := NewNs();
 //n := ns{}
 n.Comparator = bytes.Compare
 n.Put(Key("abc2"),Value("1234+2babc"))
 n.Put(Key("abc"),Value("1234"));
 n.Put(Key("abc1"),Value("1234+1"))
 n.Put(Key("abc85"),Value("1234+85"))
 n.Put(Key("abc3"),Value("1234+3"));
 n.Put(Key("abc7"),Value("1234+7"))

tp0 := time.Now() 
 for i:=10000; i >= 0; i-- {
	n.Put(Key("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-"+string(i)),Value("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+string(i)));
 }
tp1 := time.Now()
fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
/*
 for i := 0; i < len(n.index); i++ {
 fmt.Println("KV:",i,string(n.getKey(i)));
 }

t0 := time.Now()
sort.Sort(&ByKey{n});
t1 := time.Now()
fmt.Printf("The call sorting %v to run.\n", t1.Sub(t0))
 */

//sort.Sort(&ByKey{*n});
v,err := n.Get(Key("abc2"))
fmt.Println("V:",string(v),err)
//fmt.Println(string(n.kvData))
n.Delete(Key("abc2"))
//fmt.Println(string(n.kvData))
v,err = n.Get(Key("abc2"))
fmt.Println("V:",string(v),err)
//fmt.Println("INDEX:",n.index)
}


