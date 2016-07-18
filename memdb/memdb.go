package memdb

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
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
	comparator func(a,b []byte) int
	kvs kvs
	kvData kvData
	kvSize int
}

type Nss []ns

func (ns *ns) Len() int { return len(ns.kvs)}

func (ns *ns) Swap(i,j int) {ns.kvs[i],ns.kvs[j] = ns.kvs[j],ns.kvs[i]}

type ByKey struct {ns}

func (ns *ByKey) Less(i,j int) bool {
	o1 := ns.kvs[i]
	o2 := ns.kvs[j]
	k1 := Key(ns.kvData[o1.offset:o1.offset+o1.klen])
	k2 := Key(ns.kvData[o2.offset:o2.offset+o2.klen])
	return ns.compare(k2,k1) >= 0
	}

func (ns *ns) compare(key1,key2 Key) int {
	return ns.comparator([]byte(key1),[]byte(key2));
}

func (ns *ns) delete(key Key) {
	i,err := ns.find(key);
	if err != nil {fmt.Println("Delete:",i,key);}
}

func (ns *ns) find(key Key) (int,error){
	s := sort.Search(len(ns.kvs), func(i int) bool {
		//fmt.Println("Bytes compare:",string(key),string(ns.getKey(i)),bytes.Compare([]byte(key),[]byte(ns.getKey(i))) == 0);
		return ns.compare(ns.getKey(i),key) >=0 });
	fmt.Println("Search:",string(key),len(ns.kvs),s);
	if s < len(ns.kvs) && ns.compare(ns.getKey(s),key) == 0 {return s,nil}
	return -1,errors.New("<Not found>");
}

func (ns *ns) getKey(i int) Key {
	k := Key(ns.kvData[ns.kvs[i].offset:ns.kvs[i].offset + ns.kvs[i].klen])
	//fmt.Println("getKey:",i,string(k));
	return k
}

func (ns *ns) getValue(i int) Value {
	//fmt.Println("getValue:",i,ns.kvs[i]);
	return Value(ns.kvData[ns.kvs[i].offset + ns.kvs[i].klen:ns.kvs[i].offset + ns.kvs[i].klen + ns.kvs[i].vlen])
}

func (ns *ns) get(key Key) (Value,error) {
	k,err := ns.find(key);
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
	var buf bytes.Buffer
	ns.delete(key);
	buf.Write(key);
	buf.Write(value);
	ns.kvs = append(ns.kvs,NewKV(ns.kvSize,len(key),len(value)));
	ns.kvSize = ns.kvSize + len(key) + len(value);
	d := buf.Bytes()
	//fmt.Println("Put:",d);
	for _,v := range d {
		ns.kvData = append(ns.kvData,v);
	}
	//fmt.Println("kvData:",string(ns.kvData));
	return nil
}

func (ns *ns) Delete(key []byte) {
	ns.delete(key);
}

func (ns *ns) Has(key []byte) bool {
	return ns.has(key);
}

func (ns *ns) Get(key []byte) (Value,error) {
	return ns.get(key);
}

func (ns *ns) Put(key,value []byte) error {
	return ns.put(key,value);
}

func (ns *ns) Seek(key []byte) Value {
	return Value("key")
}

func NewNs() *ns {return new(ns)}


type MemDB struct{
	nss		[]ns
	nsSize	uint64
}

func (db *MemDB) getNS(parent, ns []byte) uint64 {
	val := db.nss[0].Get(Key(append(bytes.Fields(parent),ns)))
	return 0;
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


