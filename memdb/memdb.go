package main

//package memdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

type kv struct {
	offset int
	klen   int
	vlen   int
}

type node struct {
	Comparator func(a, b []byte) int
	index      []int
	kvs        []kv
	kvData     []byte
	isChanged  bool
	nodesSize  int
}

func (n *node) compare(key1, key2 []byte) int {
	return n.Comparator(key1, key2)
}

func (n *node) delete(key []byte) {
	i, err := n.find(key)
	if err == nil {
		n.deleteKey(i)
	}
}

func (n *node) deleteKey(i int) {
	idx := n.index[i]
	r := n.kvs[idx]
	if len(n.index) > 0 {
		if len(n.index) > 1 {
			copy(n.kvData[:r.offset], n.kvData[r.offset+r.klen+r.vlen:])
			copy(n.kvs[:idx], n.kvs[idx:])
			copy(n.index[:i], n.index[:i])
		} else {
			n.truncate()
		}
	}
}

func (n *node) find(key []byte) (int, error) {
	if n.isChanged {
		n.sortByKey()
		n.isChanged = false
	}
	s := sort.Search(len(n.index), func(i int) bool {
		//fmt.Println("Bytes compare:",string(key),string(n.getKey(i)),bytes.Compare(key,n.getKey(i)) == 0);
		return n.compare(n.getKey(i), key) >= 0
	})
	//fmt.Println("Search:",string(key),len(n.kvs),s);
	if s < len(n.kvs) && n.compare(n.getKey(s), key) == 0 {
		return s, nil
	}
	return -1, errors.New("<node:Not found>")
}

func (n *node) getKey(i int) []byte {
	r := n.kvs[n.index[i]]
	k := n.kvData[r.offset : r.offset+r.klen]
	//fmt.Println("getKey:",i,string(k));
	return k
}

func (n *node) getValue(i int) []byte {
	//fmt.Println("getValue:",i,n.kvs[i]);
	r := n.kvs[n.index[i]]
	return n.kvData[r.offset+r.klen : r.offset+r.klen+r.vlen]
}

func (n *node) get(key []byte) ([]byte, error) {
	k, err := n.find(key)
	//fmt.Println("get:",k,err);
	if err != nil {
		return nil, err
	}
	return n.getValue(k), nil
}

func (n *node) has(key []byte) bool {
	_, err := n.find(key)
	return err == nil
}

func NewKV(offset, lkey, lval int) kv {
	return kv{offset, lkey, lval}
}

func (n *node) put(key, value []byte) error {
	i, err := n.find(key)
	if err == nil {
		err = n.putValue(i, value)
		if err != nil {
			n.delete(key)
		}
		err = n.putKeyValue(key, value)
		if err == nil {
			n.isChanged = true
		}
	} else {
		err = n.putKeyValue(key, value)
		if err == nil {
			n.isChanged = true
		}
	}
	return err
}

func (n *node) putKeyValue(key, value []byte) error {
	n.index = append(n.index, len(n.kvs))
	n.kvs = append(n.kvs, NewKV(n.nodesSize, len(key), len(value)))
	n.kvData = append(n.kvData, key...)
	n.kvData = append(n.kvData, value...)
	n.nodesSize = n.nodesSize + len(key) + len(value)
	return nil
}

func (n *node) putValue(i int, value []byte) error {
	r := n.kvs[n.index[i]]
	if len(value) <= r.vlen {
		copy(n.kvData[r.offset+r.klen:], value)
		r.vlen = len(value)
		n.kvs[n.index[i]] = r
		return nil
	}
	return errors.New("putValue:No space")

}

func (n *node) sortByKey() {
	t0 := time.Now()
	sort.Sort(&ByKey{*n})
	t1 := time.Now()
	fmt.Printf("The call sorting %v to run.\n", t1.Sub(t0))
}

func (n *node) truncate() {
	n.kvData = n.kvData[:0]
	n.kvs = n.kvs[:0]
	n.index = n.index[:0]
}

func (n *node) Len() int { return len(n.index) }

func (n *node) Swap(i, j int) { n.index[i], n.index[j] = n.index[j], n.index[i] }

type ByKey struct{ node }

func (n *ByKey) Less(i, j int) bool {
	o1 := n.kvs[n.index[i]]
	o2 := n.kvs[n.index[j]]
	k1 := n.kvData[o1.offset : o1.offset+o1.klen]
	k2 := n.kvData[o2.offset : o2.offset+o2.klen]
	return n.compare(k2, k1) >= 0
}

type ns struct {
	mu         sync.RWMutex
	Comparator func(a, b []byte) int
	hash       hash.Hash32
	nodes      []node
	maxNodes   int
}

func (ns *ns) compare(key1, key2 []byte) int {
	return ns.Comparator([]byte(key1), []byte(key2))
}

func (ns *ns) delete(key []byte) {
	j, i, err := ns.find(key)
	//fmt.Println("Delete:",j,i,err);
	if err == nil {
		ns.deleteKey(j, i)
	}
}

func (ns *ns) deleteKey(j, i int) {
	//fmt.Println("J,I:",j,i);
	ns.nodes[j].deleteKey(i)
}

func (ns *ns) find(key []byte) (int, int, error) {
	h := ns.getHashKey(key)
	if h > len(ns.nodes) {
		return h, -1, errors.New("<find:Not found>")
	}
	n := ns.nodes[h]
	if n.isChanged {
		n.sortByKey()
		n.isChanged = false
	}
	s := sort.Search(len(n.index), func(i int) bool {
		//fmt.Println("Bytes compare:",string(key),string(ns.getKey(i)),bytes.Compare([]byte(key),[]byte(ns.getKey(i))) == 0);
		return n.compare(n.getKey(i), key) >= 0
	})
	//fmt.Println("Search:",string(key),len(n.kvs),s);
	if s < len(n.kvs) && ns.compare(n.getKey(s), key) == 0 {
		return h, s, nil
	}
	return h, -1, errors.New("<Not found>")
}

func (ns *ns) getHashKey(key []byte) int {
	ns.hash.Reset()
	ns.hash.Write(key)
	return int(ns.hash.Sum32() % uint32(ns.maxNodes))
}

func (ns *ns) getKey(j, i int) []byte {
	n := ns.nodes[j]
	r := n.kvs[n.index[i]]
	k := n.kvData[r.offset : r.offset+r.klen]
	//fmt.Println("getKey:",i,string(k));
	return k
}

func (ns *ns) getValue(j, i int) []byte {
	n := ns.nodes[j]
	r := n.kvs[n.index[i]]
	k := n.kvData[r.offset : r.offset+r.klen : r.offset+r.klen+r.vlen]
	//fmt.Println("getKey:",i,string(k));
	return k
}

func (ns *ns) get(key []byte) ([]byte, error) {
	j, k, err := ns.find(key)
	//fmt.Println("get:",j,k,err);
	if err != nil {
		return nil, err
	}
	n := ns.nodes[j]
	if n.isChanged {
		n.sortByKey()
		n.isChanged = false
	}
	return ns.getValue(j, k), nil
}

func (ns *ns) has(key []byte) bool {
	_, _, err := ns.find(key)
	return err == nil
}

func NewNode() node {
	return node{Comparator: bytes.Compare}
}

func (ns *ns) put(key, value []byte) error {
	j, i, err := ns.find(key)
	if err != nil {
		if i > 0 {
			return ns.nodes[j].putValue(i, value)
		} else {
			ns.nodes[j] = NewNode()
			//fmt.Println("Nodes",ns.nodes[j]);
			return ns.nodes[j].put(key, value)
		}

	}
	return nil
}

func (ns *ns) Delete(key []byte) {
	ns.delete(key)
}

func (ns *ns) Has(key []byte) bool {
	return ns.has(key)
}

func (ns *ns) Get(key []byte) ([]byte, error) {
	return ns.get(key)
}

func (ns *ns) Put(key, value []byte) error {
	return ns.put(key, value)
}

func (ns *ns) Seek(key []byte) []byte {
	return []byte("key")
}

func NewNs() *ns { return new(ns) }

type MemDB struct {
	nss    []ns
	nsSize uint64
}

func (db *MemDB) getNS(parent, namespace []byte) int64 {
	val, err := db.nss[0].Get(append(parent, namespace...))
	if err != nil {
		return -1
	}
	v, c := binary.Varint(val)
	if c > 0 {
		return v
	}
	return -1
}

func (db *MemDB) putNS(parent, namespace, value []byte) int64 {
	val, err := db.nss[0].Get(append(parent, namespace...))
	if err != nil {
		return -1
	}
	v, c := binary.Varint(val)
	if c > 0 {
		return v
	}
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
func main() {

	n := NewNs()
	//n := ns{}
	n.Comparator = bytes.Compare
	n.maxNodes = 1 << 16
	n.nodes = make([]node, n.maxNodes)
	n.hash = fnv.New32a()
	n.Put([]byte("abc2"), []byte("1234+2babc"))
	n.Put([]byte("abc"), []byte("1234"))
	n.Put([]byte("abc1"), []byte("1234+1"))
	n.Put([]byte("abc85"), []byte("1234+85"))
	n.Put([]byte("abc3"), []byte("1234+3"))
	n.Put([]byte("abc7"), []byte("1234+7"))

	tp0 := time.Now()
	for i := 3; i >= 0; i-- {
		n.Put([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-"+string(i)), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+string(i)))
	}
	tp1 := time.Now()
	fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
	//sort.Sort(&ByKey{*n});
	v, err := n.Get([]byte("abc2"))
	fmt.Println("V:", string(v), err)
	//fmt.Println(string(n.kvData))
	n.Delete([]byte("abc2"))
	//fmt.Println(string(n.kvData))
	v, err = n.Get([]byte("abc2"))
	fmt.Println("V:", string(v), err)
	//fmt.Println("INDEX:",n.index)
}
