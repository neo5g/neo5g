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

type iKV struct {
	offset uint64
	len    uint64
}

type node struct {
	Comparator func(a, b []byte) int
	index      []uint64
	keys       []iKV
	vals       []iKV
	keysData   []byte
	valsData   []byte
	isChanged  bool
	keysSize   uint64
	valsSize   uint64
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
	l := len(n.keys)
	fmt.Println("L",l)
	if l > 0 {
		idx := n.index[i]
		k := n.keys[idx]
		v := n.vals[idx]
		if l == 1 {
			n.keys = n.keys[:0]
			n.vals = n.vals[:0]
			n.keysData = n.keysData[:0]
			n.valsData = n.valsData[:0]
		} else {
			if uint64(l)-idx == 1 {
				n.keys = n.keys[:idx]
				n.vals = n.vals[:idx]
				n.keysData = n.keysData[:idx]
				n.valsData = n.valsData[:idx]
			} else {
				copy(n.keysData[k.offset:], n.keysData[:k.offset+k.len])
				copy(n.valsData[v.offset:], n.valsData[:v.offset+v.len])
				copy(n.keys[idx:], n.keys[idx:])
				copy(n.vals[idx:], n.vals[idx:])
				copy(n.index[i:], n.index[:i])
			}

		}
	}
	// index compress
	if i == 0 {
		n.index = n.index[:0]
	} else {
		if l-i == 1 {
			n.index = n.index[:i]
		} else {
			copy(n.index[i:], n.index[i:])
		}
	}
}

func (n *node) find(key []byte) (int, error) {
	s := sort.Search(len(n.index), func(i int) bool {
		return n.compare(n.getKey(i), key) >= 0
	})
	if s < len(n.keys) && n.compare(n.getKey(s), key) == 0 {
		return s, nil
	}
	return -1, errors.New("<node.find:Not found>")
}

func (n *node) getKey(i int) []byte {
	r := n.keys[n.index[i]]
	k := n.keysData[r.offset : r.offset+r.len]
	return k
}

func (n *node) getValue(i int) []byte {
	v := n.keys[n.index[i]]
	return n.valsData[v.offset : v.offset+v.len]
}

func (n *node) get(key []byte) ([]byte, error) {
	k, err := n.find(key)
	if err != nil {
		return nil, err
	}
	return n.getValue(k), nil
}

func (n *node) has(key []byte) bool {
	_, err := n.find(key)
	return err == nil
}

func (n *node) SortedInsert(s []uint64, f uint64) []uint64 {
	l := len(s)
	if l == 0 {
		return append(s, f)
	}

	i := sort.Search(l, func(i int) bool { return n.compare(n.getKey(i), n.getKey(int(f))) >= 0 })
	if i == l { // not found = new value is the smallest
		return append(append(make([]uint64, 0), f), s...)
	}
	if i == l-1 { // new value is the biggest
		return append(s[0:l], f)
	}
	return append(append(s[0:l], f), s[l+1:]...)
}

/*
func SortedInsert (s []Mytype, f Mytype) []Mytype {
    l:=len(s)
    if l==0 { return [f] }

    i := sort.Search(l, func(i int) bool { return s[i].Less(f)})
    if i==l {  // not found = new value is the smallest
        return append([f],s)
    }
    if i==l-1 { // new value is the biggest
        return append(s[0:l],f)
    }
    return append(s[0:l],f,s[l+1:])
}
*/

func NewKV(offset, len uint64) iKV {
	return iKV{offset, len}
}

func (n *node) put(key, value []byte) error {
	i, err := n.find(key)
	if err == nil {
		err = n.putValue(i, value)
		fmt.Println("node.put.putValue:err", err)
		if err != nil {
			fmt.Println("node.put:Delete key", key)
			n.delete(key)
			err = n.putKeyValue(key, value)
			if err == nil {
				n.isChanged = true
			}
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
	n.index = n.SortedInsert(n.index, uint64(len(n.keys)))
	n.keys = append(n.keys, NewKV(n.keysSize, uint64(len(key))))
	n.vals = append(n.vals, NewKV(n.valsSize, uint64(len(value))))
	n.keysData = append(n.keysData, key...)
	n.valsData = append(n.valsData, value...)
	n.keysSize = n.keysSize + uint64(len(key))
	n.valsSize = n.valsSize + uint64(len(value))
	return nil
}

func (n *node) putValue(i int, value []byte) error {
	v := n.vals[n.index[i]]
	fmt.Println("putValue", string(value))
	if uint64(len(value)) <= v.len {
		copy(n.valsData[v.offset+v.len:], value)
		v.len = uint64(len(value))
		n.vals[n.index[i]] = v
		return nil
	}
	fmt.Println("<putValue:No space>")
	return errors.New("<putValue:No space>")

}

/*
func (n *node) sortByKey_1() {
	t0 := time.Now()
	sort.Sort(&ByKey{*n})
	t1 := time.Now()
	fmt.Printf("The call sorting %v to run.\n", t1.Sub(t0))
}

func (n *node) Len() int { return len(n.index) }

func (n *node) Swap(i, j int) { n.index[i], n.index[j] = n.index[j], n.index[i] }

type ByKey struct{ node }

func (n *ByKey) Less(i, j int) bool {
	o1 := n.keys[n.index[i]]
	o2 := n.keys[n.index[j]]
	k1 := n.keysData[o1.offset : o1.offset+o1.len]
	k2 := n.keysData[o2.offset : o2.offset+o2.len]
	return n.compare(k2, k1) >= 0
}
*/
type ns struct {
	mu         sync.RWMutex
	Comparator func(a, b []byte) int
	hash       hash.Hash32
	nodes      []node
	maxNodes   uint64
}

func (ns *ns) compare(key1, key2 []byte) int {
	return ns.Comparator([]byte(key1), []byte(key2))
}

func (ns *ns) delete(key []byte) {
	j, i, err := ns.find(key)
	if err == nil {
		ns.deleteKey(j, i)
	}
}

func (ns *ns) deleteKey(j, i int) {
	ns.nodes[j].deleteKey(i)
}

func (ns *ns) find(key []byte) (int, int, error) {
	h := ns.getHashKey(key)
	if h > len(ns.nodes) {
		return h, -1, errors.New("<ns.find:Not found>")
	}
	n := ns.nodes[h]
	s := sort.Search(len(n.index), func(i int) bool {
		return n.compare(n.getKey(i), key) >= 0
	})
	if s < len(n.keys) && ns.compare(n.getKey(s), key) == 0 {
		return h, s, nil
	}
	return h, -1, errors.New("<ns.find:Not found>")
}

func (ns *ns) getHashKey(key []byte) int {
	ns.hash.Reset()
	ns.hash.Write(key)
	return int(ns.hash.Sum32() % uint32(ns.maxNodes))
}

func (ns *ns) getKey(j, i int) []byte {
	n := ns.nodes[j]
	k := n.keys[n.index[i]]
	return n.keysData[k.offset : k.offset+k.len]
}

func (ns *ns) getValue(j, i int) []byte {
	n := ns.nodes[j]
	v := n.vals[n.index[i]]
	return n.valsData[v.offset : v.offset+v.len]
}

func (ns *ns) get(key []byte) ([]byte, error) {
	j, k, err := ns.find(key)
	if err != nil {
		return nil, err
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
	fmt.Println("Put-j i err",j,i,err)
	if err != nil {
		if i > 0 {
			err = ns.nodes[j].putValue(i, value)
			if err != nil {
				ns.nodes[j].deleteKey(i)
				return ns.nodes[j].putKeyValue(key, value)
			}
		} else {
			ns.nodes[j] = NewNode()
			return ns.nodes[j].put(key, value)
		}
	} else {
		err = ns.nodes[j].putValue(i, value)
		fmt.Println("Put else err",err)
		if err != nil {
			ns.nodes[j].deleteKey(i)
			fmt.Println("Put else err",err)
			return ns.nodes[j].putKeyValue(key, value)
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
	nss    [](*ns)
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
	tp0 := time.Now()
	for i := 1000; i >= 0; i-- {
		//n.Put([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-"+string(i)), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+string(i)))
	}
	for i := 1000; i >= 0; i-- {
		//n.Put([]byte("Jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj-"+string(i)), []byte("Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-"+string(i)))
	}
	for i := 1000; i >= 0; i-- {
		//n.Put([]byte("Kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk-"+string(i)), []byte("Yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-"+string(i)))
	}
	n.Put([]byte("abc2"), []byte("1234+2babc"))
	n.Put([]byte("abc"), []byte("1234"))
	n.Put([]byte("abc1"), []byte("1234+1"))
	n.Put([]byte("abc85"), []byte("1234+85"))
	n.Put([]byte("abc3"), []byte("1234+3"))
	n.Put([]byte("abc7"), []byte("1234+7"))
	tp1 := time.Now()
	fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
	//sort.Sort(&ByKey{*n});
	v, err := n.Get([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-" + string(521)))
	fmt.Println("V:", string(v), err)
	n.Delete([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-" + string(521)))
	v, err = n.Get([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-" + string(521)))
	fmt.Println("V:", string(v), err)
	v, err = n.Get([]byte("abc2"))
	fmt.Println("V:", string(v), err)
	n.Put([]byte("abc2"), []byte("1234+2wwwwwwwwwwwwwwwwww"))
	v, err = n.Get([]byte("abc2"))
	fmt.Println("V:", string(v), err)
}
