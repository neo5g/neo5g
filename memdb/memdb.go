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
	"strconv"
	"sync"
	"time"
)

const (
	defaultnodesPeerNamespace = 1 << 16
	defaultfillPercent        = 0.7
)

type Options struct {
	nodesPeerNamespace uint32
	fillPercent        float64
}

type iKV struct {
	offset int
	len    int
}

type node struct {
	Comparator    func(a, b []byte) int
	index         []int
	keys          []iKV
	vals          []iKV
	reserveds     []int
	keysData      []byte
	valsData      []byte
	isChanged     bool
	keysSize      int
	valsSize      int
	reservedsSize int
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
	//fmt.Println("L",l)
	if l > 0 {
		idx := n.index[i]
		k := n.keys[idx]
		v := n.vals[idx]
		n.keysSize -= k.len
		if l == 1 {
			n.keys = n.keys[:0]
			n.vals = n.vals[:0]
			n.reserveds = n.reserveds[:0]
			n.keysData = n.keysData[:0]
			n.valsData = n.valsData[:0]
			n.valsSize = 0
			n.reservedsSize = 0
		} else {
			if l-idx == 1 {
				n.keys = n.keys[:idx]
				n.vals = n.vals[:idx]
				n.reserveds = n.reserveds[:idx]
				n.keysData = n.keysData[:idx]
				n.valsData = n.valsData[:idx]
				n.valsSize -= v.len
				n.reservedsSize -= n.reserveds[idx-1]
			} else {
				copy(n.keysData[k.offset:], n.keysData[:k.offset+k.len])
				copy(n.valsData[v.offset:], n.valsData[:v.offset+v.len])
				copy(n.keys[idx:], n.keys[idx:])
				copy(n.vals[idx:], n.vals[idx:])
				copy(n.reserveds[idx:], n.reserveds[idx:])
				copy(n.index[i:], n.index[:i])
				n.valsSize -= v.len
				n.reservedsSize -= n.reserveds[idx-1]
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

func (n *node) SortedInsert(s []int, f int) []int {
	l := len(s)
	if l == 0 {
		return append(s, f)
	}

	i := sort.Search(l, func(i int) bool { return n.compare(n.getKey(i), n.getKey(int(f))) >= 0 })
	if i == l { // not found = new value is the smallest
		return append(append(make([]int, 0), f), s...)
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

func NewKV(offset, len int) iKV {
	return iKV{offset, len}
}

func (n *node) put(key, value []byte) error {
	i, err := n.find(key)
	if err == nil {
		err = n.putValue(i, value)
		//fmt.Println("node.put.putValue:err", err)
		if err != nil {
			//fmt.Println("node.put:Delete key", key)
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
	//fmt.Println("Before:key,value",n.index,n.keys,n.vals,n.reserveds,n.reservedsSize,string(n.keysData),string(n.valsData));
	n.index = n.SortedInsert(n.index, len(n.keys))
	n.keys = append(n.keys, NewKV(n.keysSize, len(key)))
	n.vals = append(n.vals, NewKV(n.valsSize+n.reservedsSize, len(value)))
	reserveds := int((1.0 - 0.7) * float64(len(value)))
	n.reserveds = append(n.reserveds, reserveds)
	n.keysData = append(n.keysData, key...)
	n.valsData = append(n.valsData, value...)
	n.valsData = append(n.valsData, make([]byte, reserveds)...)
	n.keysSize += len(key)
	n.valsSize += len(value)
	n.reservedsSize += reserveds
	//fmt.Println("After:key,value",n.index,n.keys,n.vals,n.reserveds,n.reservedsSize,string(n.keysData),string(n.valsData));
	return nil
}

func (n *node) putValue(i int, value []byte) error {
	v := n.vals[n.index[i]]
	reserveds := n.reserveds[n.index[i]]

	//fmt.Println("Before:putValue", string(value),string(n.valsData[v.offset:v.offset+v.len]),len(value),v.len,reserveds)
	if len(value) <= v.len+reserveds {
		copy(n.valsData[v.offset:], value)
		n.reserveds[n.index[i]] = v.len + reserveds - len(value)
		v.len = len(value)
		n.vals[n.index[i]] = v
		//fmt.Println("After:putValue", string(n.valsData[v.offset:v.offset+v.len]),len(value),v.len, n.reserveds[n.index[i]])
		return nil
	}
	//fmt.Println("<putValue:No space>")
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
	if len(n.index) > 0 {
		s := sort.Search(len(n.index), func(i int) bool {
			return n.compare(n.getKey(i), key) >= 0
		})
		if s < len(n.keys) && ns.compare(n.getKey(s), key) == 0 {
			return h, s, nil
		}
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
	//fmt.Println("Put-j i err",j,i,err)
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
		if err != nil {
			ns.nodes[j].deleteKey(i)
			//fmt.Println("Put else err",err)
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

type dbIter struct {
	ns    *ns
	i     int
	j     int
	n     [][3]int
	key   int
	value int
	err   error
}

/*
func (i dbIter) sumNodes(){
	n := uint64(0)
	for i := 0,j := len(i.ns.nodes); i < j; i++ {
	i += len(i.ns.nodes[i].index);
	}
	return n
}
*/
func NewdbIter(ns *ns) *dbIter {
	dbIter := new(dbIter)
	dbIter.ns = ns
	dbIter.i = 0
	for i, j := 0, len(ns.nodes); i < j; i++ {
		l := len(ns.nodes[i].index)
		if l > 0 {
			dbIter.n = append(dbIter.n, [3]int{i, 0, l})
		}
	}
	dbIter.j = len(dbIter.n)
	return dbIter
}

func (i *dbIter) First() bool {
	fmt.Println("First:", i.i, i.j)
	if i.j > 0 {
		i.i = 0
		n := i.n[i.i]
		for k := 0; k < i.j; k++ {
			i.n[k][1] = 0
		}
		fmt.Println("First:node", n, i.ns.nodes[n[0]].index[n[1]])
		i.key = i.ns.nodes[n[0]].index[n[1]]
		i.value = i.ns.nodes[n[0]].index[n[1]]
		return true
	}
	return false
}

func (i *dbIter) Prev() bool {
	if i.i > 0 && i.j > i.i {
		i.i--
		return true
	}
	return false
}

func (i *dbIter) Next() bool {
	n := &i.n[i.i]
	n[1]++
	fmt.Println("Next-0:", i.i, i.j, n)
	if n[1] < n[2] {
		i.key = i.ns.nodes[n[0]].index[n[1]]
		i.value = i.ns.nodes[n[0]].index[n[1]]
		return true
	} else {
		i.i++
		if i.i < i.j {
			n := &i.n[i.i]
			fmt.Println("Next-1:", i.i, i.j, n)
			i.key = i.ns.nodes[n[0]].index[n[1]]
			i.value = i.ns.nodes[n[0]].index[n[1]]
			return true
		}
	}
	i.err = errors.New("<Next:Last record>")
	return false

}

func (i *dbIter) Last() bool {
	if i.j > 0 {
		i.i = len(i.n) - 1
		idx := i.n[i.i][1]
		i.key = i.ns.nodes[i.i].index[idx]
		i.value = i.ns.nodes[i.i].index[idx]
		return true
	}
	return false
}

func (i *dbIter) Key() []byte {
	return i.ns.nodes[i.n[i.i][0]].getKey(i.key)
}

func (i *dbIter) Value() []byte {
	return i.ns.nodes[i.n[i.i][0]].getValue(i.value)
}

func (i *dbIter) Validate() bool {
	fmt.Println("Validate:", i.err, i.err == nil)
	return i.err == nil
}

type MemDB struct {
	path          string
	rootNamespace *ns
	namespaces    [](*ns)
	dbSize        uint64
	options       Options
}

func (db *MemDB) Create(path string, opt Options) *MemDB {
	//if opt {
	//DefaultOptions := Options{nodesPeerNamespace:defaultnodesPeerNamespace, fillPercent:defaultfiellPercent}
	//}
	return &MemDB{path: path, options: opt}
}

func (db *MemDB) getNS(parent, namespace []byte) int64 {
	val, err := db.rootNamespace.Get(append(parent, namespace...))
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
	val, err := db.rootNamespace.Get(append(parent, namespace...))
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
		n.Put([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-"+strconv.Itoa(i)), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+strconv.Itoa(i)))
	}
	for i := 1000; i >= 0; i-- {
		n.Put([]byte("Jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj-"+strconv.Itoa(i)), []byte("Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-"+strconv.Itoa(i)))
	}
	for i := 1000; i >= 0; i-- {
		n.Put([]byte("Kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk-"+strconv.Itoa(i)), []byte("Yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-"+strconv.Itoa(i)))
	}
	//n.Put([]byte("abc2"), []byte("1234+2babc"))
	n.Put([]byte("abc"), []byte("1234"))
	n.Put([]byte("abc1"), []byte("1234+1"))
	n.Put([]byte("abc85"), []byte("1234+85"))
	n.Put([]byte("abc3"), []byte("1234+3"))
	n.Put([]byte("abc7"), []byte("1234+7"))
	tp1 := time.Now()
	fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
	//sort.Sort(&ByKey{*n});
	v, err := n.Get([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-" + strconv.Itoa(521)))
	fmt.Println("V:", string(v), err)
	n.Delete([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-" + strconv.Itoa(521)))
	v, err = n.Get([]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-" + strconv.Itoa(521) ))
	fmt.Println("V:", string(v), err)
	//v, err = n.Get([]byte("abc2"))
	//fmt.Println("V:", string(v), err)
	n.Put([]byte("abc2"), []byte("1234+2wwwwwwwwwwwwwwwwww"))
	v, err = n.Get([]byte("abc2"))
	fmt.Println("V:", string(v), err)
	n.Put([]byte("abc2"), []byte("1234+2"))
	v, err = n.Get([]byte("abc2"))
	fmt.Println("V:", string(v), err)
	iter := NewdbIter(n)
	for iter.First(); iter.Validate(); iter.Next() {
		fmt.Println("Key,Value", string(iter.Key()), string(iter.Value()))
	}
}
