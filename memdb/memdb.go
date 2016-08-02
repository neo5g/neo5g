package main

//package memdb

import (
	"bytes"
	//"encoding/binary"
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
	heap   int
	offset int
	len    int
}

type iData struct {
	ikv            []iKV
	iReserveds     []int
	iByte          []byte
	isChanged      bool
	iSize          int
	iReservedsSize int
}

type iNode struct {
	Comparator    func(a, b []byte) int
	pos	int
	index         []int
	keys          []*iKV
	vals          []*iKV
	reserveds     []int
	keysData      []byte
	valsData      []byte
	isChanged     bool
	keysSize      int
	valsSize      int
	reservedsSize int
}

func (n *iNode) compare(key1, key2 *[]byte) int {
	return n.Comparator(*key1, *key2)
}

func (n *iNode) delete(key *[]byte) {
	i, err := n.find(key)
	if err == nil {
		n.deleteKey(i)
	}
}

func (n *iNode) deleteKey(i int) {
	if n.pos >= 0 {
		fmt.Println("deleteKey:before",i,n.pos,n.index,n.keys,n.vals,n.reserveds,n.keysSize,n.valsSize,n.reservedsSize);
		idx := n.index[i]
		k := n.keys[idx]
		v := n.vals[idx]
		copy(n.index,n.index[:i]);
		copy(n.keys,n.keys[:idx]);
		copy(n.vals,n.vals[:idx]);
		copy(n.reserveds,n.reserveds[:idx]);
		copy(n.keysData,n.keysData[:idx]);
		copy(n.valsData,n.valsData[:idx]);
		if i < n.pos { 
			copy(n.index[i:],n.index[i:]);
			for start := idx; start < n.pos;start++{ 
				n.keys[start].offset -= k.len
				n.vals[start].offset -= v.len
				}
			copy(n.keys[idx:],n.keys[idx:]);
			copy(n.vals[idx:],n.vals[idx:]);
			copy(n.reserveds[idx:],n.reserveds[idx:]);
			copy(n.keysData[idx:],n.keysData[idx:]);
			copy(n.valsData[idx:],n.valsData[idx:]);
			}
		
		n.keysSize -= k.len
		n.valsSize -= v.len
		n.reservedsSize -= n.reserveds[n.pos]
		n.pos--
		fmt.Println("deleteKey:after",i,n.pos,n.index,n.keys,n.vals,n.reserveds,n.keysSize,n.valsSize,n.reservedsSize);
	}
}
func (n *iNode) find(key *[]byte) (int, error) {
	s := sort.Search(n.pos, func(i int) bool {
		return n.compare(n.getKey(i), key) >= 0
	})
	//fmt.Println("FIND:",s,"\nlen(n.keys)",cap(n.keys),"\nindex",n.index,"\nkeys",n.keys,"\nvals",n.vals);
	//fmt.Println("FIND:",s,"\nn.pos",n.pos,"\nindex",n.index,"\nkeys",n.keys[n.pos],"\nvals",n.vals[n.pos]);
	if s < n.pos && n.keys[s] != nil && n.compare(n.getKey(s), key) == 0 {
		return s, nil
	}
	return -1, errors.New("<node.find:Not found>")
}

func (n *iNode) getKey(i int) *[]byte {
	r := n.keys[n.index[i]]
	k := n.keysData[r.offset : r.offset+r.len]
	return &k
}

func (n *iNode) getValue(i int) *[]byte {
	r := n.keys[n.index[i]]
	v := n.valsData[r.offset : r.offset+r.len]
	return &v
}

func (n *iNode) get(key *[]byte) (*[]byte, error) {
	k, err := n.find(key)
	if err != nil {
		return nil, err
	}
	return n.getValue(k), nil
}

func (n *iNode) has(key *[]byte) bool {
	_, err := n.find(key)
	return err == nil
}

func (n *iNode) SortedInsert(s *[]int, f int) *[]int {
	if n.pos == 0 {
		s1 := append(*s, f)
		return &s1
	}

	i := sort.Search(n.pos, func(i int) bool { return n.compare(n.getKey(i), n.getKey(int(f))) >= 0 })
	if i == n.pos { // not found = new value is the smallest
		s1 := append(append(make([]int,0), f), *s...)
		return &s1
	}
	if i == n.pos-1 { // new value is the biggest
		s1 := append((*s)[0:n.pos], f)
		return &s1
	}
	s1 := append(append((*s)[0:n.pos], f), (*s)[n.pos+1:]...)
	return &s1
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

func NewKV(offset, len int) *iKV {
	return &iKV{offset: offset, len: len}
}

func (n *iNode) put(key, value *[]byte) error {
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

func (n *iNode) putKeyValue(key, value *[]byte) error {
	n.pos++
	if n.pos > 0 {
		//fmt.Println("Before:key,value",n.pos,n.index[n.pos],n.keys[n.pos],n.vals[n.pos],n.reserveds[n.pos]);
	}
	n.index = *n.SortedInsert(&n.index, n.pos)
	if n.pos >= cap(n.keys){
		n.keys =append(n.keys,make([]*iKV,1<<2)...) 
		}
	if n.pos >= cap(n.vals){
		n.vals =append(n.vals,make([]*iKV,1<<2)...) 
		}
	fmt.Println("Index:",n.index,n.keys,n.pos);
	if n.keys[n.pos] == nil { 
		n.keys[n.pos] = NewKV(n.keysSize, len(*key))
		}
	if n.vals[n.pos] == nil { 
		n.vals[n.pos] = NewKV(n.valsSize+n.reservedsSize, len(*value))
		}
	if n.pos >= cap(n.reserveds){ 
		n.reserveds = append(n.reserveds,make([]int,1<<2)...);
		}
	if n.keysSize + len(*key) >= cap(n.keysData) { 
		n.keysData = append(n.keysData,make([]byte,1<<6)...);
		}
	fmt.Println("Index1:",n.index,n.keys,n.pos);
	copy(n.keysData[n.keysSize:],*key) 
	reserveds := int((1.0 - 0.7) * float64(len(*value)))
	
	n.reserveds[n.pos] = reserveds
	if n.valsSize + len(*value) + n.reservedsSize >= cap(n.valsData) { 
		n.valsData = append(n.valsData,make([]byte,1<<6)...);
		}
	fmt.Println("COPY:",n.valsSize,cap(n.valsData),len(n.valsData));
	copy(n.valsData[n.valsSize:],*value);
	copy(n.valsData[n.valsSize+len(*value):],make([]byte, reserveds));
	n.keysSize += len(*key)
	n.valsSize += len(*value)
	n.reservedsSize += reserveds
	//fmt.Println("After:key,value",n.pos,n.index[n.pos],n.keys[n.pos],n.vals[n.pos],n.reserveds[n.pos]);
	fmt.Println("Size:",n.keysSize,n.valsSize,n.reservedsSize);
	return nil
}

func (n *iNode) putValue(i int, value *[]byte) error {
	v := n.vals[n.index[i]]
	reserveds := n.reserveds[n.index[i]]

	//fmt.Println("Before:putValue", string(value),string(n.valsData[v.offset:v.offset+v.len]),len(value),v.len,reserveds)
	if len(*value) <= v.len+reserveds {
		copy(n.valsData[v.offset:], *value)
		n.reserveds[n.index[i]] = v.len + reserveds - len(*value)
		v.len = len(*value)
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
type iNS struct {
	mu         sync.RWMutex
	Comparator func(a, b []byte) int
	hash       hash.Hash32
	nodes      []*iNode
	maxNodes   uint64
}

func (ns *iNS) compare(key1, key2 *[]byte) int {
	return ns.Comparator(*key1, *key2)
}

func (ns *iNS) delete(key *[]byte) {
	j, i, err := ns.find(key)
	if err == nil {
		ns.deleteKey(j, i)
	}
}

func (ns *iNS) deleteKey(j, i int) {
	ns.nodes[j].deleteKey(i)
}

func (ns *iNS) find(key *[]byte) (int, int, error) {
	h := ns.getHashKey(key)
	if h > len(ns.nodes) {
		return h, -1, errors.New("<ns.find:Not found>")
	}
	n := ns.nodes[h]
	if n == nil {
		ns.nodes[h] = NewNode()
		n = ns.nodes[h]
	}
	//fmt.Println("N:", n, h)
	if len(n.index) > 0 {
		s := sort.Search(n.pos, func(i int) bool {
			return n.compare(n.getKey(i), key) >= 0
		})
		if s < n.pos && ns.compare(n.getKey(s), key) == 0 {
			return h, s, nil
		}
	}
	return h, -1, errors.New("<ns.find:Not found>")
}

func (ns *iNS) getHashKey(key *[]byte) int {
	ns.hash.Reset()
	ns.hash.Write(*key)
	return int(ns.hash.Sum32() % uint32(ns.maxNodes))
}

func (ns *iNS) getKey(j, i int) *[]byte {
	n := ns.nodes[j]
	k := n.keys[n.index[i]]
	kd := n.keysData[k.offset : k.offset+k.len]
	return &kd
}

func (ns *iNS) getValue(j, i int) *[]byte {
	n := ns.nodes[j]
	v := n.vals[n.index[i]]
	vd := n.valsData[v.offset : v.offset+v.len]
	return &vd
}

func (ns *iNS) get(key *[]byte) (*[]byte, error) {
	j, k, err := ns.find(key)
	if err != nil {
		return nil, err
	}
	return ns.getValue(j, k), nil
}

func (ns *iNS) has(key *[]byte) bool {
	_, _, err := ns.find(key)
	return err == nil
}

func NewNode() *iNode {
	return &iNode{Comparator: bytes.Compare,keys:make([]*iKV,1<<2),vals:make([]*iKV,1<<2),keysData:make([]byte,1<<2),valsData:make([]byte,1<<6),reserveds:make([]int,1<<2),index:make([]int,1<<2),pos:-1}
}

func (ns *iNS) put(key, value *[]byte) error {
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

func (ns *iNS) Delete(key *[]byte) {
	ns.delete(key)
}

func (ns *iNS) Has(key *[]byte) bool {
	return ns.has(key)
}

func (ns *iNS) Get(key *[]byte) (*[]byte, error) {
	return ns.get(key)
}

func (ns *iNS) Put(key, value *[]byte) error {
	return ns.put(key, value)
}

//func (ns *iNS) Seek(key []byte) []*byte {
//	return []*byte(&"key")
//}

func NewNs() *iNS { return new(iNS) }

type dbIter struct {
	ns    *iNS
	i     int
	j     int
	n     [][3]int
	key   int
	value int
	err   error
}

func NewdbIter(ns *iNS) *dbIter {
	dbIter := new(dbIter)
	dbIter.ns = ns
	dbIter.i = 0
	for i, j := 0, len(ns.nodes); i < j; i++ {
		n := ns.nodes[i]
		if n == nil {
			continue
		}
		l := len(n.index)
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
	n := i.n[i.i]
	n[1]++
	//fmt.Println("Next-0:", i.i, i.j, n)
	if n[1] < n[2] {
		i.key = i.ns.nodes[n[0]].index[n[1]]
		i.value = i.ns.nodes[n[0]].index[n[1]]
		return true
	} else {
		i.i++
		if i.i < i.j {
			n := i.n[i.i]
			//fmt.Println("Next-1:", i.i, i.j, n)
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

func (i *dbIter) Key() *[]byte {
	return i.ns.nodes[i.n[i.i][0]].getKey(i.key)
}

func (i *dbIter) Value() *[]byte {
	return i.ns.nodes[i.n[i.i][0]].getValue(i.value)
}

func (i *dbIter) Validate() bool {
	//fmt.Println("Validate:", i.err, i.err == nil)
	return i.err == nil
}

type MemDB struct {
	path          string
	rootNamespace *iNS
	namespaces    [](*iNS)
	dbSize        uint64
	options       Options
}

func (db *MemDB) Create(path string, opt Options) *MemDB {
	//if opt {
	//DefaultOptions := Options{nodesPeerNamespace:defaultnodesPeerNamespace, fillPercent:defaultfiellPercent}
	//}
	return &MemDB{path: path, options: opt}
}
/*
func (db *MemDB) getNS(parent, namespace *[]byte) int64 {
	val, err := db.rootNamespace.Get(append(*parent, (*namespace)...))
	if err != nil {
		return -1
	}
	v, c := binary.Varint(*val)
	if c > 0 {
		return v
	}
	return -1
}

func (db *MemDB) putNS(parent, namespace, value *[]byte) int64 {
	val, err := db.rootNamespace.Get(append(*parent, *namespace...))
	if err != nil {
		return -1
	}
	v, c := binary.Varint(*val)
	if c > 0 {
		return v
	}
	return -1
}
*/
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
	n.maxNodes = 1 << 12
	n.nodes = make([]*iNode, n.maxNodes)
	n.hash = fnv.New32a()
	tp0 := time.Now()
	for i := 2; i >= 0; i-- {
		a,b :=[]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-"+strconv.Itoa(i)), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+strconv.Itoa(i))
		n.Put(&a,&b);
	}
	for i := 2; i >= 0; i-- {
		a,b := []byte("Jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj-"+strconv.Itoa(i)), []byte("Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-"+strconv.Itoa(i))
		n.Put(&a,&b);
	}
	for i := 2; i >= 0; i-- {
		a,b := []byte("Kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk-"+strconv.Itoa(i)), []byte("Yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-"+strconv.Itoa(i))
		n.Put(&a,&b);
	}
	m := make([][2]string,0);
	 m = append(m, [2]string{"abc2", "1234+2babc"})
	 m= append(m,[2]string{"abc2","1234_12babc"})
	m= append(m,[2]string{"abc","1234"})
	m= append(m,[2]string{"abc85","1234+85"})
	m= append(m,[2]string{"abc3","1234+3"})
	m= append(m,[2]string{"abc7","1234+7"})
	for i := range m {
	a:=[]byte(m[i][0])
	b:=[]byte(m[i][1])
	n.Put(&a,&b);
	} 
	tp1 := time.Now()
	fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
	//sort.Sort(&ByKey{*n});
	k:=[]byte("Iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-" + strconv.Itoa(1))
	v, err := n.Get(&k)
	fmt.Println("V:", v, err)
	n.Delete(&k)
	v, err = n.Get(&k)
	fmt.Println("V:", v, err)
	//v, err = n.Get([]byte("abc2"))
	//fmt.Println("V:", string(v), err)
	a,b:=[]byte("abc2"), []byte("1234+2wwwwwwwwwwwwwwwwww")
	n.Put(&a,&b)
	v, err = n.Get(&a)
	fmt.Println("V:", v, err)
	a,b = []byte("abc2"), []byte("1234+2")
	n.Put(&a,&b)
	v, err = n.Get(&a)
	fmt.Println("V:", v, err)
	iter := NewdbIter(n)
	for iter.First(); iter.Validate(); iter.Next() {
		//fmt.Println("Key,Value", string(iter.Key()), string(iter.Value()))
	}
}
