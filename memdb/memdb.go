package main

//package memdb

import (
	"bytes"
	//"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	//"os"
	"sort"
	"strconv"
	"sync"
	"time"
	//"github.com/willf/bloom"
	"github.com/tylertreat/BoomFilters"
	//"launchpad.net/gommap"
	//"github.com/edsrzf/mmap-go"
)

const (
	defaultAllocStepIndex     = 1 << 16
	defaultAllocStepKeys      = 1 << 16
	defaultAllocStepVals      = 1 << 16
	defaultAllocStepReserveds = 1 << 16
	defaultAllocStepKeysData  = 1 << 20
	defaultAllocStepValsData  = 1 << 20
	defaultNodesPeerNamespace = 1 << 12
	defaultFillPercent        = 0.7
	defaultBFOptsM            = 20
	defaultBFOptsK            = 5
	defaultBFOptsN            = 1000000
	defaultBFOptsRate         = 0.1
)

type BFOptions struct {
	m uint
	k uint
	n uint
	p float64
}

type Options struct {
	AllocStepIndex     uint32
	AllocStepKeys      uint32
	AllocStepVals      uint32
	AllocStepReserveds uint32
	AllocStepKeysData  uint32
	AllocStepValsData  uint32
	NodesPeerNamespace uint32
	FillPercent        float64
	BF                 *boom.StableBloomFilter
	BFOpts             BFOptions
}


type iKV struct {
	k,v,r int
	}

type iNode struct {
	Comparator func(a, b []byte) int
	BF         *boom.StableBloomFilter
	// [] offset in iData
	iIndex      []int
	// [0]uint32 key length
	// [1]uint32 value length
	// [2]uint32 value reserved length
	iData      []byte
	isChanged     bool
}

func DefaultOptions() Options {
	return Options{
		AllocStepIndex:     defaultAllocStepIndex,
		AllocStepKeys:      defaultAllocStepKeys,
		AllocStepVals:      defaultAllocStepVals,
		AllocStepReserveds: defaultAllocStepReserveds,
		AllocStepKeysData:  defaultAllocStepKeysData,
		AllocStepValsData:  defaultAllocStepValsData,
		NodesPeerNamespace: defaultNodesPeerNamespace,
		FillPercent:        defaultFillPercent,
		BFOpts:             BFOptions{m: defaultBFOptsM, k: defaultBFOptsK, n: defaultBFOptsN, p: defaultBFOptsRate},
	}
}

func (ikv *iKV) write(buf []byte) {
	ikv.k = byte2int(buf[:4])
	ikv.v = byte2int(buf[4:8])
	ikv.r = byte2int(buf[8:12])
}

func (ikv *iKV) read() []byte {
	buf := new(bytes.Buffer)
	buf.Write(int2byte(ikv.k))
	buf.Write(int2byte(ikv.v))
	buf.Write(int2byte(ikv.r))
	return buf.Bytes()
}

func byte2int(b []byte) int {
	return int(b[0]) | int(b[1]) << 8 | int(b[2]) << 15 | int(b[3]) << 24
	}

func int2byte(i int) []byte {
	b := make([]byte,4)
	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[2] = byte(i >> 16)
	b[3] = byte(i >> 24)
	return b
	}

func (n *iNode) compare(key1, key2 []byte) int {
	return n.Comparator(key1, key2)
}

func (n *iNode) delete(key []byte) {
	i, err := n.find(key)
	if err == nil {
		n.deleteKey(i)
	}
}

func (n *iNode) deleteKey(i int) {
	offset := n.iIndex[i]
	copy(n.iIndex, n.iIndex[:i])
	copy(n.iData,n.iData[:offset])	
	if i < len(n.iIndex)-1 {
		next := n.iIndex[i+1]
		copy(n.iIndex[i:], n.iIndex[:i])
		copy(n.iData[offset:],n.iData[next:])	
		delta := next - offset
		newlen := len(n.iIndex)-1
		for j,k := i,newlen; j < k; j++ {
			n.iIndex[j] -= delta 
			}
		n.iIndex = n.iIndex[:newlen]
		n.iData = n.iData[:len(n.iData)-delta]
		} else {
			n.iIndex = n.iIndex[:i]
			n.iData = n.iData[:offset]			
			}
		
	}

func (n *iNode) find(key []byte) (int, error) {
	if !n.BF.Test(key) {
		return 0, errors.New("*iNode.find.BloomFilter:<Key not found>")
	}
	s := sort.Search(len(n.iIndex), func(i int) bool {
		return n.compare(n.getKey(i), key) >= 0
	})
	//fmt.Println("Find:",s,len(n.iIndex))
	if s < len(n.iIndex) && n.compare(n.getKey(s), key) == 0 {
		return s, nil
	}
	return -1, errors.New("*iNode.find:<Key not found>")
}

func (n *iNode) getKey(i int) []byte {
	//fmt.Println("GetKey:",i,n.iIndex,n.iData)
	offset := n.iIndex[i]
	meta := n.iData[offset:offset+12]
	ikv := new(iKV)
	ikv.write(meta)
	k := n.iData[offset+12 : offset+12+ikv.k]
	//fmt.Println("Key:",k,offset,ikv,meta,n.iData)
	return k
}

func (n *iNode) getValue(i int) []byte {
	offset := n.iIndex[i]
	meta := n.iData[offset:offset+12]
	ikv := new(iKV)
	ikv.write(meta)
	v := n.iData[offset+12+ikv.k:offset+12+ikv.k+ikv.v]
	//fmt.Println("Value:",v)
	return v
}

func (n *iNode) get(key []byte) ([]byte, error) {
	k, err := n.find(key)
	if err != nil {
		return nil, err
	}
	return n.getValue(k), nil
}

func (n *iNode) has(key []byte) bool {
	_, err := n.find(key)
	return err == nil
}

func (n *iNode) SortedInsert(f int) {
	l := len(n.iIndex)
	//fmt.Println("Sorted insert:before",n.iIndex,f,l)
	if l == 0 {
		n.iIndex= append(n.iIndex, f)
	l = len(n.iIndex)
	//fmt.Println("Sorted insert:after",n.iIndex,f,l)

		return
	}

	i := sort.Search(l, func(i int) bool { return n.compare(n.getKey(i), n.getKey(int(f))) >= 0 })
	if i == l { // not found = new value is the smallest
		n.iIndex = append(append(make([]int, int(0)), f), n.iIndex...)
	l = len(n.iIndex)
	//fmt.Println("Sorted insert:after",n.iIndex,f,l)

		return
	}
	if i == l-1 { // new value is the biggest
		n.iIndex = append(n.iIndex, f)
	l = len(n.iIndex)
	//fmt.Println("Sorted insert:after",n.iIndex,f,l)
		return
	}
	n.iIndex = append(append(n.iIndex[0:i], f), n.iIndex[i:]...)
	l = len(n.iIndex)
	//fmt.Println("Sorted insert:after",n.iIndex,f,l)
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

func NewKV(k, v, r int) *iKV {
	return &iKV{k:k, v:v,r:r}
}

func (n *iNode) put(key, value []byte) error {
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

func (n *iNode) putKeyValue(key, value []byte) error {
	offset := len(n.iData)
	n.SortedInsert(offset)
	k,v := len(key),len(value)
	d := NewKV(k,v,int(float64(v)*float64(1.0-0.7)))
	buf := new(bytes.Buffer)
	buf.Write(d.read())
	buf.Write(key)
	buf.Write(value)
	buf.Write(make([]byte,d.r))
	n.iData = append(n.iData,buf.Bytes()...)
	n.BF.TestAndAdd(key)
	return nil
}

func (n *iNode) putValue(i int, value []byte) error {
	offset := n.iIndex[i]
	ikv := new(iKV)
	ikv.write(n.iData[offset:offset+12])
	l := len(value)
	if ikv.v+ikv.r >= l {
		ikv.r -= l-ikv.v 
		ikv.v = l
		buf := new(bytes.Buffer)		
		buf.Write(ikv.read())
		copy(n.iData[offset:],buf.Bytes())
		buf.Reset()
		copy(n.iData[offset+12+ikv.k:],value)
		copy(n.iData[offset+12+ikv.k+ikv.v:],make([]byte,ikv.r))
		return nil
		}	
	return errors.New("putValue:<No value free space>")

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
	opt        Options
	BF         *boom.StableBloomFilter
	mu         sync.RWMutex
	Comparator func(a, b []byte) int
	hash       hash.Hash32
	nodes      []*iNode
	maxNodes   uint
}

func (ns *iNS) compare(key1, key2 *[]byte) int {
	return ns.Comparator(*key1, *key2)
}

func (ns *iNS) delete(key []byte) {
	j, i, err := ns.find(key)
	if err == nil {
		ns.deleteKey(j, i)
	}
}

func (ns *iNS) deleteKey(j, i int) {
	ns.nodes[j].deleteKey(i)
}

func (ns *iNS) find(key []byte) (int, int, error) {
	h := 0
	if ns.maxNodes > 1 {
		h = ns.getHashKey(key)
	}

	//fmt.Println("H:",h,len(ns.nodes));
	if h > len(ns.nodes) {
		return h, -1, errors.New("<ns.find:Not found>")
	}
	n := ns.nodes[h]
	if n == nil {
		ns.nodes[h] = NewNode(h, ns.opt)
		n = ns.nodes[h]
	}
	//fmt.Println("N:", n, h)
	//fmt.Println("I:",len(n.index));
	kn, err := n.find(key)
	return h, kn, err
}

func (ns *iNS) getHashKey(key []byte) int {
	ns.hash.Reset()
	ns.hash.Write(key)
	return int(ns.hash.Sum32() % uint32(ns.maxNodes))
}

func (ns *iNS) getKey(j, i int) []byte {
	n := ns.nodes[j]
	offset := n.iIndex[i]
	ikv := new(iKV)
	ikv.write(n.iData[offset:offset+12])
	return n.iData[offset+12:offset+12+ikv.k]
}

func (ns *iNS) getValue(j, i int) []byte {
	n := ns.nodes[j]
	offset := n.iIndex[i]
	ikv := new(iKV)
	ikv.write(n.iData[offset:offset+12])
	//fmt.Println("getValue:",*ikv,n.iData)	
	return n.iData[offset+12+ikv.k:offset+12+ikv.k+ikv.v]
}

func (ns *iNS) get(key []byte) ([]byte, error) {
	j, k, err := ns.find(key)
	if err != nil {
		return nil, err
	}
	return ns.getValue(j, k), nil
}

func (ns *iNS) has(key []byte) bool {
	_, _, err := ns.find(key)
	return err == nil
}

func NewNode(nn int, opt Options) *iNode {
	/*
		fdKeysData,errk := os.OpenFile("key-"+strconv.Itoa(nn)+".sst",os.O_RDWR|os.O_CREATE,0666)
		if errk != nil {
			fmt.Println("ERRK:",errk)
			}
		_,errsk := fdKeysData.Seek(int64(opt.AllocStepKeysData),0)
		fdKeysData.Write([]byte(" "))
		if errsk != nil {
			fmt.Println("ERRSK:",errsk)
			}
		fdValsData,errv := os.OpenFile("val-"+strconv.Itoa(nn)+".sst",os.O_RDWR|os.O_CREATE,0666)
		if errv != nil {
			fmt.Println("ERRV:",errv)
			}
		_,errsv := fdValsData.Seek(int64(opt.AllocStepKeysData),0)
		fdValsData.Write([]byte(" "))
		if errsv != nil {
			fmt.Println("ERRSV:",errsv)
			}

		//mk,errmk := gommap.Map(fdKeysData.Fd(),gommap.PROT_READ|gommap.PROT_WRITE,gommap.MAP_PRIVATE)
		mk,errmk := mmap.Map(fdKeysData,mmap.RDWR,0)
		if errmk != nil {
			fmt.Println("ERRMK",errmk)
			}
		//mv,errmv := gommap.Map(fdValsData.Fd(),gommap.PROT_READ|gommap.PROT_WRITE,gommap.MAP_PRIVATE)
		mv,errmv := mmap.Map(fdValsData,mmap.RDWR,0)
		if errmv != nil {
			fmt.Println("ERRMV",errmv)
			}
		//fmt.Println("LEN:",len(mk),len(mv))

		//return &iNode{Comparator: bytes.Compare, keys: make([]*iKV, opt.AllocStepKeys), vals: make([]*iKV, opt.AllocStepVals), keysData: mk, valsData: mv, reserveds: make([]int, opt.AllocStepReserveds), index: make([]int, opt.AllocStepIndex), pos: -1,BF:bloom.New(bloom.EstimateParameters(opt.BFOpts.n,opt.BFOpts.p))}
	*/
	return &iNode{Comparator: bytes.Compare, BF: boom.NewDefaultStableBloomFilter(10000, 0.01)}
}

func (ns *iNS) put(key, value []byte) error {
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
			ns.nodes[j] = NewNode(j, ns.opt)
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

func (ns *iNS) Delete(key []byte) {
	ns.delete(key)
}

func (ns *iNS) Has(key []byte) bool {
	return ns.has(key)
}

func (ns *iNS) Get(key []byte) ([]byte, error) {
	return ns.get(key)
}

func (ns *iNS) Put(key, value []byte) error {
	return ns.put(key, value)
}

//func (ns *iNS) Seek(key []byte) []*byte {
//	return []*byte(&"key")
//}

func NewNs() *iNS { return &iNS{opt: DefaultOptions()} }

type dbIter struct {
	ns    *iNS
	current     int
	lenght   int
	nodes    [][3]int
	key   []byte
	value []byte
	err   error
}

func NewdbIter(ns *iNS) *dbIter {
	dbIter := new(dbIter)
	dbIter.ns = ns
	dbIter.current = 0
	for i, j := 0, len(ns.nodes); i < j; i++ {
		n := ns.nodes[i]
		if n == nil {
			continue
		}
		if l := len(n.iIndex); l > 0 {
			dbIter.nodes = append(dbIter.nodes, [3]int{i, 0, l-1})
		}
	}
	dbIter.lenght = len(dbIter.nodes)
	return dbIter
}

func (i *dbIter) First() bool {
	fmt.Println("First:", i.current, i.lenght)
	if i.lenght > 0 {
		i.current = 0
		n := i.nodes[i.current]
		for k := 0; k < i.lenght; k++ {
			i.nodes[k][1] = 0
		}
		fmt.Println("First:node", i.nodes, i.nodes[0])
		ikv := new(iKV)
		offset := i.ns.nodes[n[0]].iIndex[0]
		ikv.write(i.ns.nodes[n[0]].iData[offset:offset+12])
		i.key = i.ns.nodes[n[0]].iData[offset+12:offset+12+ikv.k]
		i.value = i.ns.nodes[n[0]].iData[offset+12+ikv.k:offset+12+ikv.k+ikv.v]

		return true
	}
	return false
}

func (i *dbIter) Prev() bool {
	n := &i.nodes[i.current]
	if n[1] > 0 {
		n[1]--
		} else {
			if i.current > 0 {
				i.current--
				n = &i.nodes[i.current]
				} else {
					i.err = errors.New("Prev:<First record>")
					return false
					}
			}
	ikv := new(iKV)
	offset := i.ns.nodes[n[0]].iIndex[n[1]]
	ikv.write(i.ns.nodes[n[0]].iData[offset:offset+12])
	i.key = i.ns.nodes[n[0]].iData[offset+12:offset+12+ikv.k]
	i.value = i.ns.nodes[n[0]].iData[offset+12+ikv.k:offset+12+ikv.k+ikv.v]
	return true
}

func (i *dbIter) Next() bool {
	n := &i.nodes[i.current]
	if n[1] < n[2] {
		n[1]++
		} else {
			if i.current < i.lenght - 1 {
				i.current++
				n = &i.nodes[i.current]
				} else {
					i.err = errors.New("Next:<Last record>")
					return false
					}
			}
	ikv := new(iKV)
	offset := i.ns.nodes[n[0]].iIndex[n[1]]
	ikv.write(i.ns.nodes[n[0]].iData[offset:offset+12])
	i.key = i.ns.nodes[n[0]].iData[offset+12:offset+12+ikv.k]
	i.value = i.ns.nodes[n[0]].iData[offset+12+ikv.k:offset+12+ikv.k+ikv.v]
	return true
}

func (i *dbIter) Last() bool {
	if i.lenght > 0 {
		i.current = len(i.nodes) - 1
		n := &i.nodes[i.current]
		for k := 0; k < i.lenght; k++ {
			i.nodes[k][1] = i.nodes[k][2]
		}		
		ikv := new(iKV)
		offset := i.ns.nodes[n[0]].iIndex[n[1]]
		ikv.write(i.ns.nodes[n[0]].iData[offset:offset+12])
		i.key = i.ns.nodes[n[0]].iData[offset+12:offset+12+ikv.k]
		i.value = i.ns.nodes[n[0]].iData[offset+12+ikv.k:offset+12+ikv.k+ikv.v]
		return true
	}
	i.err = errors.New("Last:<Iterator is empty>")
	return false
}

func (i *dbIter) Key() []byte {
	return i.key
}

func (i *dbIter) Value() []byte {
	return i.value
}

func (i *dbIter) Validate() bool {
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

	rc := 3
	n := NewNs()
	//n := ns{}
	n.Comparator = bytes.Compare
	n.maxNodes = 1 << 4
	n.nodes = make([]*iNode, n.maxNodes)
	n.hash = fnv.New32a()
	//n.BF = bloomf.New(10);
	tp0 := time.Now()
	for i := 0; i < rc; i++ {
		s := strconv.Itoa(i)
		a, b := []byte("Iiiiiiiiiiii-"+s), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+s)
		n.Put(a, b)
		a, b = []byte("Jjjjjjjjjjjjj-"+s), []byte("Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-"+s)
		n.Put(a, b)
		a, b = []byte("Kkkkkkkkkkkkk-"+s), []byte("Yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-"+s)
		n.Put(a, b)
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
		n.Put(a, b)
	}
	tp1 := time.Now()
	fmt.Printf("The call puted %v to run.\n", tp1.Sub(tp0))
	//sort.Sort(&ByKey{*n});
	k := []byte("Zzzzzzzzzzzzzzzzzzzz-" + strconv.Itoa(1))
	v, err := n.Get(k)
	fmt.Println("V:k", v, err)
	n.Delete(k)
	v, err = n.Get(k)
	fmt.Println("V:kd", v, err)
	//v, err = n.Get([]byte("abc2"))
	//fmt.Println("V:", string(v), err)
	a, b := []byte("abc2"), []byte("1234+2wwwwwwwwwwwwwwwwww")
	n.Put(a, b)
	v, err = n.Get(a)
	fmt.Println("V:a", v, err)
	a, b = []byte("abc2"), []byte("1234+23")
	n.Put(a, b)
	v, err = n.Get(a)
	fmt.Println("V:a", v, err)
	iter := NewdbIter(n)
	for iter.First(); iter.Validate(); iter.Next() {
		fmt.Println("Key,Value", string(iter.Key()), string(iter.Value()))
	}
}
