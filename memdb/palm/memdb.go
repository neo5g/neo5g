package memdb

import (
	"bytes"
	"github.com/Workiva/go-datastructures/btree/palm"
	"github.com/Workiva/go-datastructures/common"
	"os"
	"runtime"
)

type iKey []byte

type iKV struct {
	k, v []byte
}

func NewKey(b ...[]byte) *iKey {
	buf := new(bytes.Buffer)
	buf.Write(int2byte(len(b[0])))
	if len(b) == 2 {
		buf.Write(int2byte(len(b[1])))
	}
	buf.Write(b[0])
	if len(b) == 2 {
		buf.Write(b[1])
	}
	ikey := iKey(buf.Bytes())
	return &ikey
}

func (i *iKey) To(key, value []byte) int {
	buf := new(bytes.Buffer)
	buf.Write(int2byte(len(key)))
	buf.Write(int2byte(len(value)))
	buf.Write(key)
	buf.Write(value)
	return buf.Len()
}

func (i *iKey) From() (int, int, []byte, []byte) {
	k := byte2int((*i)[:4])
	v := byte2int((*i)[4:8])
	key := (*i)[8 : 8+k]
	value := (*i)[8+k : 8+k+v]
	return k, v, key, value
}

func (i *iKey) Key() []byte {
	//fmt.Println("Key:",i.iData)
	k := byte2int((*i)[:4])
	return (*i)[8 : 8+k]
}

func (i *iKey) Value() []byte {
	k := byte2int((*i)[:4])
	v := byte2int((*i)[4:8])
	return (*i)[8+k : 8+k+v]
}

func (i iKey) Reset() {
	i = i[:0]
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
	return bytes.Compare(a.Key(), b.(*iKey).Key())
}

type iKeys []iKey

type DB struct {
	tr   palm.BTree
	file *os.File
}

func (db *DB) Open(path string) error {
	return nil
}

func (db *DB) Close() {
}

func (db *DB) load() error {
	return nil
}

func (db *DB) save() error {
	return nil
}

func (db *DB) sync() error {
	return nil
}
// Удаление данных
func (db *DB) Delete(r ...iKV) {
	l := len(r)
	k := make([]common.Comparator, l)
	for i := 0; i < l; i++ {
		d := r[i]
		k[i] = NewKey(d.k, d.v)
	}
	db.tr.Delete(k...)
}

func (db *DB) Get(r ...iKV) common.Comparators {
	l := len(r)
	k := make([]common.Comparator, l)
	for i := 0; i < l; i++ {
		d := r[i]
		k[i] = NewKey(d.k, d.v)
	}
	return db.tr.Get(k...)
}

func (db *DB) Put(r ...iKV) {
	l := len(r)
	k := make([]common.Comparator, l)
	for i := 0; i < l; i++ {
		d := r[i]
		k[i] = NewKey(d.k, d.v)
	}
	db.tr.Insert(k...)
}

type Options struct {
	bufSize uint64
	ary     uint64
}

func defaultOptions() *Options {
	return &Options{bufSize: uint64(os.Getpagesize() >> 12), ary: uint64(runtime.NumCPU() >> 4)}
}

func NewDB(opt *Options) *DB {
	return &DB{tr: palm.New(opt.bufSize, opt.ary)}

}
