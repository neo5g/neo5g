package memdb

import (
	//"fmt"
	"strconv"
	"testing"
)

const (
	IterCount = 3333
)

type iB struct {
	k,v []byte
	}

func data(c int) []iB {
	keys := make([]iB, 0, c*3)
	for i := 0; i < c; i++ {
		s := strconv.Itoa(i)
		a, b := []byte("Iiiiiiiiiiii-"+s), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+s)
		keys = append(keys, iB{k:a, v:b})
		a, b = []byte("Jjjjjjjjjjjjj-"+s), []byte("Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-"+s)
		keys = append(keys, iB{k:a, v:b})
		a, b = []byte("Kkkkkkkkkkkkk-"+s), []byte("Yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-"+s)
		keys = append(keys, iB{k:a, v:b})
	}
	return keys
}

func TestGet(t *testing.T) {
	n := NewNs()
	keys := data(IterCount)
	var err error
	var r iB
	for i, _ := range keys {
		r = keys[i]
		n.Put(r.k, r.v)
	}
	for _, r := range keys {
		 _,err = n.Get(r.k)
		 if err != nil {
			 t.Log("key fail:",string(r.k))
			 //t.Fail()
			 }
	}

}


func BenchmarkPut(b *testing.B) {
	b.StopTimer()
	n := NewNs()
	keys := data(IterCount)
	b.StartTimer()
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
}

func BenchmarkDelete(b *testing.B) {
	b.StopTimer()
	n := NewNs()
	keys := data(IterCount)
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
	b.StartTimer()
	for _, r := range keys {
		n.Delete(r.k)
	}

}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()
	n := NewNs()
	keys := data(IterCount)
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
	b.StartTimer()
	for _, r := range keys {
		n.Get(r.k)
	}

}


func BenchmarkReplace(b *testing.B) {
	b.StopTimer()
	n := NewNs()
	keys := data(333)
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
	b.StartTimer()
	for _, r := range keys {
		r.v = r.v[:16]
		n.Put(r.k, r.v)
	}

}


func TestIter(t *testing.T) {
	n := NewNs()
	keys := data(333)
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
	iter := NewdbIter(n)
	for iter.First(); iter.Validate(); iter.Next() {
		_,_ = iter.Key(),iter.Value()
		//fmt.Println("Key,Value", string(iter.Key()), string(iter.Value()))
	}
}

