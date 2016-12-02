package memdb

import (
	"strconv"
	"testing"
)

const (
	IterCount = 3333
)

func data(r int) []iKV {
	keys := make([]iKV, 0, r*3)
	for i := 0; i < r; i++ {
		s := strconv.Itoa(i)
		a, b := []byte("Iiiiiiiiiiii-"+s), []byte("Vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-"+s)
		keys = append(keys, iKV{a, b})
		a, b = []byte("Jjjjjjjjjjjjj-"+s), []byte("Xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-"+s)
		keys = append(keys, iKV{a, b})
		a, b = []byte("Kkkkkkkkkkkkk-"+s), []byte("Yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-"+s)
		keys = append(keys, iKV{a, b})
	}
	return keys
}

func BenchmarkInsert(b1 *testing.B) {
	b1.StopTimer()
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	b1.StartTimer()
	db.Put(keys...)
}

func BenchmarkGetOne(b1 *testing.B) {
	b1.StopTimer()
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	b1.StartTimer()
	for i, j := 0, len(keys); i < j; i++ {
		db.Get(keys[i])
	}
}
func BenchmarkDeleteOne(b1 *testing.B) {
	b1.StopTimer()
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	b1.StartTimer()
	for i, j := 0, len(keys); i < j; i++ {
		db.Delete(keys[i])
	}
}

func BenchmarkGetMany(b1 *testing.B) {
	b1.StopTimer()
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	b1.StartTimer()
	db.Get(keys...)
}
func BenchmarkDeleteMany(b1 *testing.B) {
	b1.StopTimer()
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	b1.StartTimer()
	db.Delete(keys...)
}

func BenchmarkIter(b1 *testing.B) {
	b1.StopTimer()
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	l := db.tr.Query(NewKey([]byte("abc")), NewKey([]byte("Zzzzzzzzzzzzzzzzzzzz-"+strconv.Itoa(1))))
	b1.StartTimer()
	for i, j := 0, int(len(l)); i < j; i++ {
		_, _ = l[i].(*iKey).Key(), l[i].(*iKey).Value()
	}
}

func BenchmarkDispose(b1 *testing.B) {
	b1.StopTimer()
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	b1.StartTimer()
	db.tr.Dispose()
}
