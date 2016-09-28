package memdb

import (
	"strconv"
	"testing"
	"github.com/Workiva/go-datastructures/tree/avl"
)

const (
	IterCount = 2
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
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
}

func BenchmarkGetOne(b1 *testing.B) {
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	for i, j := 0, len(keys); i < j; i++ {
		db.Get(keys[i])
	}

}
func TestGetOne(t *testing.T) {
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	var e avl.Entries
	for i, j := 0, len(keys); i < j; i++ {
		e = db.Get(keys[i])
		if len(e) == 0 {
			t.Log("Key fail", string(keys[i].k))
		}else{
		    t.Log("Key found:",string(e[0].(*iKey).Key()))
		}
	}
}
func BenchmarkGetMany(b1 *testing.B) {
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
}

/*
func BenchmarkIter(b1 *testing.B) {
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	l := db.tr.Query(NewKey([]byte("abc")), NewKey([]byte("Zzzzzzzzzzzzzzzzzzzz-"+strconv.Itoa(1))))
	for i, j := 0, int(len(l)); i < j; i++ {
		_, _ = l[i].(*iKey).Key(), l[i].(*iKey).Value()
	}
}

func BenchmarkDispose(b1 *testing.B) {
	opt := defaultOptions()
	db := NewDB(opt)
	keys := data(IterCount)
	db.Put(keys...)
	db.tr.Dispose()
}
*/
