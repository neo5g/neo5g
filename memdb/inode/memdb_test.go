package memdb

import (
	//"fmt"
	"strconv"
	"testing"
)

const (
	IterCount = 333
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
	n := NewNs()
	keys := data(IterCount)
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
}

func BenchmarkDelete(b *testing.B) {
	n := NewNs()
	keys := data(IterCount)
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
	for _, r := range keys {
		n.Delete(r.k)
	}

}

func BenchmarkGet(b *testing.B) {
	n := NewNs()
	keys := data(IterCount)
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
	for _, r := range keys {
		n.Delete(r.k)
	}

}


func BenchmarkReplace(b *testing.B) {
	n := NewNs()
	keys := data(333)
	for _, r := range keys {
		n.Put(r.k, r.v)
	}
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

/*
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
*/
