package leveldb

type NS struct {
    id uint64
    name string
    parent uint64
    root pgid
}

type pgid struct {
    root map[int]uint64
    nodes []node
    }

type node struct {
    root []uint64    
}