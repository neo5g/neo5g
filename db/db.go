package db

import (
	"os"
	//"io"
	"fmt"
) 

type DB struct {
	file *os.File
	pageSize int
	ops struct {
		WriteAt func(b []byte, off int64) (n int, err error)
	}
}
type Container interface {
	Open(path string) error;
}

type Object struct {
	Container
	name string
	inode Node
}

type Node struct {
}

func (db *DB) Open(name string) bool {
	file,err := os.OpenFile(name,os.O_RDWR|os.O_CREAT,0600);
	if err != nil {
		fmt.Println("neo5g::db:",err);
		return false;
		}
	db.file = file
	db.ops.WriteAt = db.file.WriteAt
	db.pageSize = os.Getpagesize();
	return true
}
func (db *DB) init() error {
	var buf [0X1000]byte;
	db.file.WriteAt (buf[:],0)
	return nil;
}

