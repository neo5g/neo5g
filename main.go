package main

import (
    "github.com/NikolayChesnokov/neo4go/server"
)

func main()  {
    srv := new(server.BoltDBServer)
    srv.Start()
}