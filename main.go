package main

import (
    "github.com/neo5g/neo5g/server"
)

func main()  {
    srv := new(server.BoltDBServer)
    srv.Start()
}
