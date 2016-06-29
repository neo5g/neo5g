package main

import (
    "github.com/neo5g/neo5g/server"
)

func main()  {
    srv := new(server.Neo5gDBServer)
    srv.Start()
}
