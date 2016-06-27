package server

import (
    "net"
    "fmt"
    //"bufio"
    //"strings"
    //"golang.org/x/net/context"
    "google.golang.org/grpc"
    //"google.golang.org/grpc/peer"
    "github.com/boltdb/bolt"
    //pb "github.com/neo5g/neo5g/nqwire"
)

type BoltDBServer struct {
    db *bolt.DB
    }

func (s *neo5GConnectServer) Connect(r Neo5G_ConnectServer) error {
	request,err1 := r.Recv();
	fmt.Println("Request:",request,err1);
	err2 := r.Send(&Response{Code: 0,Msg:"Ok"});
	return err2
}

func (s *neo5GConnectServer) Execute(q Neo5G_ExecuteServer) error { 
	query,err1 := q.Recv();
	fmt.Println("Query:",query,err1);
	err2 := q.Send(&Result{Result:1});
	return err2
}


func (s *BoltDBServer) Start(){
    fmt.Println("Start neo4go server port:",7070);
    lis, err :=net.Listen("tcp",":7070");
    if err != nil{
	fmt.Println("Error listen:",err);
	return;
	}
    grpcServer := grpc.NewServer();
    RegisterNeo5GServer(grpcServer,&neo5GConnectServer{});
    grpcServer.Serve(lis);
}
