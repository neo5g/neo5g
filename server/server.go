package server

import (
    "net"
    "fmt"
    //"bufio"
    //"strings"
    //"golang.org/x/net/context"
    "google.golang.org/grpc"
    //"google.golang.org/grpc/peer"
    "github.com/syndtr/goleveldb/leveldb"
    //pb "github.com/neo5g/neo5g/nqwire"
)

type Neo5gDBServer struct {
    db *leveldb.DB
    }

func (s *neo5GConnectServer) Connect(r Neo5G_ConnectServer) error {
	request,err := r.Recv();
	fmt.Println("Request:",request,err);
	return  r.Send(&Response{Code: 0,Msg:"Ok"});
}

func (s *neo5GConnectServer) Execute(q Neo5G_ExecuteServer) error { 
	query,err := q.Recv();
	fmt.Println("Query:",query,err);
	return q.Send(&Result{Result:0});
}


func (s *Neo5gDBServer) Start(){
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
