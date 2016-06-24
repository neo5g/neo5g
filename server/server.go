package server

import (
    "net"
    "fmt"
    //"bufio"
    //"strings"
    //"golang.org/x/net/context"
    //"google.golang.org/grpc"
    //"google.golang.org/grpc/peer"
    "github.com/boltdb/bolt"
    pb "github.com/NikolayChesnokov/neo4go/nqwire"
)

type BoltDBServer struct {
    db *bolt.DB
    }

type server struct {}

func (s server) Connect(ctx context.Context, in *pb.ConnectRequest) (*pb.ConnectResponse,error){
	return &pb.ConnectResponse{Code: 0,Msg:"Ok"},nil;
}

func (s *BoltDBServer) Start(){
    fmt.Println("Start neo4go server port:",7070);
    lis, err :=net.Listen("tcp",":7070");
    if err != nil{
	fmt.Println("Error listen:",err);
	return;
	}
    srv := grpc.NewServer();
    pb.RegisterConnectServiceServer(srv, &server{});
    srv.Serve(lis);
}
