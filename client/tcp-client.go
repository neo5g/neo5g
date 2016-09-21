package main

import (
	"bufio"
	"fmt"
	cl "github.com/neo5g/neo5g/server"
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
	"os"
)

func (w *ClientConn) Writer() int {
    return 0
}

func main() {
	// connect to this socket
	conn, err := grpc.Dial("127.0.0.1:7070", grpc.WithInsecure())
	if err != nil {
		fmt.Println("Error", err)
	}
	client := cl.NewNeo5GClient(conn)
	stream, err1 := client.Connect(context.TODO())
	err1 = stream.Send(&cl.Request{Dsn: "host=localhost port=7070 dbname=gsrp user=admin password=admin", Host: "localhost", Port: 7070})
	if err1 != nil {
		fmt.Println(err1)
		return
	}
	responce, err2 := stream.Recv()
	if err2 != nil {
		fmt.Println("Responce error:", err2)
		return
	}
	fmt.Println("Responce:", responce)
	
	  for {
	    // read in input from stdin
	    reader := bufio.NewReader(os.Stdin)
	    fmt.Print("Text to send: ")
	    text, _ := reader.ReadString('\n')
	    // send to socket
	    fmt.Fprintf(conn,text + "\n")
	    // listen for reply
	    message, _ := bufio.NewReader(conn).ReadString('\n')
	    fmt.Print("Message from server: "+message)
	  }
	
}
