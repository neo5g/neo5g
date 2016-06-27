package main

import (
    "fmt"
    grpc "google.golang.org/grpc"
    context "golang.org/x/net/context"
	cl "github.com/neo5g/neo5g/server"
	)

func main() {
  // connect to this socket
  conn, err1 := grpc.Dial("127.0.0.1:7070",grpc.WithInsecure())
  if err1 !=nil {fmt.Println("Error",err1);}
  client := cl.NewNeo5GClient(conn);
  stream,err := client.Connect(context.TODO());
  err = stream.Send(&cl.Request{Dsn:"DSN"});
  if err != nil {fmt.Println(err)};
  return;
  /*
  for {
    // read in input from stdin
    reader := bufio.NewReader(os.Stdin)
    fmt.Print("Text to send: ")
    text, _ := reader.ReadString('\n')
    // send to socket
    fmt.Fprintf(conn, text + "\n")
    // listen for reply
    message, _ := bufio.NewReader(conn).ReadString('\n')
    fmt.Print("Message from server: "+message)
  }
  */
}
