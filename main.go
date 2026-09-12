package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Store struct {
	mu sync.RWMutex
	data map[string]string
}

func handleClient(conn net.Conn,store *Store){
	defer conn.Close();
	reader:=bufio.NewReader(conn)
	for {
		args,err := parseCommand(reader)
		if err!=nil{
			fmt.Errorf("Error in parseCommand\n")
			break
		}

		if len(args) <= 0{
			fmt.Errorf("Invalid arg lenght\n")
			break
		}

		cmd:=strings.ToUpper(args[0])
		if cmd == "PING"{
			conn.Write([]byte("+PONG\r\n"))
		}else if cmd == "ECHO"{
			msg:= args[1]
			response := fmt.Sprintf("$%d\r\n%s\r\n",len(msg),msg)
			conn.Write([]byte(response))
		}else if cmd == "SET" {
			key := args[1]
			value := args[2]
			store.mu.Lock()
			store.data[key] = value
			store.mu.Unlock()
			conn.Write([]byte("+OK\r\n"))
		}else if cmd== "GET" {
			key:=args[1]
			store.mu.RLock()
			value, exists := store.data[key]
			store.mu.RUnlock()
			if !exists {
				conn.Write([]byte("$-1\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
			}
	}
}
}

func parseCommand(reader *bufio.Reader) ([]string,error){
	b,err:=reader.ReadByte()
	if err!=nil{
		fmt.Print("error at parse command")
		return nil,err
	}

	if b!= '*'{
		return nil,fmt.Errorf("expected '*' , got %q",b)
	}

	parseString,err := reader.ReadString('\n')
	if err!=nil{
		return nil,err
	}
	countString:=strings.TrimSpace(parseString)

	count,err:=strconv.Atoi(countString)
	if err!=nil{
		return nil,err
	}

	args := make([]string,0,count)
	for i:=0;i < count;i++ {
		line,err:=reader.ReadString('\n')
		if err!=nil{
			return nil,err
		}
		if line[0]!='$'{
			return nil,fmt.Errorf("expected '$', got %q\n",line[0])
		}

		strlen := strings.TrimSpace(line[1:])
		len,err:=strconv.Atoi(strlen)
		if err!=nil{
			return nil,err
		}

		buf:=make([]byte,len)
		_,err = io.ReadFull(reader,buf)
		if err!=nil{
			return nil,err
		}
		
		reader.Discard(2)

		args = append(args, string(buf))
	}


	return args,nil
}
func main(){
	listener,err:= net.Listen("tcp",":6379")
	if err!=nil{
		fmt.Printf("Error while listening: %v\n",err)
		return
	}
	defer listener.Close()
	store:= &Store{data: make(map[string]string)}
	fmt.Println("Server running on :6739.....")
	for {
		conn,err:=listener.Accept()
		if err!=nil{
			fmt.Printf("Accept error: %v\n",err)
			continue
		}
		go handleClient(conn,store)
	}
}