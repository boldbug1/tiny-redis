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

type cmd struct {
	name string
	args []string
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

		cmd:= cmd{
			name: strings.ToUpper(args[0]),
			args: args[1:],
		}
		if cmd.name == "PING"{
			conn.Write([]byte("+PONG\r\n"))
		}else if cmd.name == "ECHO"{
			msg:= cmd.args[0]
			response := fmt.Sprintf("$%d\r\n%s\r\n",len(msg),msg)
			conn.Write([]byte(response))
		}else if cmd.name == "SET" {
			if len(cmd.args) != 2 {
        		conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
        		continue
    		}
			key := cmd.args[0]
			value := cmd.args[1]
			store.mu.Lock()
			store.data[key] = value
			store.mu.Unlock()
			conn.Write([]byte("+OK\r\n"))
		} else if cmd.name == "GET" {
			if len(cmd.args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			key := cmd.args[0]
			store.mu.RLock()
			value, exists := store.data[key]
			store.mu.RUnlock()

			if !exists {
				conn.Write([]byte("$-1\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
			}
		}else if cmd.name == "DEL" {
			if len(cmd.args) < 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
				continue
			}

			deletedCount := 0

			store.mu.Lock()
			for _,key := range cmd.args {
				if _,exists:=store.data[key];exists{
					delete(store.data,key)
					deletedCount++
				}
			}
			store.mu.Unlock()
			
			conn.Write([]byte(fmt.Sprintf(":%d\r\n",deletedCount)))
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