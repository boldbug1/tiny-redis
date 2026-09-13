package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	value string
	expiresAt time.Time
}
type Store struct {
	mu sync.RWMutex
	data map[string]Entry
}

type cmd struct {
	name string
	args []string
}

type AOF struct {
	mu sync.Mutex
	file *os.File
}

func (a *AOF) Write(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	_,err := a.file.Write(data)
	return err
}

func formatRESP(args []string) []byte {
    var sb strings.Builder
    sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
    for _, arg := range args {
        sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
    }
    return []byte(sb.String())
}

func handleClient(conn net.Conn,store *Store,aof *AOF){
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
			if len(cmd.args) != 2 && len(cmd.args) !=4{
        		conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
        		continue
    		}
			key := cmd.args[0]
			value := cmd.args[1]
			var expiresAt time.Time
			if len(cmd.args) == 4 {
			if strings.ToUpper(cmd.args[2]) != "EX" {
				conn.Write([]byte("-ERR syntax error\r\n"))
				continue
			}

			seconds, err := strconv.Atoi(cmd.args[3])
			if err != nil || seconds <= 0 {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}

			expiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
			}
			store.mu.Lock()
			store.data[key] = Entry{
				value: value,
				expiresAt: expiresAt,
			}
			store.mu.Unlock()
			_ = aof.Write(formatRESP(args))
			conn.Write([]byte("+OK\r\n"))
		} else if cmd.name == "GET" {
			if len(cmd.args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			key := cmd.args[0]
			store.mu.Lock()
			entry, exists := store.data[key]

			if !exists {
				store.mu.Unlock()
				conn.Write([]byte("$-1\r\n"))
				continue

			}

			if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt){
				delete(store.data,key)
				store.mu.Unlock()
				conn.Write([]byte("$-1\r\n"))
				continue
			}

			store.mu.Unlock()

			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.value), entry.value)))
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

			if deletedCount > 0 {
				_ = aof.Write(formatRESP(args))
			}
			
			conn.Write([]byte(fmt.Sprintf(":%d\r\n",deletedCount)))
	}else if cmd.name == "EXPIRE" {
		if len(cmd.args) != 2 {
			conn.Write([]byte("-ERR wrong number of arguments for 'expire' command\r\n"))
			continue
		}

		key := cmd.args[0]
		seconds,err := strconv.Atoi(cmd.args[1])
		if err!= nil || seconds < 0{
			conn.Write([]byte("-ERR value is not an integer or out of range\r\n")) 
			continue
		}

		expiresAt := time.Now().Add(time.Duration(seconds) * time.Second)

		store.mu.Lock()
		entry,exists := store.data[key]
		if !exists {
			store.mu.Unlock()
			conn.Write([]byte(":0\r\n"))
			continue
		}
		entry.expiresAt = expiresAt
		store.data[key] = entry
		store.mu.Unlock()
		_ = aof.Write(formatRESP(args))
		conn.Write([]byte(":1\r\n"))
	}else if cmd.name == "TTL" {
		if len(cmd.args) != 1 {
			conn.Write([]byte("-ERR wrong number of arguments for 'TTL' command\r\n"))
			continue
		}

		key := cmd.args[0]

		store.mu.RLock()
		entry,exists:= store.data[key]
		store.mu.RUnlock()
		if !exists {
			conn.Write([]byte(":-2\r\n"))
			continue
		}
		expiresAt := entry.expiresAt

		if !expiresAt.IsZero() && time.Now().After(expiresAt) {
			store.mu.Lock()
			delete(store.data, key) 
			store.mu.Unlock()
			conn.Write([]byte(":-2\r\n"))
			continue
		}

		remaining := int64(time.Until(expiresAt).Seconds())
		if remaining < 0 {
			remaining = 0
		}
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", remaining)))	
		} 
}
}

func startActiveExpiration(store *Store) {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for range ticker.C {
        start:= time.Now()
        for {
            store.mu.Lock()

            now := time.Now()
            checked := 0
            deleted := 0

            for key, entry := range store.data {
                if checked >= 20 {
                    break
                }

                if !entry.expiresAt.IsZero() {
                    checked++
                    if now.After(entry.expiresAt) {
                        delete(store.data, key)
                        deleted++
                    }
                }
            }

            store.mu.Unlock()

            if checked == 0 {
                break
            }

            if deleted*4 <= checked {
                break
            }

			if time.Since(start) > 25* time.Millisecond {
				break
			}
        }
    }
}

func parseCommand(reader *bufio.Reader) ([]string,error){
	b,err:=reader.ReadByte()
	if err!=nil{
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

func replayAOF(store *Store, filename string) error {
    file, err := os.Open(filename)
    if os.IsNotExist(err) {
        return nil //nothing to restore
    } else if err != nil {
        return err
    }
    defer file.Close()

    reader := bufio.NewReader(file)
    for {
        args, err := parseCommand(reader)
        if err == io.EOF {
            break //reached end of file
        }
        if err != nil {
            return fmt.Errorf("aof parse error: %w", err)
        }
        if len(args) == 0 {
            continue
        }

        name := strings.ToUpper(args[0])
        cmdArgs := args[1:]

        switch name {
        case "SET":
            if len(cmdArgs) >= 2 {
                key := cmdArgs[0]
                val := cmdArgs[1]
                var exp time.Time
                if len(cmdArgs) == 4 && strings.ToUpper(cmdArgs[2]) == "EX" {
                    secs, _ := strconv.Atoi(cmdArgs[3])
                    exp = time.Now().Add(time.Duration(secs) * time.Second)
                }
                store.data[key] = Entry{value: val, expiresAt: exp}
            }

        case "DEL":
            for _, k := range cmdArgs {
                delete(store.data, k)
            }

        case "EXPIRE":
            if len(cmdArgs) == 2 {
                key := cmdArgs[0]
                if entry, exists := store.data[key]; exists {
                    secs, _ := strconv.Atoi(cmdArgs[1])
                    entry.expiresAt = time.Now().Add(time.Duration(secs) * time.Second)
                    store.data[key] = entry
                }
            }
        }
    }
    return nil
}

func main(){
	file,err := os.OpenFile("appendonly.aof",os.O_CREATE|os.O_WRONLY|os.O_APPEND,0644)
	if err!=nil{
		fmt.Printf("AOF file error: %v\n",err)
		return
	}
	defer file.Close()

	aof := &AOF{file:file}

	listener,err:= net.Listen("tcp",":6379")
	if err!=nil{
		fmt.Printf("Error while listening: %v\n",err)
		return
	}
	defer listener.Close()
	store:= &Store{data: make(map[string]Entry)}
	fmt.Println("Server running on :6379.....")

	if err := replayAOF(store, "appendonly.aof"); err != nil {
        fmt.Printf("AOF replay error: %v\n", err)
    }

	go startActiveExpiration(store)

	for {
		conn,err:=listener.Accept()
		if err!=nil{
			fmt.Printf("Accept error: %v\n",err)
			continue
		}
		go handleClient(conn,store,aof)
	}
}