package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Entry struct {
	value     string
	expiresAt time.Time
}
type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

type cmd struct {
	name string
	args []string
}

type AOF struct {
	mu   sync.Mutex
	file *os.File
}

func (a *AOF) Write(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := a.file.Write(data)
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

func handleClient(conn net.Conn, store *Store, aof *AOF) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := parseCommand(reader)
		if err != nil {
			if err!=io.EOF{
				fmt.Printf("Error in parseCommand: %v\n",err)
			}
			break
		}

		if len(args) <= 0 {
			fmt.Printf("Invalid arg lenght\n")
			break
		}

		cmd := cmd{
			name: strings.ToUpper(args[0]),
			args: args[1:],
		}
		switch cmd.name {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(cmd.args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				continue
			}
			msg := cmd.args[0]
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
			conn.Write([]byte(response))
		case "SET":
			if len(cmd.args) != 2 && len(cmd.args) != 4 {
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
				value:     value,
				expiresAt: expiresAt,
			}
			store.mu.Unlock()
			_ = aof.Write(formatRESP([]string{"SET", key, value}))

			if !expiresAt.IsZero() {
				expiresAtMs := strconv.FormatInt(expiresAt.UnixMilli(), 10)
				_ = aof.Write(formatRESP([]string{"PEXPIREAT", key, expiresAtMs}))
			}
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			if len(cmd.args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			key := cmd.args[0]
			store.mu.RLock()
			entry, exists := store.data[key]
			store.mu.RUnlock()

			if !exists {
				conn.Write([]byte("$-1\r\n"))
				continue

			}

			if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
				store.mu.Lock()
				delete(store.data, key)
				store.mu.Unlock()
				conn.Write([]byte("$-1\r\n"))
				continue
			}

			conn.Write([]byte(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(entry.value), entry.value)))
		case "DEL":
			if len(cmd.args) == 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
				continue
			}

			deletedCount := 0

			store.mu.Lock()
			for _, key := range cmd.args {
				if _, exists := store.data[key]; exists {
					delete(store.data, key)
					deletedCount++
				}
			}
			store.mu.Unlock()

			if deletedCount > 0 {
				_ = aof.Write(formatRESP(args))
			}

			conn.Write([]byte(fmt.Appendf(nil, ":%d\r\n", deletedCount)))
		case "EXPIRE":
			if len(cmd.args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'expire' command\r\n"))
				continue
			}

			key := cmd.args[0]
			seconds, err := strconv.Atoi(cmd.args[1])
			if err != nil || seconds < 0 {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}

			expiresAt := time.Now().Add(time.Duration(seconds) * time.Second)

			store.mu.Lock()
			entry, exists := store.data[key]
			if !exists {
				store.mu.Unlock()
				conn.Write([]byte(":0\r\n"))
				continue
			}
			entry.expiresAt = expiresAt
			store.data[key] = entry
			store.mu.Unlock()

			expiresAtMs := strconv.FormatInt(expiresAt.UnixMilli(), 10)
			_ = aof.Write(formatRESP([]string{"PEXPIREAT", key, expiresAtMs}))
			conn.Write([]byte(":1\r\n"))
		case "TTL":
			if len(cmd.args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'ttl' command\r\n"))
				continue
			}

			key := cmd.args[0]

			store.mu.RLock()
			entry, exists := store.data[key]
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
			conn.Write([]byte(fmt.Appendf(nil, ":%d\r\n", remaining)))
		case "CLIENT":
            conn.Write([]byte("+OK\r\n"))
        case "SELECT":
            conn.Write([]byte("+OK\r\n"))
        case "CONFIG":
            param := ""
            if len(cmd.args) >= 2 {
                param = cmd.args[1]
            }
            conn.Write(fmt.Appendf(nil, "*2\r\n$%d\r\n%s\r\n$0\r\n\r\n", len(param), param))
        case "COMMAND":
            conn.Write([]byte("*0\r\n"))
		default:
			conn.Write([]byte(fmt.Appendf(nil, "-ERR unknown command '%s'\r\n", cmd.name)))
		}
	}
}

func startActiveExpiration(store *Store) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		start := time.Now()
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

			if time.Since(start) > 25*time.Millisecond {
				break
			}
		}
	}
}

func parseCommand(reader *bufio.Reader) ([]string, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	if b != '*' {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		fullLine := strings.TrimSpace(string(b) + line)
		if fullLine == "" {
			return nil, nil
		}
		return strings.Fields(fullLine), nil
	}

	parseString, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	countString := strings.TrimSpace(parseString)

	count, err := strconv.Atoi(countString)
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line[0] != '$' {
			return nil, fmt.Errorf("expected '$', got %q\n", line[0])
		}

		strlen := strings.TrimSpace(line[1:])
		intlen, err := strconv.Atoi(strlen)
		if err != nil {
			return nil, err
		}

		buf := make([]byte, intlen)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}

		reader.Discard(2)

		args = append(args, string(buf))
	}

	return args, nil
}

func replayAOF(store *Store, filename string) error {
	now := time.Now()
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
				store.data[key] = Entry{value: val}
			}

		case "DEL":
			for _, k := range cmdArgs {
				delete(store.data, k)
			}

		case "PEXPIREAT":
			if len(cmdArgs) == 2 {
				key := cmdArgs[0]
				ms, err := strconv.ParseInt(cmdArgs[1], 10, 64)
				if err == nil {
					exp := time.UnixMilli(ms)
					if now.After(exp) {
						delete(store.data, key)
					} else if entry, exists := store.data[key]; exists {
						entry.expiresAt = exp
						store.data[key] = entry
					}
				}
			}
		}
	}
	return nil
}

func main() {
	file, err := os.OpenFile("appendonly.aof", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("AOF file error: %v\n", err)
		return
	}
	defer file.Close()

	aof := &AOF{file: file}

	store := &Store{data: make(map[string]Entry)}

	if err := replayAOF(store, "appendonly.aof"); err != nil {
		fmt.Printf("AOF replay error: %v\n", err)
	}

	go startActiveExpiration(store)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Printf("Error while listening: %v\n", err)
		return
	}
	defer listener.Close()
	fmt.Println("Server running on :6379.....")

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleClient(conn, store, aof)
		}

	}()

	sig := <-sigChan
	fmt.Printf("\nReceived signal %v. Shutting down gracefully...\n", sig)

	listener.Close()

	aof.mu.Lock()
	_ = aof.file.Sync()
	_ = aof.file.Close()

	aof.mu.Unlock()

	fmt.Println("State flushed. Goodbye!")
}
