# tiny-redis

A minimal in-memory key-value database in Go implementing the core Redis Serialization Protocol (RESP).

---

## Architecture Overview

* **Concurrency Model:** Goroutine-per-connection pattern using Go's `net` package listening on port `:6379`.
* **Protocol Parser:** Custom RESP deserializer that reads binary-safe arrays of bulk strings (`*<count>\r\n$<len>\r\n...`) directly from buffered network streams (`bufio.Reader`).
* **Storage Engine:** Thread-safe in-memory key-value map protected by a reader-writer mutex (`sync.RWMutex`), enabling concurrent reads (`RLock`) and mutually exclusive writes (`Lock`).

```text
Clients (redis-cli / nc) 
       │  TCP (:6379)
       ▼
net.Listener.Accept() 
       │
       ├─► Goroutine (Client 1) ──► parseCommand() ──┐
       ├─► Goroutine (Client 2) ──► parseCommand() ──┼─► [sync.RWMutex] map[string]string
       └─► Goroutine (Client N) ──► parseCommand() ──┘

```

---

## Supported Commands

| Command | Format | Return |
| --- | --- | --- |
| `PING` | `PING` | `+PONG\r\n` |
| `ECHO` | `ECHO <msg>` | `$<len>\r\n<msg>\r\n` |
| `SET` | `SET <key> <val>` | `+OK\r\n` |
| `GET` | `GET <key>` | `$<len>\r\n<val>\r\n` or `$-1\r\n` |
| `DEL` | `DEL [keys...]` | `:<count>\r]n` |

---

## How to Run

### 1. Build and Start Server

```bash
# Direct run
go run main.go

# Or compile and run
go build -o tiny-redis main.go
./tiny-redis

```

### 2. Test with `redis-cli`

```bash
# Ping
redis-cli -p 6379 ping

# Echo
redis-cli -p 6379 echo "hello world"

# Set a key
redis-cli -p 6379 set foo bar

# Get a key
redis-cli -p 6379 get foo

# Missing key returns (nil)
redis-cli -p 6379 get missing

```

---

# License

MIT
