# tiny-redis

A minimal in-memory key-value database in Go implementing the core Redis Serialization Protocol (RESP).

---

## Architecture Overview

* **Concurrency Model:** Goroutine-per-connection pattern using Go's `net` package listening on port `:6379`.
* **Protocol Parser:** Custom RESP deserializer that reads binary-safe arrays of bulk strings (`*<count>\r\n$<len>\r\n...`) directly from buffered network streams (`bufio.Reader`).
* **Storage Engine:** Thread-safe in-memory store protected by a reader-writer mutex (`sync.RWMutex`). Backed by a `map[string]Entry` storing values alongside expiration timestamps.
* **Passive Eviction:** Validates key lifetime on access during read operations, purging expired entries on the fly.

```text
Clients (redis-cli / nc) 
       │  TCP (:6379)
       ▼
net.Listener.Accept() 
       │
       ├─► Goroutine (Client 1) ──► parseCommand() ──┐
       ├─► Goroutine (Client 2) ──► parseCommand() ──┼─► [sync.RWMutex] map[string]Entry
       └─► Goroutine (Client N) ──► parseCommand() ──┘

```

---

## Supported Commands

| Command | Format | Return |
| --- | --- | --- |
| `PING` | `PING` | `+PONG\r\n` |
| `ECHO` | `ECHO <msg>` | `$<len>\r\n<msg>\r\n` |
| `SET` | `SET <key> <val> [EX seconds]` | `+OK\r\n` |
| `GET` | `GET <key>` | `$<len>\r\n<val>\r\n` or `$-1\r\n` |
| `DEL` | `DEL <key> [key ...]` | `:<count>\r\n` |
| `EXPIRE` | `EXPIRE <key> <seconds>` | `:1\r\n` (set) or `:0\r\n` (not found) |
| `TTL` | `TTL <key>` | `:<seconds>\r\n`, `:-1\r\n` (no TTL), or `:-2\r\n` (missing) |

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
# Basic KV operations
redis-cli -p 6379 set foo bar
redis-cli -p 6379 get foo

# Set with expiration (seconds)
redis-cli -p 6379 set session token123 ex 30
redis-cli -p 6379 ttl session

# Update TTL on an existing key
redis-cli -p 6379 expire foo 60
redis-cli -p 6379 ttl foo

# Delete one or more keys
redis-cli -p 6379 del foo session

```

---

## License

MIT