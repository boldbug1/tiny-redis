# tiny-redis

A minimal in-memory key-value database in Go implementing the core Redis Serialization Protocol (RESP) with AOF persistence and active key eviction.

---

## Technical Details

* **Network Model:** Goroutine-per-connection pattern using Go's `net` package listening on port `:6379`.
* **Protocol Parser:** Custom RESP deserializer reading arrays of bulk strings (`*<count>\r\n$<len>\r\n...`) from a buffered network reader (`bufio.Reader`).
* **Storage:** In-memory `map[string]Entry` protected by a `sync.RWMutex` for safe concurrent access.
* **Key Expiration:**
* **Passive:** Checks expiration on read (`GET`, `TTL`) and evicts immediately if expired.
* **Active:** Background worker running on a 100ms ticker, sampling keys with an adaptive loop capped at 25ms execution time.


* **Durability (AOF):** Mutating commands (`SET`, `DEL`, `EXPIRE`) are appended to `appendonly.aof` in RESP format and replayed into memory on server boot.
* **Shutdown:** Traps `SIGINT`/`SIGTERM` to stop accepting connections, flush file buffers to disk via `fsync`, and exit cleanly.

```text
Clients (redis-cli / nc) 
       │  TCP (:6379)
       ▼
net.Listener.Accept() 
       │
       ├─► Goroutine (Client 1) ──► parseCommand() ──┐
       ├─► Goroutine (Client 2) ──► parseCommand() ──┼─► [sync.RWMutex] map[string]Entry
       └─► Goroutine (Client N) ──► parseCommand() ──┤          │
                                                     │          ▼
                                        (Mutations)  └──► AOF (appendonly.aof)
                                                                ▲
                                                         (Startup Replay)

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

# Delete keys
redis-cli -p 6379 del foo session

# Verify persistence
# Kill the server (Ctrl+C), restart it, and query your keys:
redis-cli -p 6379 get foo

```

---

## License

MIT
