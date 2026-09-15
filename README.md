# tiny-redis

A minimal, dependency-free Redis-compatible server written in Go, implementing the core Redis Serialization Protocol (RESP), deterministic AOF persistence, two-tier key eviction, and high-throughput concurrent connection handling.

---

## Technical Details

* **Network Model:** Goroutine-per-connection pattern using Go's `net` package listening on port `:6379`.
* **Dual-Format Protocol Parser:** Custom RESP deserializer that handles standard multibulk arrays (`*<count>\r\n...`) and inline plain-text commands (`PING\r\n`, `SET foo bar\r\n`) via `bufio.Reader`.
* **Storage & Concurrency:** In-memory `map[string]Entry` guarded by `sync.RWMutex`. Employs an optimistic read path (`RLock`) for cache hits and lazily escalates to an exclusive write lock (`Lock`) only when evicting expired entries.
* **Key Expiration:**
  * **Passive:** Checks TTL on read (`GET`, `TTL`) and eagerly deletes keys if the expiration timestamp has passed.
  * **Active Sweeper:** Background routine running on a 100ms ticker, randomly sampling keys in an adaptive 25ms time-capped loop to eliminate expired memory without starving concurrent queries.
* **Deterministic Durability (AOF):** Mutating commands (`SET`, `DEL`, `EXPIRE`) are committed to `appendonly.aof` using absolute millisecond UNIX timestamps (`PEXPIREAT`). This guarantees that expiration windows do not reset upon server restarts and accurately expire against wall-clock time.
* **Graceful Shutdown:** Intercepts `SIGINT` / `SIGTERM` signals to stop accepting new traffic, flushes all pending file buffers to disk via `fsync`, closes descriptors cleanly, and exits.

```text
Clients (redis-cli / redis-benchmark / nc) 
       │  TCP (:6379)
       ▼
net.Listener.Accept() 
       │
       ├─► Goroutine (Client 1) ──► parseCommand() ──┐
       ├─► Goroutine (Client 2) ──► parseCommand() ──┼─► [sync.RWMutex] map[string]Entry
       └─► Goroutine (Client N) ──► parseCommand() ──┤          │
                                                     │          ▼
                          (Mutations: SET/PEXPIREAT) └──► AOF (appendonly.aof)
                                                                ▲
                                                         (Startup Replay)

```

---

## Benchmarks

Benchmarked using the official `redis-benchmark` utility with **50 concurrent connections** across **100,000 requests**:

```bash
redis-benchmark -p 6379 -c 50 -n 100000 -t ping,set,get -q

```

| Command | Throughput (Requests/sec) | P50 Latency |
| --- | --- | --- |
| `PING` (Inline) | **105,820 req/s** | 0.215 ms |
| `PING` (RESP Multibulk) | **114,155 req/s** | 0.215 ms |
| `SET` (with AOF write) | **97,181 req/s** | 0.255 ms |
| `GET` (Concurrent `RLock`) | **108,225 req/s** | 0.223 ms |

> *Tested on Linux (x86_64) using the standard Go runtime over local loopback.*

---

## Supported Commands

| Command | Format | Return |
| --- | --- | --- |
| `PING` | `PING` | `+PONG\r\n` |
| `ECHO` | `ECHO <msg>` | `$<len>\r\n<msg>\r\n` |
| `SET` | `SET <key> <val> [EX seconds]` | `+OK\r\n` |
| `GET` | `GET <key>` | `$<len>\r\n<val>\r\n` or `$-1\r\n` (null) |
| `DEL` | `DEL <key> [key ...]` | `:<count>\r\n` |
| `EXPIRE` | `EXPIRE <key> <seconds>` | `:1\r\n` (set) or `:0\r\n` (not found) |
| `TTL` | `TTL <key>` | `:<seconds>\r\n`, `:-1\r\n` (no expiry), or `:-2\r\n` (missing/expired) |
| `CONFIG` | `CONFIG GET <param>` | `*2\r\n$<len>\r\n<param>\r\n$0\r\n\r\n` *(Benchmark support stub)* |
| `COMMAND` | `COMMAND` | `*0\r\n` *(Client negotiation stub)* |

---

## How to Run

### 1. Build and Start Server

```bash
# Run directly
go run main.go

# Or compile into a standalone binary
go build -o tiny-redis main.go
./tiny-redis

```

### 2. Connect with `redis-cli`

```bash
# Basic Key-Value operations
redis-cli -p 6379 set foo bar
redis-cli -p 6379 get foo

# Set with expiration (in seconds)
redis-cli -p 6379 set session token123 ex 30
redis-cli -p 6379 ttl session

# Update TTL on an existing key
redis-cli -p 6379 expire foo 60
redis-cli -p 6379 ttl foo

# Delete keys
redis-cli -p 6379 del foo session

# Verify crash-resilient persistence
# Stop the server with Ctrl+C, start it again, and retrieve your data:
redis-cli -p 6379 get foo

```

### 3. Run Benchmarks

```bash
redis-benchmark -p 6379 -c 50 -n 100000 -t ping,set,get -q

```

---

## License

MIT
