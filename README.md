# ENCS5324 Assignment 1: gRPC chat room (Part 2) and distributed PoW search (Part 3)

## Project layout

```
encs5324-a1/
├── go.mod
├── part2/
│   ├── proto/chat.proto            service definition (Send = unary, Subscribe = server streaming)
│   ├── chatpb/                     generated gRPC code (do not edit)
│   └── cmd/
│       ├── chat-server/main.go     gRPC server
│       └── chat-client/main.go     client
└── part3/
    ├── proto/pow.proto             Coordinator service (RegisterWorker, Heartbeat)
    │                               Worker service (Search = server streaming)
    ├── powpb/                      generated gRPC code (do not edit)
    ├── coordinator/main.go         REST API + SSE + scheduler + fault tolerance
    └── worker/main.go              PoW search + registration / heartbeats
```

## Requirements

- Go 1.22 or newer (https://go.dev/dl)
- curl or Postman, to act as the client in Part 3

`protoc` is **not** needed: the generated code (`chatpb/`, `powpb/`) is already included.

## Build

Run these commands from the project folder (the one that contains `go.mod`):

```bash
go mod tidy              # first time only: downloads gRPC and protobuf
go build -o bin/ ./...   # builds chat-server, chat-client, coordinator and worker into ./bin
```

On Windows the programs are `bin\chat-server.exe` and so on. In PowerShell, `./bin/coordinator` works as written.

Each program keeps running in its terminal, so **start every program in its own terminal window**.

---

## Part 2: Chat room (unary + server streaming)

| Terminal | Command | What it does |
|---|---|---|
| 1 | `./bin/chat-server` | starts the gRPC server on port 6000 |
| 2 | `./bin/chat-client bob subscribe` | **server streaming**: stays open and prints every new message |
| 3 | `./bin/chat-client alice send "hello everyone"` | **unary**: sends one message, prints its sequence number |
| 3 | `./bin/chat-client carol send "hi bob"` | another unary call |

Expected output:
- terminal 3: `sent, seq=1`, then `sent, seq=2`
- terminal 2: `[1] alice: hello everyone`, then `[2] carol: hi bob`, arriving live over the stream
- terminal 1: `Subscribe: bob joined`, `Send: seq=1 alice: hello everyone`, ...

Press Ctrl+C to stop a program.

---

## Part 3: Distributed Proof-of-Work nonce search

The puzzle: find a nonce such that `SHA-256(data + nonce)` (as hex) starts with `difficulty` zeros.
For example, `data = "hello"` and nonce `42` hash the text `hello42`.

### 1. Start the coordinator (terminal 1)

```bash
./bin/coordinator
```

Flags (all optional):

| Flag | Default | Meaning |
|---|---|---|
| `-http` | 8080 | port of the REST API (for clients) |
| `-grpc` | 50051 | gRPC port (for workers) |
| `-range` | 1000000 | number of nonces in each range given to a worker |
| `-timeout` | 3s | a worker with no heartbeat for this long is considered dead |

### 2. Start the workers (terminal 2)

```bash
./bin/worker -n 4
```

This starts 4 workers named `w7001` to `w7004` (on ports 7001 to 7004). Each worker shows
`registered with coordinator`.

| Flag | Default | Meaning |
|---|---|---|
| `-n` | 1 | number of workers to start |
| `-port` | 7001 | port of the first worker (worker i uses port + i − 1) |
| `-coordinator` | localhost:50051 | address of the coordinator's gRPC server |

To be able to stop **one** worker (fault-tolerance test), start them as separate processes,
one per terminal:

```bash
./bin/worker -port 7001
./bin/worker -port 7002
./bin/worker -port 7003
```

The coordinator and workers can be started in any order. A worker retries until the coordinator is up.

### 3. Use the REST API (terminal 3, curl or Postman)

| Method | URL | Body | Result |
|---|---|---|---|
| POST | `http://localhost:8080/jobs` | `{"data": "hello", "difficulty": 6}` | **202** + `{"job_id": "job-1"}` |
| GET | `http://localhost:8080/jobs/job-1` | | status (RUNNING / FOUND / CANCELLED), nonces checked, elapsed time, nonce and hash when found |
| GET | `http://localhost:8080/jobs/job-1/events` | | live progress (Server-Sent Events), ends with a `found` or `cancelled` event |
| DELETE | `http://localhost:8080/jobs/job-1` | | cancels the job (**409** if it already finished) |
| GET | `http://localhost:8080/jobs` | | all jobs |
| GET | `http://localhost:8080/workers` | | all workers, alive or dead, and their current range |

Errors: **400** for invalid input (missing data, difficulty not 1–8, bad JSON), **404** for an unknown job,
**409** when cancelling a finished job.

With curl:

```bash
curl -X POST localhost:8080/jobs -d '{"data": "hello", "difficulty": 6}'
curl localhost:8080/jobs/job-1
curl -N localhost:8080/jobs/job-1/events
curl -X DELETE localhost:8080/jobs/job-1
curl localhost:8080/jobs
curl localhost:8080/workers
```

In Windows PowerShell, type `curl.exe` (not `curl`) and escape the quotes in the JSON:
`curl.exe -X POST localhost:8080/jobs -d '{\"data\":\"hello\",\"difficulty\":6}'`

In Postman: choose the method, enter the URL, and for POST open **Body → raw → JSON** and paste the body.

### What to look for in the logs

| Situation | Coordinator log | Worker log |
|---|---|---|
| Work is split | `job job-1: range 3 [3000000, 4000000) -> w7002` | `[w7002] START job-1 [3000000, 4000000)` |
| Nonce found | `job job-1 FOUND by w7001: nonce=..., cancelling other searches` | other workers: `STOPPED job-1 ...: context cancelled` |
| Job cancelled (DELETE) | `job job-1 CANCELLED by client, cancelling its searches` | every worker: `STOPPED job-1 ...: context cancelled` |
| Worker crashed (Ctrl+C) | `Search on w7002 failed (...); reassigning range 5` | |
| Worker frozen | `worker w7003 missed heartbeats -> dead` | |

---

## Regenerating the gRPC code (only if you change a .proto file)

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

protoc -I part2/proto --go_out=. --go_opt=module=a1 --go-grpc_out=. --go-grpc_opt=module=a1 part2/proto/chat.proto
protoc -I part3/proto --go_out=. --go_opt=module=a1 --go-grpc_out=. --go-grpc_opt=module=a1 part3/proto/pow.proto
```

`protoc` itself is available at https://github.com/protocolbuffers/protobuf/releases.
