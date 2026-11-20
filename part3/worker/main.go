package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"puzzle/powpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type worker struct {
	powpb.UnimplementedWorkerServer
	id string
}

// Search checks every nonce in [start, end), streams progress, and ends with one result.
func (w *worker) Search(req *powpb.SearchRequest, stream powpb.Worker_SearchServer) error {
	ctx := stream.Context()
	zeros := strings.Repeat("0", int(req.Difficulty))
	log.Printf("[%s] START %s [%d, %d)", w.id, req.JobId, req.Start, req.End)
	lastReport := time.Now()

	for n := req.Start; n < req.End; n++ {
		checked := n - req.Start + 1
		sum := sha256.Sum256([]byte(req.Data + strconv.FormatUint(n, 10)))
		hash := hex.EncodeToString(sum[:])
		if strings.HasPrefix(hash, zeros) {
			log.Printf("[%s] FOUND %s nonce=%d hash=%s", w.id, req.JobId, n, hash)
			return stream.Send(&powpb.SearchUpdate{Checked: checked, Done: true, Found: true, Nonce: n, Hash: hash})
		}
		if checked%10000 == 0 {
			// Stop promptly when the coordinator cancels this call.
			if ctx.Err() != nil {
				log.Printf("[%s] STOPPED %s [%d, %d): context cancelled after %d nonces", w.id, req.JobId, req.Start, req.End, checked)
				return ctx.Err()
			}
			if time.Since(lastReport) > 500*time.Millisecond {
				lastReport = time.Now()
				if err := stream.Send(&powpb.SearchUpdate{Checked: checked}); err != nil {
					return err
				}
			}
		}
	}
	log.Printf("[%s] DONE %s [%d, %d): no solution", w.id, req.JobId, req.Start, req.End)
	return stream.Send(&powpb.SearchUpdate{Checked: req.End - req.Start, Done: true})
}

// heartbeats registers the worker and then sends a heartbeat every second.
func heartbeats(id, addr, coordAddr string) {
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	c := powpb.NewCoordinatorClient(conn)
	register := func() {
		for {
			_, err := c.RegisterWorker(context.Background(), &powpb.RegisterRequest{WorkerId: id, Address: addr})
			if err == nil {
				log.Printf("[%s] registered with coordinator", id)
				return
			}
			time.Sleep(time.Second)
		}
	}
	register()
	for range time.Tick(time.Second) {
		r, err := c.Heartbeat(context.Background(), &powpb.HeartbeatRequest{WorkerId: id})
		if err == nil && !r.Known {
			register()
		}
	}
}

func main() {
	n := flag.Int("n", 1, "number of workers to start")
	port := flag.Int("port", 7001, "port of the first worker (worker i uses port+i-1)")
	coord := flag.String("coordinator", "localhost:50051", "coordinator gRPC address")
	flag.Parse()

	for i := 1; i <= *n; i++ {
		p := *port + i - 1
		id := fmt.Sprintf("w%d", p) // name = port, e.g. w7001
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", p))
		if err != nil {
			log.Fatal(err)
		}
		s := grpc.NewServer()
		powpb.RegisterWorkerServer(s, &worker{id: id})
		go s.Serve(lis)
		go heartbeats(id, fmt.Sprintf("localhost:%d", p), *coord)
		log.Printf("[%s] listening on :%d", id, p)
	}
	select {} // run forever
}
