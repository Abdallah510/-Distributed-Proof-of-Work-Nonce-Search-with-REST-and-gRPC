package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"puzzle/powpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Job struct {
	ID         string
	Data       string
	Difficulty int
	Status     string // RUNNING, FOUND, CANCELLED
	Checked    uint64
	Nonce      uint64
	Hash       string
	Start, End time.Time

	ctx    context.Context
	cancel context.CancelFunc 
	next   uint64            
	retry  []uint64          
	subs   []chan string     
}

type Worker struct {
	ID       string
	Addr     string
	Alive    bool
	LastBeat time.Time
	JobID    string 
	Range    uint64 

	client powpb.WorkerClient
	cancel context.CancelFunc 
	token  int           
}

type Coordinator struct {
	powpb.UnimplementedCoordinatorServer
	mu        sync.Mutex
	jobs      map[string]*Job
	order     []string 
	workers   map[string]*Worker
	rangeSize uint64
	timeout   time.Duration
	rr        int 
}

// ======================= gRPC: called by workers =======================

func (c *Coordinator) RegisterWorker(ctx context.Context, r *powpb.RegisterRequest) (*powpb.RegisterReply, error) {
	conn, err := grpc.NewClient(r.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.workers[r.WorkerId] = &Worker{ID: r.WorkerId, Addr: r.Address, Alive: true, LastBeat: time.Now(), client: powpb.NewWorkerClient(conn)}
	c.mu.Unlock()
	log.Printf("worker %s registered (%s)", r.WorkerId, r.Address)
	c.schedule()
	return &powpb.RegisterReply{}, nil
}

func (c *Coordinator) Heartbeat(ctx context.Context, r *powpb.HeartbeatRequest) (*powpb.HeartbeatReply, error) {
	c.mu.Lock()
	w, ok := c.workers[r.WorkerId]
	if ok {
		w.LastBeat = time.Now()
		if !w.Alive {
			w.Alive = true
			log.Printf("worker %s is alive again", w.ID)
			defer c.schedule()
		}
	}
	c.mu.Unlock()
	return &powpb.HeartbeatReply{Known: ok}, nil
}

// ======================= scheduling =======================

func (c *Coordinator) schedule() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.workers {
		if !w.Alive || w.JobID != "" {
			continue
		}
		j := c.nextRunningJob()
		if j == nil {
			return
		}
		var r uint64
		if len(j.retry) > 0 { 
			r, j.retry = j.retry[0], j.retry[1:]
		} else {
			r = j.next
			j.next++
		}
		c.assign(w, j, r)
	}
}

func (c *Coordinator) nextRunningJob() *Job {
	for i := 0; i < len(c.order); i++ {
		j := c.jobs[c.order[(c.rr+i)%len(c.order)]]
		if j.Status == "RUNNING" {
			c.rr = (c.rr + i + 1) % len(c.order)
			return j
		}
	}
	return nil
}


func (c *Coordinator) assign(w *Worker, j *Job, r uint64) {
	ctx, cancel := context.WithCancel(j.ctx) // child of the job context
	w.JobID, w.Range, w.cancel = j.ID, r, cancel
	w.token++
	start := r * c.rangeSize
	log.Printf("job %s: range %d [%d, %d) -> %s", j.ID, r, start, start+c.rangeSize, w.ID)
	go c.search(ctx, w, w.token, j, &powpb.SearchRequest{
		JobId: j.ID, Data: j.Data, Difficulty: int32(j.Difficulty), Start: start, End: start + c.rangeSize})
}

// search runs one server-streaming Search call and handles its outcome.
func (c *Coordinator) search(ctx context.Context, w *Worker, token int, j *Job, req *powpb.SearchRequest) {
	var last uint64 // progress already counted
	stream, err := w.client.Search(ctx, req)
	for err == nil {
		var u *powpb.SearchUpdate
		if u, err = stream.Recv(); err != nil {
			break
		}
		c.mu.Lock()
		j.Checked += u.Checked - last
		last = u.Checked
		if u.Done && u.Found && j.Status == "RUNNING" { // early termination
			j.Status, j.Nonce, j.Hash, j.End = "FOUND", u.Nonce, u.Hash, time.Now()
			j.cancel() // stops every other Search of this job
			log.Printf("job %s FOUND by %s: nonce=%d, cancelling other searches", j.ID, w.ID, u.Nonce)
			c.finish(j, "found")
		}
		c.mu.Unlock()
		if u.Done {
			break
		}
	}

	c.mu.Lock()
	if w.token == token && w.JobID != "" { // still our assignment
		w.JobID = ""
		if err != nil && ctx.Err() == nil && j.Status == "RUNNING" { // Search failed: worker is down
			log.Printf("job %s: Search on %s failed (%v); reassigning range %d", j.ID, w.ID, err, w.Range)
			w.Alive = false
			j.retry = append(j.retry, w.Range)
		}
	}
	c.mu.Unlock()
	c.schedule() // give this worker (or another one) the next range
}

// monitor marks workers dead when heartbeats stop and reassigns their range.
func (c *Coordinator) monitor() {
	for range time.Tick(500 * time.Millisecond) {
		c.mu.Lock()
		for _, w := range c.workers {
			if w.Alive && time.Since(w.LastBeat) > c.timeout {
				w.Alive = false
				log.Printf("worker %s missed heartbeats -> dead", w.ID)
				if w.JobID != "" {
					j := c.jobs[w.JobID]
					if j.Status == "RUNNING" {
						j.retry = append(j.retry, w.Range)
						log.Printf("job %s: range %d of %s will be reassigned", j.ID, w.Range, w.ID)
					}
					w.cancel()
					w.JobID = ""
					w.token++
				}
			}
		}
		c.mu.Unlock()
		c.schedule()
	}
}

// ======================= SSE helpers =======================

func (c *Coordinator) view(j *Job) map[string]any {
	end := time.Now()
	if j.Status != "RUNNING" {
		end = j.End
	}
	v := map[string]any{"job_id": j.ID, "data": j.Data, "difficulty": j.Difficulty, "status": j.Status,
		"nonces_checked": j.Checked, "elapsed_ms": end.Sub(j.Start).Milliseconds()}
	if j.Status == "FOUND" {
		v["nonce"], v["hash"] = j.Nonce, j.Hash
	}
	return v
}

// publish sends one SSE event to every subscriber of the job (c.mu held).
func (c *Coordinator) publish(j *Job, event string) {
	b, _ := json.Marshal(c.view(j))
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", event, b)
	for _, ch := range j.subs {
		select {
		case ch <- msg:
		default: // slow subscriber: skip this update
		}
	}
}

// finish sends the final event and closes all SSE streams of the job (c.mu held).
func (c *Coordinator) finish(j *Job, event string) {
	c.publish(j, event)
	for _, ch := range j.subs {
		close(ch)
	}
	j.subs = nil
}

func (c *Coordinator) progressLoop() {
	for range time.Tick(time.Second) {
		c.mu.Lock()
		for _, j := range c.jobs {
			if j.Status == "RUNNING" {
				c.publish(j, "progress")
			}
		}
		c.mu.Unlock()
	}
}

// ======================= REST API =======================

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func errJSON(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (c *Coordinator) postJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Data       *string `json:"data"`
		Difficulty *int    `json:"difficulty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		errJSON(w, 400, "invalid JSON")
		return
	}
	if body.Data == nil || *body.Data == "" {
		errJSON(w, 400, "missing data")
		return
	}
	if body.Difficulty == nil || *body.Difficulty < 1 || *body.Difficulty > 8 {
		errJSON(w, 400, "difficulty must be between 1 and 8")
		return
	}
	c.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{ID: fmt.Sprintf("job-%d", len(c.jobs)+1), Data: *body.Data, Difficulty: *body.Difficulty,
		Status: "RUNNING", Start: time.Now(), ctx: ctx, cancel: cancel}
	c.jobs[j.ID] = j
	c.order = append(c.order, j.ID)
	c.mu.Unlock()
	log.Printf("job %s submitted: data=%q difficulty=%d", j.ID, j.Data, j.Difficulty)
	c.schedule()
	writeJSON(w, 202, map[string]string{"job_id": j.ID})
}

func (c *Coordinator) getJob(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.jobs[r.PathValue("id")]
	if !ok {
		errJSON(w, 404, "job not found")
		return
	}
	writeJSON(w, 200, c.view(j))
}

func (c *Coordinator) deleteJob(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.jobs[r.PathValue("id")]
	if !ok {
		errJSON(w, 404, "job not found")
		return
	}
	if j.Status != "RUNNING" {
		errJSON(w, 409, "job already finished: "+j.Status)
		return
	}
	j.Status, j.End = "CANCELLED", time.Now()
	j.cancel() // cancels the context of every Search call of this job -> workers stop
	log.Printf("job %s CANCELLED by client, cancelling its searches", j.ID)
	c.finish(j, "cancelled")
	writeJSON(w, 200, c.view(j))
}

func (c *Coordinator) listJobs(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := []map[string]any{}
	for _, id := range c.order {
		list = append(list, c.view(c.jobs[id]))
	}
	writeJSON(w, 200, list)
}

func (c *Coordinator) listWorkers(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := []map[string]any{}
	for _, wk := range c.workers {
		v := map[string]any{"worker_id": wk.ID, "address": wk.Addr, "alive": wk.Alive, "assignment": nil}
		if wk.JobID != "" {
			start := wk.Range * c.rangeSize
			v["assignment"] = map[string]any{"job_id": wk.JobID, "range": []uint64{start, start + c.rangeSize}}
		}
		list = append(list, v)
	}
	writeJSON(w, 200, list)
}

// events streams job progress with Server-Sent Events.
func (c *Coordinator) events(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	j, ok := c.jobs[r.PathValue("id")]
	if !ok {
		c.mu.Unlock()
		errJSON(w, 404, "job not found")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	b, _ := json.Marshal(c.view(j))
	fmt.Fprintf(w, "event: status\ndata: %s\n\n", b)
	w.(http.Flusher).Flush()
	if j.Status != "RUNNING" { 
		c.mu.Unlock()
		return
	}
	ch := make(chan string, 16)
	j.subs = append(j.subs, ch)
	c.mu.Unlock()

	for {
		select {
		case msg, open := <-ch:
			if !open { 
				return
			}
			fmt.Fprint(w, msg)
			w.(http.Flusher).Flush()
		case <-r.Context().Done(): // client disconnected
			c.mu.Lock()
			for i, s := range j.subs {
				if s == ch {
					j.subs = append(j.subs[:i], j.subs[i+1:]...)
					break
				}
			}
			c.mu.Unlock()
			return
		}
	}
}

func main() {
	httpPort := flag.Int("http", 8080, "HTTP port")
	grpcPort := flag.Int("grpc", 50051, "gRPC port")
	rangeSize := flag.Uint64("range", 1_000_000, "nonces per range")
	timeout := flag.Duration("timeout", 3*time.Second, "heartbeat timeout")
	flag.Parse()

	c := &Coordinator{jobs: map[string]*Job{}, workers: map[string]*Worker{}, rangeSize: *rangeSize, timeout: *timeout}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatal(err)
	}
	g := grpc.NewServer()
	powpb.RegisterCoordinatorServer(g, c)
	go g.Serve(lis)
	go c.monitor()
	go c.progressLoop()

	http.HandleFunc("POST /jobs", c.postJob)
	http.HandleFunc("GET /jobs", c.listJobs)
	http.HandleFunc("GET /jobs/{id}", c.getJob)
	http.HandleFunc("DELETE /jobs/{id}", c.deleteJob)
	http.HandleFunc("GET /jobs/{id}/events", c.events)
	http.HandleFunc("GET /workers", c.listWorkers)
	log.Printf("coordinator: HTTP :%d, gRPC :%d, range size %d", *httpPort, *grpcPort, *rangeSize)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *httpPort), nil))
}
