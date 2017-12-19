package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fireworq/benchworker/scenario"
)

type Worker struct {
	server        *http.Server
	addr          net.Addr
	reqs          *requests
	totalJobs     uint32
	completedJobs uint32
	finished      chan struct{}
}

func NewWorker(host string) (*Worker, error) {
	listener, err := net.Listen("tcp", host+":0")
	if err != nil {
		log.Panic(err)
	}

	worker := &Worker{
		addr:      listener.Addr(),
		reqs:      &requests{m: make(map[uint]status)},
		totalJobs: ^uint32(0),
		finished:  make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/work", worker.handle)

	server := &http.Server{Handler: mux}
	go func() {
		server.Serve(listener)
	}()

	worker.server = server

	return worker, nil
}

func (w *Worker) Errors() uint {
	return w.TotalJobs() - w.CompletedJobs()
}

func (w *Worker) Inspect() {
	ids := make([]uint, 0)
	for id, _ := range w.reqs.m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		st := w.reqs.m[id]
		var conflict string
		if st.requested != st.expected {
			conflict = " !"
		}
		fmt.Printf("%04d: %d / %d%s\n", id, st.requested, st.expected, conflict)
	}

	fmt.Printf("Worker processed: %d / %d\n", w.CompletedJobs(), w.TotalJobs())
}

func (w *Worker) Finished() <-chan struct{} {
	return w.finished
}

func (w *Worker) Stop() {
	w.server.Close()
}

func (w *Worker) Addr() string {
	return w.addr.String()
}

func (w *Worker) Url() string {
	return "http://" + w.Addr() + "/work"
}

func (w *Worker) handle(writer http.ResponseWriter, req *http.Request) {
	var payload scenario.Payload
	decoder := json.NewDecoder(req.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		http.Error(writer, fmt.Sprintf("400 Bad Request\n\n%s", err), http.StatusBadRequest)
	}

	time.Sleep(time.Duration(payload.CycleTime) * time.Second)

	st, ok := w.reqs.inc(&payload)

	if st.requested == payload.RequestCount {
		atomic.AddUint32(&w.completedJobs, 1)
	}

	res := &Result{Status: "success"}
	if !ok {
		res.Status = "failure"
	}
	json, err := json.Marshal(res)
	if err != nil {
		log.Panic(err)
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Content-Length", strconv.Itoa(len(json)))
	writer.WriteHeader(200)
	writer.Write(json)

	go w.checkIfFinished()
}

func (w *Worker) CompletedJobs() uint {
	return uint(atomic.LoadUint32(&w.completedJobs))
}

func (w *Worker) TotalJobs() uint {
	return uint(atomic.LoadUint32(&w.totalJobs))
}

func (w *Worker) SetTotalJobs(totalJobs uint) {
	atomic.StoreUint32(&w.totalJobs, uint32(totalJobs))
	go w.checkIfFinished()
}

func (w *Worker) checkIfFinished() {
	if w.CompletedJobs() >= w.TotalJobs() {
		w.finished <- struct{}{}
	}
}

type status struct {
	expected  uint
	requested uint
	failed    uint
}

type requests struct {
	sync.Mutex
	m map[uint]status
}

func (r *requests) inc(job *scenario.Payload) (status, bool) {
	r.Lock()
	defer r.Unlock()

	result := true

	st, ok := r.m[job.Id]
	if !ok {
		st = status{expected: job.RequestCount}
	}

	if st.failed < job.FailCount {
		st.failed++
		result = false
	}
	st.requested++

	r.m[job.Id] = st

	return st, result
}

type Result struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
