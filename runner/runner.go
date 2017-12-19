package runner

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/fireworq/benchworker/api"
	"github.com/fireworq/benchworker/scenario"
	"github.com/fireworq/benchworker/worker"
)

func benchmark(block func()) time.Duration {
	start := time.Now()
	block()
	return time.Now().Sub(start)
}

type Job struct {
	*scenario.Job
	Url      string `json:"url"`
	Category string `json:"category"`
}

type Result struct {
	QueueTotalJobs  uint          `json:"queue_total_jobs"`
	QueueErrors     uint          `json:"queue_errors"`
	QueueTime       time.Duration `json:"queue_time"`
	WorkerTotalJobs uint          `json:"worker_total_jobs"`
	WorkerErrors    uint          `json:"worker_errors"`
	WorkerTime      time.Duration `json:"worker_time"`
}

func (r *Result) Add(other *Result) *Result {
	return &Result{
		QueueTotalJobs:  r.QueueTotalJobs + other.QueueTotalJobs,
		QueueErrors:     r.QueueErrors + other.QueueErrors,
		QueueTime:       r.QueueTime + other.QueueTime,
		WorkerTotalJobs: r.WorkerTotalJobs + other.WorkerTotalJobs,
		WorkerErrors:    r.WorkerErrors + other.WorkerErrors,
		WorkerTime:      r.WorkerTime + other.WorkerTime,
	}
}

func (r *Result) QueueThroughput() float64 {
	return float64((r.QueueTotalJobs - r.QueueErrors)) / r.QueueTime.Seconds()
}

func (r *Result) WorkerThroughput() float64 {
	return float64((r.WorkerTotalJobs - r.WorkerErrors)) / r.WorkerTime.Seconds()
}

var queueErrorPenalty float64 = 2
var workerErrorPenalty float64 = 2

func (r *Result) QueueScore() float64 {
	return r.QueueThroughput() - queueErrorPenalty*float64(r.QueueErrors)
}

func (r *Result) WorkerScore() float64 {
	return r.WorkerThroughput() - workerErrorPenalty*float64(r.WorkerErrors)
}

func (r *Result) Score() float64 {
	return r.WorkerScore() - queueErrorPenalty*float64(r.QueueErrors)
}

type runner struct {
	client     *api.Client
	workerHost string
}

func NewRunner(client *api.Client, workerHost string) *runner {
	return &runner{client: client, workerHost: workerHost}
}

func (r *runner) Run(scenario *scenario.ScenarioReader) (*Result, error) {
	worker, err := worker.NewWorker(r.workerHost)
	if err != nil {
		return nil, err
	}

	q := start(r.client, scenario.JobConcurrency)

	var (
		queueTime  time.Duration
		workerTime time.Duration
	)

	workerTime = benchmark(func() {
		wait := uint(0)

		queueTime = benchmark(func() {
			for i := uint(0); i < scenario.TotalJobs; i++ {
				job := scenario.NextJob()

				t := job.RunAfter + job.Payload.CycleTime
				if wait < t {
					wait = t
				}

				q.push(&Job{
					job,
					worker.Url(),
					scenario.QueueName,
				})
			}
			<-q.end()
		})

		worker.SetTotalJobs(scenario.TotalJobs - q.errorCount())
		wait *= worker.TotalJobs()/scenario.MaxWorkers + 1

		select {
		case <-worker.Finished():
			break
		case <-time.After(time.Duration(wait+10) * time.Second):
			if len(os.Getenv("DEBUG")) > 0 {
				worker.Inspect()
				log.Println("Timed out")
			}
		}
	})
	worker.Stop()

	return &Result{
		QueueTotalJobs:  scenario.TotalJobs,
		QueueErrors:     q.errorCount(),
		QueueTime:       queueTime,
		WorkerTotalJobs: worker.TotalJobs(),
		WorkerErrors:    worker.Errors(),
		WorkerTime:      workerTime,
	}, nil
}

type queue struct {
	buffer  chan *Job
	stopped chan struct{}
	errors  uint32
}

func start(client *api.Client, concurrency uint) *queue {
	q := &queue{
		buffer:  make(chan *Job, concurrency),
		stopped: make(chan struct{}),
	}
	go q.loop(client)
	return q
}

func (q *queue) errorCount() uint {
	return uint(atomic.LoadUint32(&q.errors))
}

func (q *queue) end() <-chan struct{} {
	q.buffer <- nil
	return q.stopped
}

func (q *queue) push(job *Job) {
	q.buffer <- job
}

func (q *queue) loop(client *api.Client) {
	sem := semaphore.NewWeighted(int64(cap(q.buffer)))
	var wg sync.WaitGroup

Loop:
	for {
		job := <-q.buffer
		if job == nil {
			wg.Wait()
			break Loop
		}

		if err := sem.Acquire(context.Background(), 1); err != nil {
			log.Panic(err)
		}

		wg.Add(1)
		go func(job *Job) {
			defer sem.Release(1)
			defer wg.Done()

			res, err := client.Enqueue(job.Category, job)
			if err != nil || res.StatusCode >= 400 {
				atomic.AddUint32(&q.errors, 1)
			}
		}(job)
	}

	q.stopped <- struct{}{}
}
