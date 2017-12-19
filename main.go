package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fireworq/benchworker/api"
	"github.com/fireworq/benchworker/runner"
	"github.com/fireworq/benchworker/scenario"
)

func main() {
	var (
		showVersion bool
		conf        config
	)
	out := os.Stderr
	log.SetOutput(out)

	flags := flag.NewFlagSet(Name, flag.ContinueOnError)
	flags.SetOutput(out)
	flags.Usage = func() {
		fmt.Fprint(out, helpText)
	}
	flags.BoolVar(&showVersion, "v", false, "")
	flags.BoolVar(&showVersion, "version", false, "")
	flags.Int64Var(&conf.seed, "seed", 3141592, "")
	flags.StringVar(&conf.targetAddr, "target-address", "127.0.0.1:8080", "")
	flags.StringVar(&conf.workerHost, "worker-host", "127.0.0.1", "")

	if err := flags.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	if showVersion {
		fmt.Fprintln(out, versionString(" "))
		os.Exit(0)
	}

	bench := &benchmarker{out: out}

	s := &scenario.Scenario{
		PollingInterval: 100,
		MaxWorkers:      50,
		TotalJobs:       1000,
		MaxPayloadSize:  10,
		ErrorRate:       0,
		MaxCycleTime:    0,
		MaxRunAfter:     0,
		MaxRetries:      0,
	}
	s = bench.optimize(&conf, s)
	s.QueueName = "benchmark1"

	fmt.Fprintln(out, "--------------------------------")

	r := bench.iterate(10, &conf, s)

	fmt.Fprintln(out, "[Queue]")
	fmt.Fprintf(out, "Took        %.3f sec.\n", r.QueueTime.Seconds())
	fmt.Fprintf(out, "Throughput: %.3f /s\n", r.QueueThroughput())
	fmt.Fprintf(out, "Score:      %.3f\n", r.QueueScore())
	fmt.Fprintln(out, "[Worker]")
	fmt.Fprintf(out, "Took        %.3f sec.\n", r.WorkerTime.Seconds())
	fmt.Fprintf(out, "Throughput: %.3f /s\n", r.WorkerThroughput())
	fmt.Fprintf(out, "Score:      %.3f\n", r.WorkerScore())
	fmt.Fprintln(out, "[Overall]")
	fmt.Fprintf(out, "Score:      %.3f\n", r.Score())

	j, err := json.Marshal(&result{
		Scenario: s,
		Result:   r,
	})
	if err != nil {
		log.Panic(err)
	}

	fmt.Println(string(j))
}

func versionString(sep string) string {
	var build string
	if len(Build) == 0 {
		Build = "SNAPSHOT"
	}
	if Build != "release" {
		build = "-" + Build
	}
	return strings.Join([]string{Name, sep, Version, build}, "")
}

var helpText = `Usage: benchworker [options]

  A benchmark tool for Fireworq.

Options:

  --version, -v  Show the version string.
  --help, -h     Show the help message.

  --target-address
    default: 127.0.0.1:8080

    Specifies the address of the target job queue in a form
    <host>:<port>.

  --worker-host
    default: 127.0.0.1

    Specifies the host of a worker for a benchmark.  The port number
    of the worker is a free port assigned automatically.
`

type benchmarker struct {
	out io.Writer
}

func (bench *benchmarker) prepare(conf *config, s *scenario.Scenario) {
	client, err := api.New("http://" + conf.targetAddr)
	if err != nil {
		log.Panic(err)
	}

	client.CreateQueue(s.QueueName, s.MaxWorkers, s.PollingInterval)
	client.CreateRouting(s.QueueName, s.QueueName)
	time.Sleep(3 * time.Second)
}

func (bench *benchmarker) optimize(conf *config, s0 *scenario.Scenario) *scenario.Scenario {
	var start uint = 10
	var step uint = 10
	var tries = 5
	var epsilon float64 = 50

	var tooMuchReducing = 3
	var tooMuchNotGrowing = 3
	reducing := 0
	notGrowing := 0

	s := *s0
	s.QueueName = "benchmark0"
	bench.prepare(conf, &s)

	var max scenario.Scenario

	var bestScore float64
	for c := uint(start); true; c += step {
		s.JobConcurrency = c
		var sum float64
		for i := 0; i < tries; i++ {
			bench.printProgress(i, tries)
			r := bench.doBenchmark(conf, &s)
			s := r.Score()
			sum += s
		}
		score := sum / float64(tries)
		fmt.Fprintf(bench.out, "\rJobConcurrency: %d, Score: %.3f\n", c, score)

		if score < bestScore {
			reducing++
		}
		if score-bestScore < epsilon {
			notGrowing++
		}

		if bestScore < score {
			max = s
			reducing = 0
			notGrowing = 0
			bestScore = score
		}

		if reducing >= tooMuchReducing || notGrowing >= tooMuchNotGrowing {
			break
		}
	}

	return &max
}

func (bench *benchmarker) iterate(n uint, conf *config, s *scenario.Scenario) *runner.Result {
	bench.prepare(conf, s)

	var sum = &runner.Result{}
	for i := uint(0); i < n; i++ {
		bench.printProgress(int(i), int(n))
		sum = sum.Add(bench.doBenchmark(conf, s))
	}
	fmt.Fprint(bench.out, "\r"+strings.Repeat(" ", int(n+2))+"\r")

	return sum
}

func (bench *benchmarker) doBenchmark(conf *config, s *scenario.Scenario) *runner.Result {
	scenario := scenario.NewScenario(conf.seed, s)

	client, err := api.New("http://" + conf.targetAddr)
	if err != nil {
		log.Panic(err)
	}

	runner := runner.NewRunner(client, conf.workerHost)
	result, err := runner.Run(scenario)
	if err != nil {
		log.Panic(err)
	}

	return result
}

func (bench *benchmarker) printProgress(i, n int) {
	bar := "[" + strings.Repeat("=", i) + ">" + strings.Repeat(" ", n-i-1) + "]"
	fmt.Fprint(bench.out, "\r"+bar)
}

type config struct {
	seed       int64
	targetAddr string
	workerHost string
}

type result struct {
	Scenario *scenario.Scenario `json:"scenario"`
	Result   *runner.Result     `json:"result"`
}
