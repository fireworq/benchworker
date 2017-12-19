package scenario

import (
	"math/rand"
	"strings"
)

type Scenario struct {
	QueueName       string `json:"queue_name"`
	TotalJobs       uint   `json:"total_jobs"`
	JobConcurrency  uint   `json:"job_concurrency"`
	MaxWorkers      uint   `json:"max_workers"`
	PollingInterval uint   `json:"polling_interval"`
	ErrorRate       uint   `json:"error_rate"`
	MaxPayloadSize  uint   `json:"max_payload_size"`
	MaxRetries      uint   `json:"max_retries"`
	MaxRunAfter     uint   `json:"max_run_after"`
	MaxCycleTime    uint   `json:"max_cycle_time"`
}

type ScenarioReader struct {
	*Scenario
	rand  *rand.Rand
	maxId uint
}

func NewScenario(seed int64, scenario *Scenario) *ScenarioReader {
	rand := rand.New(rand.NewSource(seed))
	return &ScenarioReader{scenario, rand, 0}
}

func (s *ScenarioReader) NextJob() *Job {
	id := s.maxId + 1
	s.maxId = id

	cycleTime := s.randUintMax(s.MaxCycleTime)

	maxRetries := s.randUintMax(s.MaxRetries)
	var requestCount uint = 1
	var failCount uint

	if s.randUintn(100) < s.ErrorRate {
		failCount = maxRetries + 1
		requestCount = failCount
	}

	len := s.randUintMax(s.MaxPayloadSize)

	return &Job{
		Payload: &Payload{
			Id:           id,
			CycleTime:    cycleTime,
			RequestCount: requestCount,
			FailCount:    failCount,
			Value:        strings.Repeat(".", int(len)),
		},
		RunAfter:   s.randUintMax(s.MaxRunAfter),
		MaxRetries: maxRetries,
		Timeout:    cycleTime + 5,
	}
}

func (s *ScenarioReader) randUintMax(max uint) uint {
	return s.randUintn(max + 1)
}

func (s *ScenarioReader) randUintn(n uint) uint {
	return uint(s.rand.Int31n(int32(n)))
}
