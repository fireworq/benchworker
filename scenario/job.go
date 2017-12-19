package scenario

type Job struct {
	Payload    *Payload `json:"payload"`
	RunAfter   uint     `json:"run_after"`
	MaxRetries uint     `json:"max_retries"`
	Timeout    uint     `json:"timeout"`
}

type Payload struct {
	Id           uint   `json:"id"`
	CycleTime    uint   `json:"cycle_time"`
	RequestCount uint   `json:"request_count"`
	FailCount    uint   `json:"fail_count"`
	Value        string `json:"value"`
}
