package solution

import ("errors"
		"fmt"
		"sync"
)


type jobState int

const (
	statePending    jobState = iota
	stateLeased 
	stateCompleted
)
 
const maxRunCount = 10
type job struct{
	id string
	payload []byte
	state jobState
	leasedTo string
	leaseExpiresAt int64
	runCount int
	priority int
	enqueuedAt int64
}

// LeasedJob is what a worker receives when it successfully leases a job.
type LeasedJob struct {
	ID             string
	Payload        []byte
	LeaseExpiresAt int64 // milliseconds, in the same frame as Clock.Now()
}

// Clock is the queue's view of time. The test harness supplies a fake clock
// so it can fast-forward past lease expiries without sleeping.
type Clock interface {
	Now() int64 // milliseconds
}

// Stats reports the current size of each job state.
type Stats struct {
	Pending   int // never leased, or failed and waiting
	Leased    int // currently held by a worker (may have expired)
	Completed int // acked successfully
}

// JobQueue is what you implement.
//
// LEVELS TO IMPLEMENT:
//  1. Basic Enqueue / Lease / Ack / Fail with no failures
//  2. Worker crash recovery — when a lease expires, the job must be re-leasable
//  3. Bounded duplication under crash + dropped-ack rates of 0.1% to 50%
type JobQueue struct {
	mu      sync.Mutex
	clock   Clock
	jobs    []*job
	nextID  int
	walPath string // path to this queue's WAL file
}

// New constructs a fresh empty queue (no persistence). Used by the test harness.
func New(clock Clock) *JobQueue {
	return &JobQueue{
		clock:   clock,
		jobs:    make([]*job, 0),
		walPath: "", // empty = WAL disabled
	}
}

// NewFromWAL constructs a queue that persists to disk and restores from an
// existing WAL file on startup. Use this in production.
func NewFromWAL(clock Clock, walFile string) *JobQueue {
	q := &JobQueue{
		clock:   clock,
		jobs:    make([]*job, 0),
		walPath: walFile,
	}
	walReplay(q) // restore previous state from disk
	return q
}

// Enqueue stores a new job for processing and returns its unique ID.
//
// TODO:
//   - Generate an ID (any unique identifier)
// enqueueWithPriorityLocked stores a job. Must be called with the lock already held.
func (q *JobQueue) enqueueWithPriorityLocked(payload []byte, priority int) (string, error) {
	q.nextID++
	id := fmt.Sprintf("job-%d", q.nextID)
	j := &job{
		id:         id,
		payload:    payload,
		priority:   priority,
		state:      statePending,
		enqueuedAt: q.clock.Now(),
	}
	q.jobs = append(q.jobs, j)
	if q.walPath != "" {
		walAppend(q.walPath, fmt.Sprintf("ENQUEUE %s %d %d %s", id, q.nextID, j.priority, string(payload)))
	}
	return id, nil
}

func (q *JobQueue) Enqueue(payload []byte) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enqueueWithPriorityLocked(payload, 1)
}

// EnqueueWithPriority is the public version with explicit priority.
func (q *JobQueue) EnqueueWithPriority(payload []byte, priority int) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enqueueWithPriorityLocked(payload, priority)
}

// Lease atomically reserves the next available job for `workerID` for
// `leaseMS` milliseconds. Returns (nil, nil) when no job is available.
//
// TODO:
//   - Find an unleased pending job (or a job whose lease has expired)
//   - Mark it as leased to workerID until clock.Now() + leaseMS
//   - Return the leased job
func (q *JobQueue) Lease(workerID string, leaseMS int64) (*LeasedJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock.Now() // read clock AFTER acquiring the lock
	var best *job
	for _, j := range q.jobs {
		// Must be available
		available := j.state == statePending ||
			(j.state == stateLeased && now >= j.leaseExpiresAt)
		if !available {
			continue
		}
		// Dedup check (from Challenge 1)
		if j.runCount >= maxRunCount {
			continue
		}
		if best == nil {
			best = j
			continue
		}
		// ── Priority comparison with starvation prevention ──
		// A job gets +1 effective priority for every 5000ms it has waited
		waitBoostJ := int((now - j.enqueuedAt) / 5000)
		waitBoostBest := int((now - best.enqueuedAt) / 5000)
		effectivePriorityJ := j.priority + waitBoostJ
		effectivePriorityBest := best.priority + waitBoostBest
		if effectivePriorityJ > effectivePriorityBest {
			best = j
		}
	}
	if best == nil {
		return nil, nil // no job available
	}
	best.state = stateLeased
	best.leasedTo = workerID
	best.leaseExpiresAt = now + leaseMS
	best.runCount++
	if q.walPath != "" {
		walAppend(q.walPath, fmt.Sprintf("LEASED %s %s %d", best.id, workerID, best.leaseExpiresAt))
	}
	return &LeasedJob{
		ID:             best.id,
		Payload:        best.payload,
		LeaseExpiresAt: best.leaseExpiresAt,
	}, nil
}

// Ack marks a job as completed. Should return an error if `workerID` is not
// the current lease holder (e.g. the lease expired and was reassigned).
//
// TODO:
//   - Validate workerID owns the current lease
//   - Mark the job completed
//   - Reject stale acks
func (q *JobQueue) Ack(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock.Now() // read clock AFTER acquiring the lock
	for _, j := range q.jobs {
		if j.id == jobID {
			if j.state != stateLeased ||
				j.leasedTo != workerID ||
				now >= j.leaseExpiresAt {
				return errors.New("ack rejected: stale or invalid lease")
			}
			j.state = stateCompleted
			if q.walPath != "" {
				walAppend(q.walPath, fmt.Sprintf("ACK %s", j.id))
			}
			return nil
		}
	}
	return errors.New("job not found")
}

// Fail releases the lease without completing. The job must become
// immediately re-leasable.
//
// TODO:
//   - Validate workerID owns the lease
//   - Return the job to the pending state
func (q *JobQueue) Fail(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, j := range q.jobs {
		if j.id == jobID {
			if j.leasedTo != workerID {
				return errors.New("fail rejected: not the lease holder")
			}
			j.state = statePending
			j.leasedTo = ""
			j.leaseExpiresAt = 0
			if q.walPath != "" {
				walAppend(q.walPath, fmt.Sprintf("FAIL %s", j.id))
			}
			return nil
		}
	}
	return errors.New("job not found")
}


// Stats returns counts of jobs by state. Used for harness progress checks.
//
// TODO: count pending / leased / completed jobs
func (q *JobQueue) Stats() Stats {
	s := Stats{}
	q.mu.Lock()
    defer q.mu.Unlock()
	for _,j := range q.jobs {
		switch j.state{
		case statePending:
			s.Pending++
		case stateCompleted:
			s.Completed++
		case stateLeased:
			s.Leased++
		}
	}
	return s
}
