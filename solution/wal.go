package solution

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// walAppend writes one log entry to the given WAL file path.
// Format: "VERB jobID [extra fields]\n"
func walAppend(path string, line string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, line)
	return err
}

// walReplay reads the WAL file at q.walPath and replays entries into the queue.
// Called once at startup inside NewFromWAL().
func walReplay(q *JobQueue) error {
	f, err := os.Open(q.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no WAL yet — fresh start, not an error
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line) // split by whitespace, like Python's line.split()
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "ENQUEUE":
			// Format: ENQUEUE <id> <nextID> <priority> <payload>
			if len(parts) < 5 {
				continue
			}
			id := parts[1]
			nextID, _ := strconv.Atoi(parts[2])
			priority, _ := strconv.Atoi(parts[3])
			payload := []byte(strings.Join(parts[4:], " "))
			if nextID > q.nextID {
				q.nextID = nextID
			}
			q.jobs = append(q.jobs, &job{
				id:         id,
				payload:    payload,
				state:      statePending,
				priority:   priority,
				enqueuedAt: 0,
			})

		case "LEASED":
			// Format: LEASED <jobID> <workerID> <expiresAt>
			if len(parts) < 4 {
				continue
			}
			jobID := parts[1]
			workerID := parts[2]
			expires, _ := strconv.ParseInt(parts[3], 10, 64)
			for _, j := range q.jobs {
				if j.id == jobID {
					j.state = stateLeased
					j.leasedTo = workerID
					j.leaseExpiresAt = expires
				}
			}

		case "ACK":
			// Format: ACK <jobID>
			if len(parts) < 2 {
				continue
			}
			for _, j := range q.jobs {
				if j.id == parts[1] {
					j.state = stateCompleted
				}
			}

		case "FAIL":
			// Format: FAIL <jobID>
			if len(parts) < 2 {
				continue
			}
			for _, j := range q.jobs {
				if j.id == parts[1] {
					j.state = statePending
					j.leasedTo = ""
					j.leaseExpiresAt = 0
				}
			}
		}
	}
	return scanner.Err()
}
