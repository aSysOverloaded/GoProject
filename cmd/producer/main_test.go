package main

import (
	"encoding/json"
	"testing"
)

// TestEveryEnqueueIsUnique covers the root cause of the deadline leak seen on
// Kubernetes. The consumer uses a job's serialised bytes as its identity in
// Redis, so two enqueues of the same logical job must never serialise
// identically - otherwise, while both are in flight, they share one deadline
// entry and one worker's release frees the other's claim.
func TestEveryEnqueueIsUnique(t *testing.T) {
	a, err := json.Marshal(newJob(48))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(newJob(48))
	if err != nil {
		t.Fatal(err)
	}

	if string(a) == string(b) {
		t.Fatalf("two enqueues of job-48 serialised identically:\n  %s\nthey would collide as one deadline member while both are in flight", a)
	}

	// The logical identity must be unchanged; only the per-enqueue identity
	// differs.
	var ja, jb Job
	_ = json.Unmarshal(a, &ja)
	_ = json.Unmarshal(b, &jb)
	if ja.ID != "job-48" || jb.ID != "job-48" {
		t.Errorf("job IDs = %q, %q; want both job-48", ja.ID, jb.ID)
	}
	if ja.EnqueueID == "" || jb.EnqueueID == "" {
		t.Errorf("enqueue IDs must be set, got %q and %q", ja.EnqueueID, jb.EnqueueID)
	}
}
