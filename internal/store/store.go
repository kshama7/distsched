// Package store is the durable BoltDB-backed persistence layer for the
// scheduler. Jobs and workers are stored as protobuf-marshaled values keyed by
// ID. The in-memory priority queue is always a cache rebuilt from this store on
// startup; this package is the system of record.
package store

import (
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	distschedv1 "github.com/kshama7/distsched/gen/go/distsched/v1"
)

// ErrNotFound is returned when a key is absent.
var ErrNotFound = errors.New("not found")

var (
	bucketJobs       = []byte("jobs")
	bucketWorkers    = []byte("workers")
	bucketDeadLetter = []byte("dead_letter")
	bucketMeta       = []byte("meta")
)

// Store wraps a BoltDB database.
type Store struct {
	db *bolt.DB
}

// Open opens (creating if needed) the BoltDB file at path and ensures the
// required buckets exist.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt %q: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketJobs, bucketWorkers, bucketDeadLetter, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("create bucket %q: %w", b, err)
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close flushes and closes the database.
func (s *Store) Close() error { return s.db.Close() }

// PutJob durably upserts a job.
func (s *Store) PutJob(job *distschedv1.Job) error {
	data, err := proto.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job %q: %w", job.GetId(), err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketJobs).Put([]byte(job.GetId()), data)
	})
}

// GetJob loads a job by ID, returning ErrNotFound if absent.
func (s *Store) GetJob(id string) (*distschedv1.Job, error) {
	job := &distschedv1.Job{}
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketJobs).Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		return proto.Unmarshal(data, job)
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// DeleteJob removes a job; deleting a missing job is a no-op.
func (s *Store) DeleteJob(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketJobs).Delete([]byte(id))
	})
}

// ListJobs returns all jobs. Ordering is BoltDB key order (by ID); callers that
// need priority ordering use the queue.
func (s *Store) ListJobs() ([]*distschedv1.Job, error) {
	var jobs []*distschedv1.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketJobs).ForEach(func(_, v []byte) error {
			job := &distschedv1.Job{}
			if err := proto.Unmarshal(v, job); err != nil {
				return err
			}
			jobs = append(jobs, job)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// PutWorker durably upserts a worker record.
func (s *Store) PutWorker(w *distschedv1.WorkerInfo) error {
	data, err := proto.Marshal(w)
	if err != nil {
		return fmt.Errorf("marshal worker %q: %w", w.GetId(), err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorkers).Put([]byte(w.GetId()), data)
	})
}

// GetWorker loads a worker by ID, returning ErrNotFound if absent.
func (s *Store) GetWorker(id string) (*distschedv1.WorkerInfo, error) {
	w := &distschedv1.WorkerInfo{}
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketWorkers).Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		return proto.Unmarshal(data, w)
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

// ListWorkers returns all known workers.
func (s *Store) ListWorkers() ([]*distschedv1.WorkerInfo, error) {
	var workers []*distschedv1.WorkerInfo
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorkers).ForEach(func(_, v []byte) error {
			w := &distschedv1.WorkerInfo{}
			if err := proto.Unmarshal(v, w); err != nil {
				return err
			}
			workers = append(workers, w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return workers, nil
}

// DeleteWorker removes a worker record.
func (s *Store) DeleteWorker(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorkers).Delete([]byte(id))
	})
}

// PutDeadLetter records a job that exhausted its retries. The job also remains
// in the jobs bucket in the FAILED state; the dead-letter bucket is a dedicated
// index for operators to inspect failures.
func (s *Store) PutDeadLetter(job *distschedv1.Job) error {
	data, err := proto.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal dead-letter job %q: %w", job.GetId(), err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDeadLetter).Put([]byte(job.GetId()), data)
	})
}

// ListDeadLetter returns all dead-lettered jobs.
func (s *Store) ListDeadLetter() ([]*distschedv1.Job, error) {
	var jobs []*distschedv1.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDeadLetter).ForEach(func(_, v []byte) error {
			job := &distschedv1.Job{}
			if err := proto.Unmarshal(v, job); err != nil {
				return err
			}
			jobs = append(jobs, job)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// CountDeadLetter returns the number of dead-lettered jobs.
func (s *Store) CountDeadLetter() (int, error) {
	n := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(bucketDeadLetter).Stats().KeyN
		return nil
	})
	return n, err
}
