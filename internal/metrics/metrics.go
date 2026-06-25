// Package metrics defines the Prometheus instrumentation for a scheduler node.
// Each node owns a private registry (so several in one process — e.g. tests —
// never collide), exposing event counters plus a live collector that reads
// current queue/lease/worker state on scrape.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// LiveSources supplies current values sampled at scrape time.
type LiveSources struct {
	QueueDepth      func() float64
	InFlight        func() float64
	DeadLetterTotal func() float64
	IsLeader        func() float64
	Term            func() float64
	// WorkersByState maps a worker state label to a count.
	WorkersByState func() map[string]float64
}

// Metrics holds the counters and registry for one node.
type Metrics struct {
	Reg *prometheus.Registry

	JobsSubmitted    prometheus.Counter
	JobsSucceeded    prometheus.Counter
	JobsDeadLettered prometheus.Counter
	JobsCanceled     prometheus.Counter
	TasksDispatched  prometheus.Counter
	TasksRetried     prometheus.Counter
	TasksRequeued    prometheus.Counter
}

// New builds the metrics for a node and registers the counters and the live
// collector on a fresh registry.
func New(src LiveSources) *Metrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	m := &Metrics{
		Reg: reg,
		JobsSubmitted: f.NewCounter(prometheus.CounterOpts{
			Name: "distsched_jobs_submitted_total", Help: "Jobs accepted via SubmitJob.",
		}),
		JobsSucceeded: f.NewCounter(prometheus.CounterOpts{
			Name: "distsched_jobs_succeeded_total", Help: "Jobs that reached SUCCEEDED.",
		}),
		JobsDeadLettered: f.NewCounter(prometheus.CounterOpts{
			Name: "distsched_jobs_dead_lettered_total", Help: "Jobs that exhausted retries and were dead-lettered.",
		}),
		JobsCanceled: f.NewCounter(prometheus.CounterOpts{
			Name: "distsched_jobs_canceled_total", Help: "Jobs canceled (directly or by a failed dependency).",
		}),
		TasksDispatched: f.NewCounter(prometheus.CounterOpts{
			Name: "distsched_tasks_dispatched_total", Help: "Task attempts leased to workers.",
		}),
		TasksRetried: f.NewCounter(prometheus.CounterOpts{
			Name: "distsched_tasks_retried_total", Help: "Task attempts rescheduled after a failure.",
		}),
		TasksRequeued: f.NewCounter(prometheus.CounterOpts{
			Name: "distsched_tasks_requeued_total", Help: "Task attempts requeued after a lost worker or expired lease.",
		}),
	}
	reg.MustRegister(newLiveCollector(src))
	return m
}

var (
	queueDepthDesc = prometheus.NewDesc("distsched_queue_depth", "Ready jobs in the priority queue.", nil, nil)
	inFlightDesc   = prometheus.NewDesc("distsched_inflight_tasks", "Outstanding task leases.", nil, nil)
	deadLetterDesc = prometheus.NewDesc("distsched_dead_letter_size", "Jobs currently in the dead-letter queue.", nil, nil)
	leaderDesc     = prometheus.NewDesc("distsched_is_leader", "1 if this node is the cluster leader, else 0.", nil, nil)
	termDesc       = prometheus.NewDesc("distsched_election_term", "Current election term.", nil, nil)
	workersDesc    = prometheus.NewDesc("distsched_workers", "Registered workers by liveness state.", []string{"state"}, nil)
)

type liveCollector struct{ src LiveSources }

func newLiveCollector(src LiveSources) *liveCollector { return &liveCollector{src: src} }

func (c *liveCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueDepthDesc
	ch <- inFlightDesc
	ch <- deadLetterDesc
	ch <- leaderDesc
	ch <- termDesc
	ch <- workersDesc
}

func (c *liveCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue, c.src.QueueDepth())
	ch <- prometheus.MustNewConstMetric(inFlightDesc, prometheus.GaugeValue, c.src.InFlight())
	ch <- prometheus.MustNewConstMetric(deadLetterDesc, prometheus.GaugeValue, c.src.DeadLetterTotal())
	ch <- prometheus.MustNewConstMetric(leaderDesc, prometheus.GaugeValue, c.src.IsLeader())
	ch <- prometheus.MustNewConstMetric(termDesc, prometheus.GaugeValue, c.src.Term())
	for state, n := range c.src.WorkersByState() {
		ch <- prometheus.MustNewConstMetric(workersDesc, prometheus.GaugeValue, n, state)
	}
}
