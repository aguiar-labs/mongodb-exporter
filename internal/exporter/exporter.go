package exporter

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/aguiar-labs/mongodb-exporter/internal/config"
)

// Exporter wires configuration, metrics registry and collection routines.
type Exporter struct {
	cfg      config.Config
	registry *prometheus.Registry

	mongoUp                prometheus.Gauge
	mongoSlowGauge         *prometheus.GaugeVec
	mongoSlowTotal         *prometheus.CounterVec
	mongoSlowAvgMillis     *prometheus.GaugeVec
	mongoSlowMaxMillis     *prometheus.GaugeVec
	mongoSlowExecTimeTotal *prometheus.CounterVec

	indexAccess     *prometheus.CounterVec
	indexAccessRate *prometheus.GaugeVec
	indexSize       *prometheus.GaugeVec
	indexSince      *prometheus.GaugeVec

	lastSeenTs     map[string]time.Time
	lastIndexOps   map[string]int64
	lastIndexSince map[string]time.Time
}

// New creates a configured exporter instance with all metrics registered.
func New(cfg config.Config) *Exporter {
	e := &Exporter{
		cfg:      cfg,
		registry: prometheus.NewRegistry(),
		mongoUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mongodb",
			Name:      "up",
			Help:      "1 if MongoDB ping succeeds, 0 otherwise.",
		}),
		mongoSlowGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mongodb",
				Name:      "slow_queries",
				Help:      "Number of slow operations (≥ slow_ms) per db/collection/command since last interval.",
			},
			[]string{"db", "collection", "command", "app", "plan", "op", "query"},
		),
		mongoSlowTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mongodb",
				Name:      "slow_queries_total",
				Help:      "Cumulative slow operations (≥ slow_ms) per db/collection/command.",
			},
			[]string{"db", "collection", "command", "app", "plan", "op", "query"},
		),
		mongoSlowAvgMillis: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mongodb",
				Name:      "slow_query_avg_ms",
				Help:      "Average latency (ms) of slow operations per db/collection/command.",
			},
			[]string{"db", "collection", "command", "app", "plan"},
		),
		mongoSlowMaxMillis: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mongodb",
				Name:      "slow_query_max_ms",
				Help:      "Maximum latency (ms) of slow operations per db/collection/command.",
			},
			[]string{"db", "collection", "command", "app", "plan"},
		),
		mongoSlowExecTimeTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mongodb",
				Name:      "slow_queries_exec_time_total_ms",
				Help:      "Total execution time (ms) accumulated by slow queries per db/collection/command.",
			},
			[]string{"db", "collection", "command", "app", "plan"},
		),
		indexAccess: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mongodb",
				Name:      "index_access_ops_total",
				Help:      "Cumulative number of index access operations as reported by $indexStats (per db/collection/index).",
			},
			[]string{"db", "collection", "index"},
		),
		indexAccessRate: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mongodb",
				Name:      "index_access_ops_per_second",
				Help:      "Per-interval rate of index access operations computed from $indexStats (per db/collection/index).",
			},
			[]string{"db", "collection", "index"},
		),
		indexSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mongodb",
				Name:      "index_size_bytes",
				Help:      "Index size in bytes from collStats.indexSizes (per db/collection/index).",
			},
			[]string{"db", "collection", "index"},
		),
		indexSince: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "mongodb",
				Name:      "index_access_since_timestamp",
				Help:      "Unix timestamp from $indexStats.accesses.since (per db/collection/index).",
			},
			[]string{"db", "collection", "index"},
		),
		lastSeenTs:     make(map[string]time.Time),
		lastIndexOps:   make(map[string]int64),
		lastIndexSince: make(map[string]time.Time),
	}

	e.registry.MustRegister(
		e.mongoUp,
		e.mongoSlowGauge,
		e.mongoSlowTotal,
		e.mongoSlowAvgMillis,
		e.mongoSlowMaxMillis,
		e.mongoSlowExecTimeTotal,
		e.indexAccess,
		e.indexAccessRate,
		e.indexSize,
		e.indexSince,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return e
}

// Start launches all background collectors using the provided context.
func (e *Exporter) Start(ctx context.Context) {
	go e.runIndexStatsCollector(ctx)
	go e.runSlowCollector(ctx)
	go e.runPinger(ctx)
}

// Registry exposes the Prometheus registry used by the exporter.
func (e *Exporter) Registry() *prometheus.Registry {
	return e.registry
}

func (e *Exporter) clientOptions() *options.ClientOptions {
	return options.Client().
		ApplyURI(e.cfg.MongoURI).
		SetServerSelectionTimeout(e.cfg.ServerSelectionTimeout).
		SetConnectTimeout(e.cfg.ConnectTimeout).
		SetSocketTimeout(e.cfg.SocketTimeout).
		SetMaxPoolSize(e.cfg.MaxPoolSize)
}

func (e *Exporter) shouldCollectCollection(name string) bool {
	if strings.HasPrefix(name, "system.") {
		return false
	}
	if e.cfg.IncludeCollections != nil && !e.cfg.IncludeCollections.MatchString(name) {
		return false
	}
	if e.cfg.ExcludeCollections != nil && e.cfg.ExcludeCollections.MatchString(name) {
		return false
	}
	return true
}

func splitNamespace(ns string) (string, string) {
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", ns
}
