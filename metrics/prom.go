package metrics

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	httpReqLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "eventsum",
		Subsystem: "http_server",
		Name:      "request_latency_ms",
		Help:      "Latency in ms of http requests grouped by req path",
		Buckets:   buckets(),
	}, []string{"path"})

	httpStatus = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "eventsum",
		Subsystem: "http_server",
		Name:      "status_count",
		Help:      "The count of http responses issued classified by status and api endpoint",
	}, []string{"path", "code"})

	eventStoreDbErrCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "eventsum",
		Subsystem: "event_store",
		Name:      "db_error",
		Help:      "The count of db errors by db name and type of operation",
	}, []string{"operation"})

	eventStoreTimer = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "eventmaster",
		Subsystem: "event_store",
		Name:      "method_time",
		Help:      "Time of event store methods by method name",
		Buckets:   buckets(),
	}, []string{"method"})
)

// RegisterPromMetrics registers all the metrics that eventsum uses.
func RegisterPromMetrics() error {
	if err := prometheus.Register(httpReqLatencies); err != nil {
		return errors.Wrap(err, "registering http request latency")
	}

	if err := prometheus.Register(httpStatus); err != nil {
		return errors.Wrap(err, "registering http response counter")
	}

	if err := prometheus.Register(eventStoreDbErrCounter); err != nil {
		return errors.Wrap(err, "registering event store errors")
	}

	return nil
}

// msSince returns milliseconds since start.
func msSince(start time.Time) float64 {
	return float64(time.Since(start) / time.Millisecond)
}

// buckets returns the default prometheus buckets scaled to milliseconds.
func buckets() []float64 {
	r := []float64{}

	for _, v := range prometheus.DefBuckets {
		r = append(r, v*float64(time.Second/time.Millisecond))
	}
	return r
}
