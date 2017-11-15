package metrics

import (
	"fmt"
	"time"
)

// EventStoreLatency records eventstore latency for a named op.
func EventStoreLatency(op string, start time.Time) {
	eventStoreTimer.WithLabelValues(op).Observe(msSince(start))
}

// DBError increments a counter for a db error operation.
func DBError(op string) {
	eventStoreDbErrCounter.WithLabelValues(op).Inc()
}

// HTTPLatency records the latency of http calls is ms.
func HTTPLatency(path string, start time.Time) {
	httpReqLatencies.WithLabelValues(path).Observe(msSince(start))
}

func HTTPStatus(path string, status int) {
	httpStatus.WithLabelValues(path, fmt.Sprintf("%d", bucketHTTPStatus(status))).Inc()
}

// bucketHTTPStatus rounds down to the nearest hundred to facilitate categorizing http statuses.
func bucketHTTPStatus(i int) int {
	return i - i%100
}
