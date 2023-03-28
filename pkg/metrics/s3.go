package metrics

import (
	"github.com/go-kit/kit/metrics/prometheus"
	stdprom "github.com/prometheus/client_golang/prometheus"
)

type S3Metrics struct {
	S3WriteLatency *prometheus.Histogram
	S3ReadLatency  *prometheus.Histogram
	S3WriteSize    *prometheus.Counter
	S3ReadSize     *prometheus.Counter
}

func NewS3Metrics() *S3Metrics {
	return &S3Metrics{
		S3WriteLatency: prometheus.NewHistogramFrom(stdprom.HistogramOpts{
			Name:    "s3_write_latency",
			Help:    "S3 write latency",
			Buckets: getLoadTimeBucket(),
		}, []string{"bucket"}),
		S3ReadLatency: prometheus.NewHistogramFrom(stdprom.HistogramOpts{
			Name:    "s3_read_latency",
			Help:    "S3 read latency",
			Buckets: getLoadTimeBucket(),
		}, []string{"bucket"}),
		S3WriteSize: prometheus.NewCounterFrom(stdprom.CounterOpts{
			Name: "s3_write_size",
			Help: "S3 write size",
		}, []string{"bucket"}),
		S3ReadSize: prometheus.NewCounterFrom(stdprom.CounterOpts{
			Name: "s3_read_size",
			Help: "S3 read size",
		}, []string{"bucket"}),
	}
}

func (m *S3Metrics) ObserveWriteLatency(bucket string, latency float64) {
	m.S3WriteLatency.With("bucket", bucket).Observe(latency)
}

func (m *S3Metrics) ObserveReadLatency(bucket string, latency float64) {
	m.S3ReadLatency.With("bucket", bucket).Observe(latency)
}

func (m *S3Metrics) IncreaseWriteSize(bucket string, size int64) {
	m.S3WriteSize.With("bucket", bucket).Add(float64(size))
}

func (m *S3Metrics) IncreaseReadSize(bucket string, size int64) {
	m.S3ReadSize.With("bucket", bucket).Add(float64(size))
}
