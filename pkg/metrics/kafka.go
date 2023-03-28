package metrics

import (
	"github.com/go-kit/kit/metrics/prometheus"
	stdprom "github.com/prometheus/client_golang/prometheus"
)

type KafkaMetrics struct {
	KafkaWriterOffset *prometheus.Counter
	KafkaReaderOffset *prometheus.Counter
	KafkaCurrentTopic *prometheus.Gauge
	KafkaLatency      *prometheus.Histogram
}

func NewKafkaMetrics() *KafkaMetrics {
	return &KafkaMetrics{
		KafkaWriterOffset: prometheus.NewCounterFrom(stdprom.CounterOpts{
			Name: "kafka_writer_offset",
			Help: "Kafka writer offset",
		}, []string{"topic"}),
		KafkaReaderOffset: prometheus.NewCounterFrom(stdprom.CounterOpts{
			Name: "kafka_reader_offset",
			Help: "Kafka reader offset",
		}, []string{"topic"}),
		KafkaCurrentTopic: prometheus.NewGaugeFrom(stdprom.GaugeOpts{
			Name: "kafka_current_topic",
			Help: "Kafka current topic",
		}, []string{"topic"}),
		KafkaLatency: prometheus.NewHistogramFrom(stdprom.HistogramOpts{
			Name:    "kafka_latency",
			Help:    "Kafka latency",
			Buckets: getLoadTimeBucket(),
		}, []string{"topic"}),
	}
}

func (m *KafkaMetrics) IncreaseWriterOffset(topic string, offset int64) {
	m.KafkaWriterOffset.With(topic).Add(float64(offset))
}

func (m *KafkaMetrics) IncreaseReaderOffset(topic string, offset int64) {
	m.KafkaReaderOffset.With(topic).Add(float64(offset))
}

func (m *KafkaMetrics) SwitchTopic(origin, target string) {
	m.KafkaCurrentTopic.With(origin).Set(0)
	m.KafkaCurrentTopic.With(target).Set(1)
}

func (m *KafkaMetrics) ObserveLatency(topic string, latency float64) {
	m.KafkaLatency.With(topic).Observe(latency)
}
