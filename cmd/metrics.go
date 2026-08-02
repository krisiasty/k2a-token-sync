package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type runtimeMetricDefinition struct {
	current *prometheus.Desc
	maximum *prometheus.Desc
	value   func(runtimeValues) float64
}

// runtimeTelemetryCollector exports the same sampled values as the periodic
// log. The interval maxima reset after each log; Prometheus retains their time
// series and can aggregate them over longer windows.
type runtimeTelemetryCollector struct {
	telemetry *runtimeTelemetry
	metrics   []runtimeMetricDefinition
}

func newRuntimeTelemetryCollector(telemetry *runtimeTelemetry) *runtimeTelemetryCollector {
	definition := func(name, maximumName, help string, value func(runtimeValues) float64) runtimeMetricDefinition {
		return runtimeMetricDefinition{
			current: prometheus.NewDesc(name, "Current "+help+".", nil, nil),
			maximum: prometheus.NewDesc(maximumName, "Maximum sampled "+help+" since the previous telemetry log.", nil, nil),
			value:   value,
		}
	}

	return &runtimeTelemetryCollector{
		telemetry: telemetry,
		metrics: []runtimeMetricDefinition{
			definition(
				"k2a_token_sync_runtime_uptime_seconds",
				"k2a_token_sync_runtime_uptime_interval_max_seconds",
				"process uptime in seconds",
				func(v runtimeValues) float64 { return v.uptime.Seconds() },
			),
			definition(
				"k2a_token_sync_runtime_goroutines",
				"k2a_token_sync_runtime_goroutines_interval_max",
				"number of live goroutines",
				func(v runtimeValues) float64 { return float64(v.goroutines) },
			),
			definition(
				"k2a_token_sync_runtime_os_threads",
				"k2a_token_sync_runtime_os_threads_interval_max",
				"number of live OS threads owned by the Go runtime",
				func(v runtimeValues) float64 { return float64(v.osThreads) },
			),
			definition(
				"k2a_token_sync_runtime_heap_alloc_bytes",
				"k2a_token_sync_runtime_heap_alloc_interval_max_bytes",
				"bytes occupied by allocated heap objects",
				func(v runtimeValues) float64 { return float64(v.heapAllocBytes) },
			),
			definition(
				"k2a_token_sync_runtime_heap_inuse_bytes",
				"k2a_token_sync_runtime_heap_inuse_interval_max_bytes",
				"bytes in heap spans that contain objects",
				func(v runtimeValues) float64 { return float64(v.heapInuseBytes) },
			),
			definition(
				"k2a_token_sync_runtime_stack_inuse_bytes",
				"k2a_token_sync_runtime_stack_inuse_interval_max_bytes",
				"bytes reserved for goroutine stacks",
				func(v runtimeValues) float64 { return float64(v.stackInuseBytes) },
			),
			definition(
				"k2a_token_sync_runtime_reserved_bytes",
				"k2a_token_sync_runtime_reserved_interval_max_bytes",
				"bytes of read-write memory mapped by the Go runtime",
				func(v runtimeValues) float64 { return float64(v.runtimeReservedBytes) },
			),
			definition(
				"k2a_token_sync_runtime_heap_objects",
				"k2a_token_sync_runtime_heap_objects_interval_max",
				"number of live or not-yet-swept heap objects",
				func(v runtimeValues) float64 { return float64(v.heapObjects) },
			),
		},
	}
}

func (c *runtimeTelemetryCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metrics {
		ch <- metric.current
		ch <- metric.maximum
	}
}

func (c *runtimeTelemetryCollector) Collect(ch chan<- prometheus.Metric) {
	current, maximum := c.telemetry.snapshot()
	for _, metric := range c.metrics {
		ch <- prometheus.MustNewConstMetric(metric.current, prometheus.GaugeValue, metric.value(current))
		ch <- prometheus.MustNewConstMetric(metric.maximum, prometheus.GaugeValue, metric.value(maximum))
	}
}

func newMetricsHandler(telemetry *runtimeTelemetry) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		newRuntimeTelemetryCollector(telemetry),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
