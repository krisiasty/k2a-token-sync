package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRuntimeTelemetryTracksAndResetsIntervalMaxima(t *testing.T) {
	startedAt := time.Unix(1_700_000_000, 0)
	now := startedAt.Add(time.Second)
	values := []runtimeValues{
		{
			goroutines:           10,
			osThreads:            4,
			heapAllocBytes:       100,
			heapInuseBytes:       220,
			stackInuseBytes:      50,
			runtimeReservedBytes: 500,
			heapObjects:          20,
		},
		{
			goroutines:           8,
			osThreads:            6,
			heapAllocBytes:       140,
			heapInuseBytes:       200,
			stackInuseBytes:      70,
			runtimeReservedBytes: 480,
			heapObjects:          25,
		},
	}
	next := 0
	telemetry := &runtimeTelemetry{
		logger:    slog.New(slog.DiscardHandler),
		startedAt: startedAt,
		now:       func() time.Time { return now },
		read: func() runtimeValues {
			value := values[next]
			next++
			return value
		},
	}

	telemetry.sample()
	now = startedAt.Add(6 * time.Second)
	telemetry.sample()

	current, maximum := telemetry.snapshot()
	if current != (runtimeValues{
		uptime:               6 * time.Second,
		goroutines:           8,
		osThreads:            6,
		heapAllocBytes:       140,
		heapInuseBytes:       200,
		stackInuseBytes:      70,
		runtimeReservedBytes: 480,
		heapObjects:          25,
	}) {
		t.Fatalf("current values = %+v", current)
	}
	if maximum != (runtimeValues{
		uptime:               6 * time.Second,
		goroutines:           10,
		osThreads:            6,
		heapAllocBytes:       140,
		heapInuseBytes:       220,
		stackInuseBytes:      70,
		runtimeReservedBytes: 500,
		heapObjects:          25,
	}) {
		t.Fatalf("maximum values = %+v", maximum)
	}

	reportedCurrent, reportedMaximum := telemetry.takeReport()
	if reportedCurrent != current || reportedMaximum != maximum {
		t.Fatalf("report = (%+v, %+v), want (%+v, %+v)", reportedCurrent, reportedMaximum, current, maximum)
	}
	_, resetMaximum := telemetry.snapshot()
	if resetMaximum != current {
		t.Fatalf("maximum after report = %+v, want current %+v", resetMaximum, current)
	}
}

func TestRuntimeTelemetryLogContainsCurrentAndMaximumGroups(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	startedAt := time.Unix(1_700_000_000, 0)
	telemetry := &runtimeTelemetry{
		logger:    logger,
		startedAt: startedAt,
		now:       func() time.Time { return startedAt.Add(10 * time.Second) },
		read: func() runtimeValues {
			return runtimeValues{goroutines: 7, osThreads: 3, heapAllocBytes: 100, heapInuseBytes: 150,
				stackInuseBytes: 20, runtimeReservedBytes: 300, heapObjects: 11}
		},
		maximum: runtimeValues{goroutines: 9, osThreads: 4, heapAllocBytes: 120, heapInuseBytes: 180,
			stackInuseBytes: 25, runtimeReservedBytes: 350, heapObjects: 13},
		initialized: true,
	}

	telemetry.logReport()

	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode telemetry log: %v", err)
	}
	if event["msg"] != "runtime telemetry" {
		t.Fatalf("message = %v", event["msg"])
	}
	current, ok := event["current"].(map[string]any)
	if !ok {
		t.Fatalf("current group = %#v", event["current"])
	}
	maximum, ok := event["max"].(map[string]any)
	if !ok {
		t.Fatalf("max group = %#v", event["max"])
	}
	if current["uptime_seconds"] != float64(10) || current["goroutines"] != float64(7) {
		t.Fatalf("current group = %#v", current)
	}
	if maximum["goroutines"] != float64(9) || maximum["runtime_reserved_bytes"] != float64(350) {
		t.Fatalf("max group = %#v", maximum)
	}
}

func TestMetricsEndpointExportsCurrentAndIntervalMaximum(t *testing.T) {
	telemetry := &runtimeTelemetry{
		current: runtimeValues{
			uptime: 7*time.Second + 500*time.Millisecond, goroutines: 5, osThreads: 3,
			heapAllocBytes: 100, heapInuseBytes: 150, stackInuseBytes: 20,
			runtimeReservedBytes: 300, heapObjects: 11,
		},
		maximum: runtimeValues{
			uptime: 8 * time.Second, goroutines: 9, osThreads: 4,
			heapAllocBytes: 120, heapInuseBytes: 180, stackInuseBytes: 25,
			runtimeReservedBytes: 350, heapObjects: 13,
		},
		initialized: true,
	}
	handler := newHealthHandler(
		slog.New(slog.DiscardHandler),
		newHealthState(),
		newMetricsHandler(telemetry),
	)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain") {
		t.Fatalf("Content-Type = %q", contentType)
	}

	body := recorder.Body.String()
	for _, metric := range []string{
		"k2a_token_sync_runtime_uptime_seconds 7.5",
		"k2a_token_sync_runtime_uptime_interval_max_seconds 8",
		"k2a_token_sync_runtime_goroutines 5",
		"k2a_token_sync_runtime_goroutines_interval_max 9",
		"k2a_token_sync_runtime_os_threads 3",
		"k2a_token_sync_runtime_heap_alloc_bytes 100",
		"k2a_token_sync_runtime_heap_inuse_bytes 150",
		"k2a_token_sync_runtime_stack_inuse_bytes 20",
		"k2a_token_sync_runtime_reserved_bytes 300",
		"k2a_token_sync_runtime_heap_objects 11",
		"go_goroutines ",
		"process_start_time_seconds ",
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metrics response does not contain %q", metric)
		}
	}
}

func TestReadRuntimeValues(t *testing.T) {
	values := readRuntimeValues()
	if values.goroutines == 0 {
		t.Error("goroutine count is zero")
	}
	if values.osThreads == 0 {
		t.Error("OS thread count is zero")
	}
	if values.heapAllocBytes == 0 || values.heapObjects == 0 {
		t.Errorf("heap values = %+v", values)
	}
	if values.heapInuseBytes < values.heapAllocBytes {
		t.Errorf("heap in use %d is below allocated heap %d", values.heapInuseBytes, values.heapAllocBytes)
	}
	if values.runtimeReservedBytes < values.heapInuseBytes+values.stackInuseBytes {
		t.Errorf("runtime reserved bytes are inconsistent: %+v", values)
	}
}

func TestTelemetrySamplingReportingAndCollectionAreConcurrentSafe(t *testing.T) {
	var value atomic.Uint64
	telemetry := &runtimeTelemetry{
		logger:    slog.New(slog.DiscardHandler),
		startedAt: time.Now(),
		now:       time.Now,
		read: func() runtimeValues {
			n := value.Add(1)
			return runtimeValues{
				goroutines: n, osThreads: n, heapAllocBytes: n, heapInuseBytes: n,
				stackInuseBytes: n, runtimeReservedBytes: n, heapObjects: n,
			}
		},
	}
	telemetry.sample()

	collector := newRuntimeTelemetryCollector(telemetry)
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			telemetry.sample()
		}
	})
	wg.Go(func() {
		for range 100 {
			telemetry.takeReport()
		}
	})
	wg.Go(func() {
		for range 100 {
			if _, err := registry.Gather(); err != nil {
				t.Errorf("gather metrics: %v", err)
			}
		}
	})
	wg.Wait()
}

func TestTelemetryIntervals(t *testing.T) {
	if telemetrySampleInterval != time.Second {
		t.Fatalf("sample interval = %v", telemetrySampleInterval)
	}
	if telemetryReportInterval != 10*time.Minute {
		t.Fatalf("report interval = %v", telemetryReportInterval)
	}
}
