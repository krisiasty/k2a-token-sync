package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

	current, maximum := telemetry.current, telemetry.maximum
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
	resetMaximum := telemetry.maximum
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

func TestTelemetrySamplingAndReportingAreConcurrentSafe(t *testing.T) {
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
	wg.Wait()

	// The invariant the locking exists to hold: a maximum is only ever raised by a
	// sample, and reset to the sample it was reset at, so it can never end up
	// behind the current value. A torn read or a lost update shows up here.
	current, maximum := telemetry.current, telemetry.maximum
	for _, field := range []struct {
		name             string
		current, maximum uint64
	}{
		{"goroutines", current.goroutines, maximum.goroutines},
		{"os_threads", current.osThreads, maximum.osThreads},
		{"heap_alloc_bytes", current.heapAllocBytes, maximum.heapAllocBytes},
		{"heap_inuse_bytes", current.heapInuseBytes, maximum.heapInuseBytes},
		{"stack_inuse_bytes", current.stackInuseBytes, maximum.stackInuseBytes},
		{"runtime_reserved_bytes", current.runtimeReservedBytes, maximum.runtimeReservedBytes},
		{"heap_objects", current.heapObjects, maximum.heapObjects},
	} {
		if field.maximum < field.current {
			t.Errorf("%s: maximum %d is behind current %d", field.name, field.maximum, field.current)
		}
	}
}

func TestTelemetryIntervals(t *testing.T) {
	if telemetrySampleInterval != time.Second {
		t.Fatalf("sample interval = %v", telemetrySampleInterval)
	}
	if telemetryReportInterval != 10*time.Minute {
		t.Fatalf("report interval = %v", telemetryReportInterval)
	}
}
