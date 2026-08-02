package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// scrape returns the metrics as a map of series to value.
//
// Parsed rather than string-matched because Prometheus writes floats in
// exponent form, so a timestamp asserted as a literal would be a test of
// formatting rather than of the value.
func scrape(t *testing.T, state *healthState) map[string]float64 {
	t.Helper()

	handler := newHealthHandler(slog.New(slog.DiscardHandler), state, newMetricsHandler(state))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("scrape returned %d: %s", recorder.Code, recorder.Body.String())
	}

	series := map[string]float64{}
	for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cut := strings.LastIndex(line, " ")
		if cut < 0 {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(line[cut+1:]), 64)
		if err != nil {
			continue // exemplars and other trailing forms are not under test
		}
		series[line[:cut]] = value
	}
	return series
}

func TestClusterGaugesReportTheStatusSnapshot(t *testing.T) {
	t.Parallel()

	token := time.Date(2026, 9, 1, 14, 14, 16, 0, time.UTC)
	self := time.Date(2026, 10, 31, 14, 9, 16, 0, time.UTC)
	cert := time.Date(2027, 7, 31, 13, 28, 52, 0, time.UTC)
	synced := time.Date(2026, 8, 2, 18, 42, 25, 0, time.UTC)

	state := newHealthState()
	state.record([]clusterReport{
		{
			Name: "downstream-1", Synced: true,
			TokenExpiresAt: token, SelfCredentialExpiresAt: self,
			ServingCertExpiresAt: cert, SyncedAt: synced,
		},
		{Name: "downstream-2", Synced: false, TokenExpiresAt: token, SyncedAt: synced},
	}, time.Minute)

	series := scrape(t, state)
	want := map[string]float64{
		`k2a_token_sync_cluster_ready{cluster="downstream-1"}`:                                        1,
		`k2a_token_sync_cluster_ready{cluster="downstream-2"}`:                                        0,
		`k2a_token_sync_cluster_token_expiration_timestamp_seconds{cluster="downstream-1"}`:           float64(token.Unix()),
		`k2a_token_sync_cluster_self_credential_expiration_timestamp_seconds{cluster="downstream-1"}`: float64(self.Unix()),
		`k2a_token_sync_cluster_serving_cert_expiration_timestamp_seconds{cluster="downstream-1"}`:    float64(cert.Unix()),
		`k2a_token_sync_cluster_last_sync_timestamp_seconds{cluster="downstream-1"}`:                  float64(synced.Unix()),
		`k2a_token_sync_cluster_token_expiration_timestamp_seconds{cluster="downstream-2"}`:           float64(token.Unix()),
	}
	for name, expected := range want {
		got, found := series[name]
		if !found {
			t.Errorf("%s is missing from the metric", name)
			continue
		}
		if got != expected {
			t.Errorf("%s = %v, want %v", name, got, expected)
		}
	}
}

// A deadline nobody knows must be absent, not zero.
//
// Zero is 1970 once it reaches Prometheus, so a cluster that has simply never
// published yet would satisfy every "expires soon" alert at once — the metric
// would be loudest exactly where it knows least.
func TestAnUnknownDeadlineIsOmittedRatherThanExportedAsZero(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	state.record([]clusterReport{{Name: "awaiting-credential", Synced: false}}, time.Minute)

	series := scrape(t, state)
	for _, name := range []string{
		`k2a_token_sync_cluster_token_expiration_timestamp_seconds{cluster="awaiting-credential"}`,
		`k2a_token_sync_cluster_self_credential_expiration_timestamp_seconds{cluster="awaiting-credential"}`,
		`k2a_token_sync_cluster_serving_cert_expiration_timestamp_seconds{cluster="awaiting-credential"}`,
		`k2a_token_sync_cluster_last_sync_timestamp_seconds{cluster="awaiting-credential"}`,
	} {
		if value, found := series[name]; found {
			t.Errorf("%s was exported as %v; an unknown deadline must be absent", name, value)
		}
	}
	// The cluster is still visible, just without deadlines it does not have.
	if _, found := series[`k2a_token_sync_cluster_ready{cluster="awaiting-credential"}`]; !found {
		t.Error("the cluster vanished entirely instead of reporting not-ready")
	}
}

// Series follow the inventory. A cluster removed from it stops being exported
// rather than freezing at its last value, which would age into a false alert.
func TestSeriesDisappearWithTheCluster(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	state.record([]clusterReport{{Name: "leaving", Synced: true}, {Name: "staying", Synced: true}}, time.Minute)
	if _, found := scrape(t, state)[`k2a_token_sync_cluster_ready{cluster="leaving"}`]; !found {
		t.Fatal("the cluster was not exported in the first place")
	}

	state.record([]clusterReport{{Name: "staying", Synced: true}}, time.Minute)
	series := scrape(t, state)
	if _, found := series[`k2a_token_sync_cluster_ready{cluster="leaving"}`]; found {
		t.Error("a cluster no longer in the inventory is still exported")
	}
	if _, found := series[`k2a_token_sync_cluster_ready{cluster="staying"}`]; !found {
		t.Error("the remaining cluster stopped being exported")
	}
}

// The endpoint must not re-export what the runtime already reports.
//
// It did: eight custom gauges sat beside the standard collectors describing the
// same quantities, sampled a second earlier. This is the guard against that
// coming back — the runtime numbers belong to the Go and process collectors, and
// what this tool adds is what only it knows.
func TestTheEndpointDoesNotRestateTheRuntimeCollectors(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	state.record([]clusterReport{{Name: "downstream-1", Synced: true}}, time.Minute)
	series := scrape(t, state)

	for name := range series {
		if strings.HasPrefix(name, "k2a_token_sync_runtime_") {
			t.Errorf("%s duplicates a metric the standard collectors already export", name)
		}
	}
	for _, standard := range []string{"go_goroutines", "go_threads", "go_memstats_heap_alloc_bytes", "process_start_time_seconds"} {
		if _, found := series[standard]; !found {
			t.Errorf("%s is missing; the standard collectors are what supply the runtime view", standard)
		}
	}
}

// The whole chain for the deadline that matters most: a status read from the API,
// through the pass, into the snapshot, out of the endpoint. Constructing a
// clusterReport by hand tests the collector but not the plumbing that fills it.
func TestTheSelfCredentialDeadlineReachesTheEndpoint(t *testing.T) {
	t.Parallel()

	self := time.Date(2026, 10, 31, 14, 9, 16, 0, time.UTC)
	inv := newFakeInventory("downstream-1")
	status := readyStatus()
	status.SelfCredentialExpiresAt = &metav1.Time{Time: self}
	inv.prior["downstream-1"] = status

	s := testScheduler(t, inv, newFakeReconciler())
	s.tick(t.Context())
	s.wg.Wait()

	const name = `k2a_token_sync_cluster_self_credential_expiration_timestamp_seconds{cluster="downstream-1"}`
	got, found := scrape(t, s.health)[name]
	if !found {
		t.Fatal("the self-credential deadline never reached the endpoint")
	}
	if got != float64(self.Unix()) {
		t.Errorf("%s = %v, want %v", name, got, float64(self.Unix()))
	}
}
