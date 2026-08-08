package main

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// clusterCollector exports the per-cluster facts worth alerting on.
//
// It reads the same snapshot /status serves, so the two cannot disagree, and it
// reads it at scrape time rather than caching: whatever the last pass established
// is what a scrape sees.
//
// Deliberately absent is anything about this process's own memory or goroutines.
// The Go and process collectors registered alongside already export all of it,
// and export it better — read at scrape time rather than from a sample up to a
// second old. What only this tool knows is the state of the credentials it
// maintains, and that is what these are.
type clusterCollector struct {
	state *healthState

	ready         *prometheus.Desc
	schemaCurrent *prometheus.Desc
	tokenExpiry   *prometheus.Desc
	selfExpiry    *prometheus.Desc
	servingExpiry *prometheus.Desc
	lastSync      *prometheus.Desc
}

func newClusterCollector(state *healthState) *clusterCollector {
	label := []string{"cluster"}
	return &clusterCollector{
		state: state,
		// No cluster label: one CRD serves every connection, so a stale schema
		// is a fact about the process rather than about any one cluster.
		schemaCurrent: prometheus.NewDesc(
			"k2a_token_sync_crd_schema_current",
			"Whether the ClusterConnection CRD matches this binary, 1 or 0. "+
				"0 means the API server is discarding spec fields it does not recognise, so a connection may be "+
				"running with settings it appears to have been given. Absent when the schema could not be read.",
			nil, nil),
		ready: prometheus.NewDesc(
			"k2a_token_sync_cluster_ready",
			"Whether ArgoCD holds a current registration for this cluster, 1 or 0.",
			label, nil),
		tokenExpiry: prometheus.NewDesc(
			"k2a_token_sync_cluster_token_expiration_timestamp_seconds",
			"When the credential published to ArgoCD for this cluster expires.",
			label, nil),
		selfExpiry: prometheus.NewDesc(
			"k2a_token_sync_cluster_self_credential_expiration_timestamp_seconds",
			"When k2a-token-sync's own credential for this cluster expires. Past this it cannot mint, and only bootstrap restores access.",
			label, nil),
		servingExpiry: prometheus.NewDesc(
			"k2a_token_sync_cluster_serving_cert_expiration_timestamp_seconds",
			"When the serving certificate observed at this cluster's endpoint expires.",
			label, nil),
		lastSync: prometheus.NewDesc(
			"k2a_token_sync_cluster_last_sync_timestamp_seconds",
			"When a pass last succeeded for this cluster. A failing cluster stops advancing this while its other series stay current.",
			label, nil),
	}
}

func (c *clusterCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.ready, c.schemaCurrent, c.tokenExpiry, c.selfExpiry, c.servingExpiry, c.lastSync,
	} {
		ch <- d
	}
}

func (c *clusterCollector) Collect(ch chan<- prometheus.Metric) {
	// Exported only once the schema has actually been checked. A gauge reading 1
	// because nothing has run yet would be indistinguishable from one reading 1
	// because the schema is current, and the difference matters most in the
	// seconds after a rollout — the same reason an unknown deadline is omitted
	// rather than exported as zero.
	if check, checked := c.state.schemaCheck(); checked && check.Unverifiable == nil {
		gauge(ch, c.schemaCurrent, boolAsFloat(!check.Stale()))
	}

	for _, report := range c.state.report().Clusters {
		gauge(ch, c.ready, boolAsFloat(report.Synced), report.Name)
		timestamp(ch, c.tokenExpiry, report.TokenExpiresAt, report.Name)
		timestamp(ch, c.selfExpiry, report.SelfCredentialExpiresAt, report.Name)
		timestamp(ch, c.servingExpiry, report.ServingCertExpiresAt, report.Name)
		timestamp(ch, c.lastSync, report.SyncedAt, report.Name)
	}
}

func gauge(ch chan<- prometheus.Metric, desc *prometheus.Desc, value float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

// timestamp exports a deadline as seconds since the epoch, and exports nothing at
// all when there is no deadline to report.
//
// Absolute rather than "seconds remaining", because remaining is only true at the
// instant it is scraped, while `expiration_timestamp_seconds - time()` is correct
// whenever the query runs. Omitting the unknown matters more: a zero would read as
// 1970 and fire every expiry alert at once for a cluster that has simply never
// published yet.
func timestamp(ch chan<- prometheus.Metric, desc *prometheus.Desc, at time.Time, labels ...string) {
	if at.IsZero() {
		return
	}
	gauge(ch, desc, float64(at.Unix()), labels...)
}

func boolAsFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func newMetricsHandler(state *healthState) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		newClusterCollector(state),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
