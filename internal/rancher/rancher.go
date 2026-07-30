// Package rancher talks to the Rancher management API.
//
// Rancher is used strictly on the control path: to reach a downstream cluster
// through its API proxy so the daemon can provision credentials, and to trigger
// RKE2 certificate rotation. ArgoCD never talks to Rancher — the credentials
// this package helps produce point at the downstream cluster directly. That
// keeps Rancher out of the GitOps request path, which is the whole point of the
// exercise: if Rancher is down, reconciliation pauses but ArgoCD keeps working
// until the current token nears expiry.
package rancher

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"k8s.io/client-go/rest"
)

const (
	requestTimeout    = 30 * time.Second
	maxResponseBytes  = 4 << 20
	rotatePollTimeout = 30 * time.Minute
	rotatePollPeriod  = 30 * time.Second

	// stateActive is the Rancher cluster state indicating a healthy cluster.
	stateActive = "active"
)

// Client is a minimal Rancher v3 API client.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
	logger  *slog.Logger
}

// Options configures a Client.
type Options struct {
	BaseURL string
	Token   string

	// CA is an optional PEM bundle used to verify the Rancher endpoint.
	CA []byte

	// InsecureSkipTLSVerify disables verification of the Rancher endpoint.
	// It affects only the control path, never the credentials handed to ArgoCD.
	InsecureSkipTLSVerify bool

	Logger *slog.Logger
}

// New builds a Rancher client.
func New(opts Options) (*Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	switch {
	case opts.InsecureSkipTLSVerify:
		tlsConfig.InsecureSkipVerify = true
	case len(opts.CA) > 0:
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(opts.CA) {
			return nil, fmt.Errorf("rancher CA bundle contains no usable certificates")
		}
		tlsConfig.RootCAs = pool
	}

	return &Client{
		baseURL: opts.BaseURL,
		token:   opts.Token,
		logger:  opts.Logger,
		http: &http.Client{
			Timeout:   requestTimeout,
			Transport: &http.Transport{TLSClientConfig: tlsConfig},
		},
	}, nil
}

// Cluster is the subset of a Rancher cluster we care about.
type Cluster struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

type clusterCollection struct {
	Data []Cluster `json:"data"`
}

// FindCluster resolves a Rancher cluster by its display name.
func (c *Client) FindCluster(ctx context.Context, name string) (*Cluster, error) {
	var collection clusterCollection
	path := "/v3/clusters?name=" + url.QueryEscape(name)
	if err := c.do(ctx, http.MethodGet, path, nil, &collection); err != nil {
		return nil, fmt.Errorf("looking up rancher cluster %q: %w", name, err)
	}

	// Rancher's name filter is a prefix match in some versions, so confirm.
	for i := range collection.Data {
		if collection.Data[i].Name == name {
			return &collection.Data[i], nil
		}
	}
	return nil, fmt.Errorf("rancher has no cluster named %q", name)
}

// GetCluster fetches a single cluster by Rancher ID.
func (c *Client) GetCluster(ctx context.Context, id string) (*Cluster, error) {
	var cluster Cluster
	if err := c.do(ctx, http.MethodGet, "/v3/clusters/"+id, nil, &cluster); err != nil {
		return nil, fmt.Errorf("getting rancher cluster %s: %w", id, err)
	}
	return &cluster, nil
}

// ProxyRESTConfig returns a Kubernetes client configuration that reaches the
// named downstream cluster through the Rancher API proxy.
//
// The proxy is the bootstrap authority: Rancher's agent already holds
// administrative rights in every cluster it manages, so this requires no
// per-cluster credential of our own.
func (c *Client) ProxyRESTConfig(clusterID string, ca []byte, insecure bool) *rest.Config {
	cfg := &rest.Config{
		Host:        fmt.Sprintf("%s/k8s/clusters/%s", c.baseURL, clusterID),
		BearerToken: c.token,
		Timeout:     requestTimeout,
	}
	switch {
	case insecure:
		cfg.TLSClientConfig = rest.TLSClientConfig{Insecure: true}
	case len(ca) > 0:
		cfg.TLSClientConfig = rest.TLSClientConfig{CAData: ca}
	}
	return cfg
}

type rotatePayload struct {
	CACertificates bool     `json:"caCertificates"`
	Services       []string `json:"services,omitempty"`
}

// RotateCertificates triggers an RKE2 certificate rotation and waits for the
// cluster to cycle back to active.
//
// This restarts downstream control-plane components, which is why it is opt-in
// per cluster. The CA is deliberately left alone: rotating it would invalidate
// every kubeconfig and every ArgoCD caData in one step.
func (c *Client) RotateCertificates(ctx context.Context, clusterID string) error {
	payload, err := json.Marshal(rotatePayload{CACertificates: false})
	if err != nil {
		return fmt.Errorf("encoding rotation payload: %w", err)
	}

	path := fmt.Sprintf("/v3/clusters/%s?action=rotateCertificates", clusterID)
	if err := c.do(ctx, http.MethodPost, path, payload, nil); err != nil {
		return fmt.Errorf("triggering certificate rotation for %s: %w", clusterID, err)
	}

	return c.waitForRotation(ctx, clusterID)
}

// waitForRotation waits for the cluster to leave the active state and return to
// it. Waiting only for "active" would match immediately, before Rancher has
// begun the rotation, and report success on a rotation that never happened.
func (c *Client) waitForRotation(ctx context.Context, clusterID string) error {
	deadline := time.Now().Add(rotatePollTimeout)
	left := false

	ticker := time.NewTicker(rotatePollPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		if time.Now().After(deadline) {
			if !left {
				return fmt.Errorf("cluster %s never left the active state; rotation may not have started", clusterID)
			}
			return fmt.Errorf("cluster %s did not return to the active state within %s", clusterID, rotatePollTimeout)
		}

		cluster, err := c.GetCluster(ctx, clusterID)
		if err != nil {
			// Transient proxy or API errors are expected while the control
			// plane restarts; keep polling until the deadline.
			c.logger.Warn("polling cluster state failed, retrying", "cluster_id", clusterID, "error", err)
			continue
		}

		switch {
		case !left && cluster.State != stateActive:
			left = true
			c.logger.Info("rotation started", "cluster_id", clusterID, "state", cluster.State)
		case left && cluster.State == stateActive:
			c.logger.Info("rotation complete, cluster active", "cluster_id", clusterID)
			return nil
		default:
			c.logger.Debug("waiting for rotation", "cluster_id", clusterID, "state", cluster.State, "left_active", left)
		}
	}
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, out any) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("reading response from %s: %w", path, err)
	}
	if len(raw) > maxResponseBytes {
		return fmt.Errorf("response from %s exceeded %d bytes", path, maxResponseBytes)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%s %s: rancher returned HTTP %d: %s", method, path, resp.StatusCode, apiError(raw))
	}

	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decoding response from %s: %w", path, err)
	}
	return nil
}

// apiError extracts Rancher's error message, falling back to a truncated body.
func apiError(raw []byte) string {
	var payload struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	}
	if json.Unmarshal(raw, &payload) == nil && payload.Message != "" {
		if payload.Code != "" {
			return fmt.Sprintf("%s (%s)", payload.Message, payload.Code)
		}
		return payload.Message
	}
	const maxSnippet = 256
	if len(raw) > maxSnippet {
		return string(raw[:maxSnippet]) + "…"
	}
	return string(raw)
}
