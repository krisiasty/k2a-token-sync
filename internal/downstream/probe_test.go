package downstream

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

// ProbeServingCert is the one place this tool decides whether ArgoCD will be able
// to reach a cluster, and the interesting cases are all failures: an endpoint
// whose certificate is untrusted, misnamed or expired must still be *described*,
// because "your certificate expires on Tuesday" is the whole value of the probe.
// So each case below asserts that a rejected certificate is still read.

func TestProbeServingCertAcceptsACertificateSignedByTheClusterCA(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t)
	leaf := ca.issue(t, certOptions{ips: []string{"127.0.0.1"}})
	endpoint := serveTLS(t, leaf)

	cert, err := ProbeServingCert(context.Background(), endpoint, ca.pem)
	if err != nil {
		t.Fatalf("probing a valid endpoint returned an error: %v", err)
	}
	if !cert.TrustedByCA {
		t.Error("a certificate signed by the cluster CA was reported as untrusted")
	}
	if cert.HostnameError != nil {
		t.Errorf("a certificate carrying the endpoint's IP reported a hostname error: %v", cert.HostnameError)
	}
	if cert.DaysRemaining() < 300 {
		t.Errorf("DaysRemaining is %d, want roughly a year", cert.DaysRemaining())
	}
}

func TestProbeServingCertReportsAnUntrustedCertificateWithoutAcceptingIt(t *testing.T) {
	t.Parallel()

	// A different CA entirely: the shape of a cluster whose CA bundle has been
	// rotated, or an endpoint fronted by something other than the API server.
	other := newTestCA(t)
	leaf := other.issue(t, certOptions{ips: []string{"127.0.0.1"}})
	endpoint := serveTLS(t, leaf)

	cert, err := ProbeServingCert(context.Background(), endpoint, newTestCA(t).pem)
	if err != nil {
		t.Fatalf("probing an untrusted endpoint should describe it, not fail: %v", err)
	}
	if cert.TrustedByCA {
		t.Error("a certificate signed by an unrelated CA was reported as trusted")
	}
	if cert.NotAfter.IsZero() {
		t.Error("no expiry was read from the rejected certificate")
	}
}

func TestProbeServingCertNamesTheMissingSAN(t *testing.T) {
	t.Parallel()

	// The common real failure: the serving certificate covers the node names but
	// not the address ArgoCD is pointed at.
	ca := newTestCA(t)
	leaf := ca.issue(t, certOptions{dnsNames: []string{"node-1.example"}})
	endpoint := serveTLS(t, leaf)

	cert, err := ProbeServingCert(context.Background(), endpoint, ca.pem)
	if err != nil {
		t.Fatalf("probing a misnamed endpoint should describe it, not fail: %v", err)
	}
	if cert.HostnameError == nil {
		t.Fatal("a certificate with no SAN for the endpoint reported no hostname error")
	}
	// The message has to be actionable on its own: it is what the operator sees.
	for _, want := range []string{"127.0.0.1", "node-1.example", "SAN"} {
		if !strings.Contains(cert.HostnameError.Error(), want) {
			t.Errorf("the hostname error does not mention %q: %v", want, cert.HostnameError)
		}
	}
	// The chain itself is sound, and saying so separates the two problems.
	if !cert.TrustedByCA {
		t.Error("a certificate signed by the cluster CA was reported as untrusted")
	}
}

func TestProbeServingCertReadsAnExpiredCertificate(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t)
	leaf := ca.issue(t, certOptions{
		ips:       []string{"127.0.0.1"},
		notBefore: time.Now().Add(-48 * time.Hour),
		notAfter:  time.Now().Add(-24 * time.Hour),
	})
	endpoint := serveTLS(t, leaf)

	cert, err := ProbeServingCert(context.Background(), endpoint, ca.pem)
	if err != nil {
		t.Fatalf("probing an expired endpoint should describe it, not fail: %v", err)
	}
	if cert.TrustedByCA {
		t.Error("an expired certificate was reported as trusted")
	}
	if cert.DaysRemaining() >= 0 {
		t.Errorf("DaysRemaining is %d for an expired certificate, want negative", cert.DaysRemaining())
	}
}

func TestProbeServingCertRejectsAnUnusableCABundle(t *testing.T) {
	t.Parallel()

	// Nothing is dialled: a bundle that parses to no certificates can only be a
	// configuration error, and reporting it as "untrusted" would misdirect.
	_, err := ProbeServingCert(context.Background(), "127.0.0.1:1", []byte("not a certificate"))
	if err == nil {
		t.Fatal("an unusable CA bundle was accepted")
	}
	if !strings.Contains(err.Error(), "CA bundle") {
		t.Errorf("the error does not point at the CA bundle: %v", err)
	}
}

// --- test certificate authority ---

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

type certOptions struct {
	dnsNames  []string
	ips       []string
	notBefore time.Time
	notAfter  time.Time
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating a CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-cluster-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating a CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing the CA certificate: %v", err)
	}
	return &testCA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// issue produces a serving certificate, standing in for the one an API server
// presents at its endpoint.
func (ca *testCA) issue(t *testing.T, opts certOptions) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating a serving key: %v", err)
	}

	if opts.notBefore.IsZero() {
		opts.notBefore = time.Now().Add(-time.Hour)
	}
	if opts.notAfter.IsZero() {
		opts.notAfter = time.Now().Add(365 * 24 * time.Hour)
	}
	ips := make([]net.IP, 0, len(opts.ips))
	for _, ip := range opts.ips {
		ips = append(ips, net.ParseIP(ip))
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "kube-apiserver"},
		NotBefore:    opts.notBefore,
		NotAfter:     opts.notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     opts.dnsNames,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating a serving certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// serveTLS listens on a loopback port and completes handshakes for as long as the
// test runs, returning the host:port to probe. Handshake errors are expected —
// most of these tests make the client reject the certificate.
func serveTLS(t *testing.T, cert tls.Certificate) string {
	t.Helper()

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	ctx := t.Context()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				if tlsConn, ok := conn.(*tls.Conn); ok {
					_ = tlsConn.HandshakeContext(ctx)
				}
			}()
		}
	}()

	return listener.Addr().String()
}
