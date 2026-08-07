package reconcile

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
)

// recordedEvent is one Event a pass asked for.
type recordedEvent struct {
	Type    string
	Cluster string
	Reason  string
	Message string
}

// fakeRecorder collects what a pass recorded, so a test can assert what an Event
// said and — the more important half — that nothing was recorded at all.
type fakeRecorder struct {
	mu     sync.Mutex
	events []recordedEvent
}

func (f *fakeRecorder) Normal(_ context.Context, cluster, reason, message string) {
	f.add(corev1.EventTypeNormal, cluster, reason, message)
}

func (f *fakeRecorder) Warning(_ context.Context, cluster, reason, message string) {
	f.add(corev1.EventTypeWarning, cluster, reason, message)
}

func (f *fakeRecorder) add(eventType, cluster, reason, message string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, recordedEvent{eventType, cluster, reason, message})
}

func (f *fakeRecorder) recorded() []recordedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

// recorderFor reaches the fake that newReconciler installs.
func recorderFor(t *testing.T, r *Reconciler) *fakeRecorder {
	t.Helper()
	rec, ok := r.events.(*fakeRecorder)
	if !ok {
		t.Fatalf("the reconciler's recorder is %T, want the test fake", r.events)
	}
	return rec
}

// only asserts that exactly one Event was recorded and returns it.
func only(t *testing.T, events []recordedEvent) recordedEvent {
	t.Helper()
	if len(events) != 1 {
		t.Fatalf("%d events were recorded, want exactly 1: %+v", len(events), events)
	}
	return events[0]
}

// The property that keeps the five-minute cadence affordable. A pass on a healthy
// cluster is a genuine no-op — nothing written to ArgoCD, nothing written to the
// downstream cluster — and an Event would put an API write back into it, one per
// cluster per five minutes, burying the rare ones this exists for.
//
// The first pass here establishes the state a healthy cluster is in; the second is
// the one under test, and it has to record nothing at all.
func TestAnUnchangedPassRecordsNothing(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h := newPassHarness(t)

	first, err := h.reconciler.Cluster(ctx, h.cluster, v1alpha1.ClusterConnectionStatus{}, 1)
	if err != nil {
		t.Fatalf("the first pass failed, so there is no steady state to test: %v", err)
	}
	// One Event, for the credential the first pass had to publish. Asserted here
	// because it is also what proves the second pass reached the same code and chose
	// differently, rather than failing early somewhere before any of it.
	reissued := only(t, h.recorder.recorded())
	if reissued.Reason != v1alpha1.ReasonCredentialReissued {
		t.Fatalf("the first pass recorded %q, want %q", reissued.Reason, v1alpha1.ReasonCredentialReissued)
	}

	second, err := h.reconciler.Cluster(ctx, h.cluster, first, 1)
	if err != nil {
		t.Fatalf("the second pass failed: %v", err)
	}
	if second.LastAction != "up-to-date" {
		t.Fatalf("lastAction after the second pass is %q, want %q — the pass was not the no-op this tests",
			second.LastAction, "up-to-date")
	}

	if events := h.recorder.recorded(); len(events) != 1 {
		t.Errorf("an unchanged pass recorded %d further events, want none: %+v",
			len(events)-1, events[1:])
	}
}

// A deleted ServiceAccount is the case this whole feature is for. Nothing about
// the published Secret changes — it still carries a bearer token and the
// fingerprint still matches — so the only trace of the repair is a log line from
// whichever pass happened to catch it.
func TestARestoredIdentityIsRecorded(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h := newPassHarness(t)

	first, err := h.reconciler.Cluster(ctx, h.cluster, v1alpha1.ClusterConnectionStatus{}, 1)
	if err != nil {
		t.Fatalf("the first pass failed: %v", err)
	}

	// What a person does, and the only thing this tool depends on that they can.
	if err := h.downstream.CoreV1().ServiceAccounts("kube-system").
		Delete(ctx, "argocd-manager", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting ArgoCD's ServiceAccount: %v", err)
	}

	h.recorder = &fakeRecorder{}
	h.reconciler.events = h.recorder

	if _, err := h.reconciler.Cluster(ctx, h.cluster, first, 1); err != nil {
		t.Fatalf("the pass that should have restored the identity failed: %v", err)
	}

	events := h.recorder.recorded()
	restored, found := findEvent(events, v1alpha1.ReasonIdentityRestored)
	if !found {
		t.Fatalf("no %s event was recorded: %+v", v1alpha1.ReasonIdentityRestored, events)
	}
	if restored.Type != corev1.EventTypeWarning {
		t.Errorf("the event is %s, want %s: a deleted identity is not routine",
			restored.Type, corev1.EventTypeWarning)
	}
	// The message has to say which half was missing. A recreated ServiceAccount has
	// a new UID, so ArgoCD's token stopped authenticating; a recreated binding
	// leaves a token that works perfectly well.
	for _, want := range []string{"kube-system/argocd-manager", "ServiceAccount", "reissued"} {
		if !strings.Contains(restored.Message, want) {
			t.Errorf("the message does not mention %q: %q", want, restored.Message)
		}
	}

	// And the reissue it forces, since a token bound to the old UID is dead.
	if _, found := findEvent(events, v1alpha1.ReasonCredentialReissued); !found {
		t.Errorf("no %s event accompanied the restored identity: %+v",
			v1alpha1.ReasonCredentialReissued, events)
	}
}

// Renewal failing is a state, and the condition is what carries it. What is worth
// an Event is the moment it started — and if that were recorded on every failing
// pass instead, a renewal broken for a fortnight would bury it under four thousand
// copies of itself.
func TestAFailingRenewalIsRecordedOnceAndAgainWhenTheReasonChanges(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cluster := testCluster()
	expires := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// No token reactor, and a reactor that refuses: minting a replacement fails.
	downstreamClient := fake.NewClientset(rootCA())
	downstreamClient.PrependReactor("create", "serviceaccounts",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() != "token" {
				return false, nil, nil
			}
			return true, nil, errors.New("the API server refused to mint a token")
		})

	r := newReconciler(fake.NewClientset(storedCredential(expires)), func() (kubernetes.Interface, error) {
		return fake.NewClientset(rootCA()), nil
	})
	recorder := recorderFor(t, r)

	// Due for renewal: issued a day ago, which is the interval.
	status := v1alpha1.ClusterConnectionStatus{
		SelfCredentialExpiresAt: &metav1.Time{Time: expires},
		SelfCredentialIssuedAt:  &metav1.Time{Time: r.now().Add(-25 * time.Hour)},
	}
	access := &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: expires}

	r.maintainSelfCredential(ctx, cluster, access, &status, r.logger, r.now())

	failed := only(t, recorder.recorded())
	if failed.Type != corev1.EventTypeWarning {
		t.Errorf("the event is %s, want %s", failed.Type, corev1.EventTypeWarning)
	}
	// The same reason the condition carries, so that 'kubectl describe' and
	// 'kubectl get ccon' do not name one failure two different ways.
	if failed.Reason != v1alpha1.ReasonRenewalMintFailed {
		t.Errorf("the event reason is %q, want the condition's %q",
			failed.Reason, v1alpha1.ReasonRenewalMintFailed)
	}
	if !strings.Contains(failed.Message, "bootstrapped again") {
		t.Errorf("the message does not say what running out costs: %q", failed.Message)
	}

	// The failure continues, and the object already says so.
	r.maintainSelfCredential(ctx, cluster, access, &status, r.logger, r.now())
	if events := recorder.recorded(); len(events) != 1 {
		t.Fatalf("a renewal that was already failing recorded %d events, want 1: %+v", len(events), events)
	}

	// A different reason points at a different place to go and look, so it is news
	// again: minting now works, and it is verification that fails.
	mintsTokens(downstreamClient, time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC))
	r.clientForToken = func(_, _ string, _ []byte) (kubernetes.Interface, error) {
		return fake.NewClientset(), nil // cannot read the CA: the token does not work
	}

	r.maintainSelfCredential(ctx, cluster, access, &status, r.logger, r.now())

	events := recorder.recorded()
	if len(events) != 2 {
		t.Fatalf("a renewal failing for a new reason recorded %d events in total, want 2: %+v",
			len(events), events)
	}
	if events[1].Reason != v1alpha1.ReasonRenewalUnverified {
		t.Errorf("the second event reason is %q, want %q",
			events[1].Reason, v1alpha1.ReasonRenewalUnverified)
	}
}

// The other half of the pair. Recovery is only interesting against a failure that
// was recorded: there is one ordinary renewal per cluster per day, and recording
// those would be the noise this feature exists to avoid.
func TestRecoveryIsRecordedOnlyAfterAFailure(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cluster := testCluster()
	oldExpiry := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	newExpiry := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	renewalWorks := func(t *testing.T) (*Reconciler, *fakeRecorder, *clusterAccess) {
		t.Helper()
		downstreamClient := fake.NewClientset(rootCA())
		mintsTokens(downstreamClient, newExpiry)
		r := newReconciler(fake.NewClientset(storedCredential(oldExpiry)), func() (kubernetes.Interface, error) {
			return fake.NewClientset(rootCA()), nil
		})
		return r, recorderFor(t, r), &clusterAccess{client: downstreamClient, ca: testCA, expiresAt: oldExpiry}
	}

	dueForRenewal := func(r *Reconciler) v1alpha1.ClusterConnectionStatus {
		return v1alpha1.ClusterConnectionStatus{
			SelfCredentialExpiresAt: &metav1.Time{Time: oldExpiry},
			SelfCredentialIssuedAt:  &metav1.Time{Time: r.now().Add(-25 * time.Hour)},
		}
	}

	t.Run("an ordinary renewal records nothing", func(t *testing.T) {
		t.Parallel()
		r, recorder, access := renewalWorks(t)
		status := dueForRenewal(r)

		r.maintainSelfCredential(ctx, cluster, access, &status, r.logger, r.now())

		if events := recorder.recorded(); len(events) != 0 {
			t.Errorf("a routine daily renewal recorded %d events, want none: %+v", len(events), events)
		}
	})

	t.Run("a renewal after a failure records the recovery", func(t *testing.T) {
		t.Parallel()
		r, recorder, access := renewalWorks(t)
		status := dueForRenewal(r)
		// What the previous pass left behind, which is the only thing that makes this
		// pass a recovery rather than a Tuesday.
		setCondition(&status, v1alpha1.ConditionSelfCredentialValid, metav1.ConditionFalse,
			v1alpha1.ReasonRenewalMintFailed, "the API server refused to mint a token", 1)

		r.maintainSelfCredential(ctx, cluster, access, &status, r.logger, r.now())

		recovered := only(t, recorder.recorded())
		if recovered.Type != corev1.EventTypeNormal {
			t.Errorf("the event is %s, want %s", recovered.Type, corev1.EventTypeNormal)
		}
		if recovered.Reason != v1alpha1.ReasonRenewalRecovered {
			t.Errorf("the event reason is %q, want %q", recovered.Reason, v1alpha1.ReasonRenewalRecovered)
		}
		// It names what had been failing, since that is what someone reading the
		// history is trying to pair this with.
		if !strings.Contains(recovered.Message, v1alpha1.ReasonRenewalMintFailed) {
			t.Errorf("the message does not say what had been failing: %q", recovered.Message)
		}
		if !strings.Contains(recovered.Message, newExpiry.Format(time.RFC3339)) {
			t.Errorf("the message does not carry the replacement's expiry: %q", recovered.Message)
		}
	})
}

func findEvent(events []recordedEvent, reason string) (recordedEvent, bool) {
	for _, event := range events {
		if event.Reason == reason {
			return event, true
		}
	}
	return recordedEvent{}, false
}

// passHarness is everything a whole pass needs, which is more than the other tests
// in this package require: a real TLS endpoint, because the serving-certificate
// probe opens a connection to see what ArgoCD would encounter and cannot be
// answered by a clientset.
type passHarness struct {
	reconciler *Reconciler
	recorder   *fakeRecorder
	cluster    config.Cluster
	downstream *fake.Clientset
}

func newPassHarness(t *testing.T) *passHarness {
	t.Helper()

	endpoint, caPEM := testEndpoint(t)

	cluster := config.Cluster{
		Name:                   "standalone-1",
		Endpoint:               endpoint,
		SecretName:             "cluster-standalone-1",
		SelfServiceAccountName: "k2a-token-sync",
		TokenTTL:               720 * time.Hour,
		SelfTokenTTL:           2160 * time.Hour,
		ExpiryWarnThreshold:    2160 * time.Hour,
		ServiceAccount:         config.ServiceAccountRef{Name: "argocd-manager", Namespace: "kube-system"},
	}

	// ArgoCD's identity is already in place, so a healthy pass has nothing to
	// repair. The tests that want a repair remove it.
	downstreamClient := fake.NewClientset(
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: "kube-system"},
			Data:       map[string]string{"ca.crt": string(caPEM)},
		},
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager", Namespace: "kube-system"},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "argocd-manager-role-binding"},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cluster-admin"},
			Subjects: []rbacv1.Subject{{
				Kind: rbacv1.ServiceAccountKind, Name: "argocd-manager", Namespace: "kube-system",
			}},
		},
	)
	// Generous enough that neither credential is anywhere near due at the frozen
	// clock below, so a second pass has nothing to do.
	mintsTokens(downstreamClient, time.Date(2027, 8, 1, 12, 0, 0, 0, time.UTC))
	allowsClusterAdmin(t, downstreamClient)

	local := fake.NewClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-1-credentials", Namespace: testNamespace},
		Data: map[string][]byte{
			"token":      []byte(oldToken),
			"ca.crt":     caPEM,
			"expires-at": []byte(time.Date(2027, 1, 1, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)),
		},
	})

	r := newReconciler(local, func() (kubernetes.Interface, error) { return downstreamClient, nil })

	return &passHarness{
		reconciler: r,
		recorder:   recorderFor(t, r),
		cluster:    cluster,
		downstream: downstreamClient,
	}
}

// allowsClusterAdmin answers the access review a pass runs against a credential
// it has just minted.
//
// A reactor rather than a seeded object, because the fake clientset cannot handle
// SelfSubjectAccessReview at all: it has no schema for the type, so the create
// fails outright and the pass reads that as a credential the API server refused.
func allowsClusterAdmin(t *testing.T, client *fake.Clientset) {
	t.Helper()
	client.PrependReactor("create", "selfsubjectaccessreviews",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, &authorizationv1.SelfSubjectAccessReview{
				Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true},
			}, nil
		})
}

// testEndpoint serves TLS on a loopback port with a certificate signed by the CA
// it returns, standing in for a downstream API server at its direct endpoint.
//
// The probe is the one part of a pass that cannot be answered by a fake clientset:
// it exists to observe what ArgoCD will actually encounter. A test claiming a pass
// recorded nothing has to have got past it for the claim to mean anything.
func testEndpoint(t *testing.T) (endpoint string, caPEM []byte) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating a CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-cluster-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating a CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parsing the CA certificate: %v", err)
	}

	servingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating a serving key: %v", err)
	}
	servingTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "kube-apiserver"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	servingDER, err := x509.CreateCertificate(rand.Reader, servingTemplate, caCert, &servingKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating a serving certificate: %v", err)
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{servingDER}, PrivateKey: servingKey}},
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
			// The handshake has to complete: it is the only thing the probe wants, and
			// closing before it does looks to the caller like an endpoint that hung up.
			go func() {
				defer func() { _ = conn.Close() }()
				if tlsConn, ok := conn.(*tls.Conn); ok {
					_ = tlsConn.HandshakeContext(ctx)
				}
			}()
		}
	}()

	return listener.Addr().String(), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}
