// Package events records the handful of things k2a-token-sync does that are worth
// finding after the fact.
//
// Everything about a connection that is a *state* already lives on the object:
// the conditions and lastAction say how it stands now. What they cannot carry is
// sequence — a credential reissued because ArgoCD's ServiceAccount had been
// deleted, a renewal that failed for a fortnight and then recovered — and
// 'kubectl describe ccon' is the first place anyone looks for that.
//
// What is recorded is deliberately rare. A pass on a healthy cluster writes
// nothing at all, which is what makes a five-minute cadence affordable, and an
// Event per pass would both bury the interesting ones and put an API write back
// into a loop built to avoid them.
//
// This is not k8s.io/client-go/tools/record. That wants a runtime.Scheme with
// ClusterConnection registered, which api/v1alpha1 deliberately does not provide,
// and a broadcaster goroutine whose aggregation exists to tame recorders that
// fire constantly. Neither earns its keep for a handful of Events that fire on
// transitions.
package events

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// component names this tool as the source of an Event. It is what 'kubectl
// describe' prints in the From column.
const component = "k2a-token-sync"

// writeTimeout bounds one Event write.
//
// Recording is best-effort and must never hold anything up: these calls sit
// inside a reconciliation pass and inside the poll loop, and an API server that
// accepts a connection and then goes quiet would otherwise stall whichever of the
// two was unlucky.
const writeTimeout = 10 * time.Second

// ObjectReferencer names the object an Event is about.
//
// The UID is why this is a lookup rather than a reference assembled from a name.
// kubectl's Events section field-selects on involvedObject.uid, so an Event
// carrying only a name and a namespace is accepted by the API server and then
// never shown by the one command it exists for.
type ObjectReferencer interface {
	Reference(ctx context.Context, name string) (corev1.ObjectReference, error)
}

// Recorder writes Events about ClusterConnections in k2a-token-sync's own
// namespace, which is the namespace those objects live in.
type Recorder struct {
	client    kubernetes.Interface
	namespace string
	refs      ObjectReferencer
	logger    *slog.Logger

	now func() time.Time
}

// New builds a Recorder.
func New(client kubernetes.Interface, namespace string, refs ObjectReferencer, logger *slog.Logger) *Recorder {
	return &Recorder{
		client:    client,
		namespace: namespace,
		refs:      refs,
		logger:    logger,
		now:       time.Now,
	}
}

// Normal records something that went as intended.
func (r *Recorder) Normal(ctx context.Context, cluster, reason, message string) {
	r.emit(ctx, cluster, corev1.EventTypeNormal, reason, message)
}

// Warning records something an operator would want to have known about.
func (r *Recorder) Warning(ctx context.Context, cluster, reason, message string) {
	r.emit(ctx, cluster, corev1.EventTypeWarning, reason, message)
}

// emit writes one Event, or explains in the log why it could not.
//
// Nothing here returns an error, and that is the contract rather than an
// oversight: an Event is a record of work that has already happened, so failing
// to write one must not be able to fail the work. Returning an error would invite
// exactly one of two mistakes at every call site — ignoring it, or acting on it.
//
// An Event lost because the pass's context has already expired is one such case.
// The condition still carries the state, so what is lost is the note in the
// history rather than the fact itself.
func (r *Recorder) emit(ctx context.Context, cluster, eventType, reason, message string) {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	ref, err := r.refs.Reference(ctx, cluster)
	if err != nil {
		r.logger.Warn("could not record an event against the ClusterConnection",
			"cluster", cluster, "event_reason", reason, "error", err)
		return
	}

	event := r.event(ref, eventType, reason, message)
	if _, err := r.client.CoreV1().Events(r.namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil {
		r.logger.Warn("could not record an event against the ClusterConnection",
			"cluster", cluster, "event_reason", reason, "error", err)
	}
}

// event is the Event as it goes to the API server.
//
// The legacy core/v1 shape: a source and a first and last timestamp, rather than
// an eventTime with a reporting controller and an action. That is what the API
// server accepts without further required fields, and what 'kubectl describe'
// renders; the series machinery the newer shape exists for has nothing here to
// aggregate.
func (r *Recorder) event(ref corev1.ObjectReference, eventType, reason, message string) *corev1.Event {
	now := r.now()
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      eventName(ref.Name, reason, now),
			Namespace: r.namespace,
		},
		InvolvedObject: ref,
		Reason:         reason,
		Message:        message,
		Type:           eventType,
		Source:         corev1.EventSource{Component: component},
		FirstTimestamp: metav1.NewTime(now),
		LastTimestamp:  metav1.NewTime(now),
		Count:          1,
	}
}

// eventName is unique by construction, and predictable, which generateName is
// not: the fake clientset does not implement it, so a name the server assigns
// cannot be exercised in a test.
//
// The reason is in there because the clock alone is not enough. One pass can
// restore an identity and reissue a credential, and on a platform whose wall
// clock is coarse those two land in the same nanosecond. A cluster reconciles
// serially and emits at most one Event per reason per pass, so cluster plus
// reason plus the clock cannot collide.
func eventName(cluster, reason string, now time.Time) string {
	return fmt.Sprintf("%s.%s.%x", cluster, strings.ToLower(reason), now.UnixNano())
}
