// Package inventory reads the cluster inventory from ClusterConnection objects.
//
// Discovery is a plain List on a timer rather than an informer. The inventory
// changes when a human adds a cluster — a few times a year — so a watch would
// buy sub-second pickup in exchange for a cache with a lifecycle, watch
// reconnects and a workqueue to deduplicate events. A list has none of that, and
// it doubles as the recovery mechanism: whatever k2a-token-sync's view was, the next
// poll replaces it.
package inventory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
	"github.com/krisiasty/k2a-token-sync/internal/config"
)

// GroupVersionResource identifies ClusterConnection objects.
var GroupVersionResource = schema.GroupVersionResource{
	Group:    "k2a-token-sync.io",
	Version:  "v1alpha1",
	Resource: "clusterconnections",
}

// Entry is one inventory entry: the resolved cluster, plus the object bookkeeping
// k2a-token-sync needs to write status back and to notice edits.
type Entry struct {
	Cluster config.Cluster

	// Generation and ObservedGeneration are compared to decide whether the spec
	// has changed since k2a-token-sync last acted on it. A spec ahead of its status
	// is due immediately, which is what makes an edit take effect within one poll
	// rather than at the next scheduled pass.
	Generation         int64
	ObservedGeneration int64

	// Status is the last status k2a-token-sync wrote, and the only record of what it
	// published: it holds no read permission on the generated Secrets.
	Status v1alpha1.ClusterConnectionStatus

	// InvalidReason is set when the entry cannot be reconciled: its spec does not
	// resolve, or another connection claims the same Secret. Such an entry is
	// reported on the object rather than dropped silently, since a cluster
	// vanishing from the inventory because of a typo is the worst of both outcomes.
	InvalidReason string

	// InvalidCause says which of those it is, as one of the condition reasons in
	// the API package. The message above is for a person; this is what decides
	// which conditions get written to the object, and the two kinds want different
	// ones: a spec only this object can fix, against a conflict that names another.
	InvalidCause string
}

// Edited reports whether the spec has changed since k2a-token-sync last recorded a
// status for it.
func (e Entry) Edited() bool {
	return e.Generation != e.ObservedGeneration
}

// Client lists and updates ClusterConnection objects in one namespace.
type Client struct {
	resource dynamic.ResourceInterface
}

// NewClient binds a dynamic client to k2a-token-sync's own namespace.
//
// Objects are namespaced and only this namespace is watched, so k2a-token-sync needs
// a Role rather than a ClusterRole and holds no cluster-scoped permission.
func NewClient(dyn dynamic.Interface, namespace string) *Client {
	return &Client{resource: dyn.Resource(GroupVersionResource).Namespace(namespace)}
}

// List returns every ClusterConnection in the namespace, resolved into clusters.
//
// Entries whose spec cannot be resolved are returned with InvalidReason set,
// rather than dropped, since admission cannot see cross-object conflicts and a
// cluster vanishing from the inventory over a typo is the worst outcome.
func (c *Client) List(ctx context.Context) ([]Entry, error) {
	list, err := c.resource.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing clusterconnections: %w", err)
	}

	entries := make([]Entry, 0, len(list.Items))
	for i := range list.Items {
		entries = append(entries, decode(&list.Items[i]))
	}

	blockContestedSecrets(entries)
	return entries, nil
}

// blockContestedSecrets holds back every claimant when two or more connections
// resolve to the same Secret.
//
// The obvious alternative — let the first one seen keep it — reads as though it
// protects the incumbent, and does the opposite. List order is not an ownership
// boundary: it is alphabetical, so adding a connection whose name happens to sort
// earlier makes the newcomer the owner. It would then publish its own endpoint and
// credential over a Secret another cluster is still registered under, while the
// dispossessed one is quietly excluded from reconciliation, its status frozen at
// whatever it last said. ArgoCD would go on trusting a registration that now points
// somewhere else entirely.
//
// There is no answer to "which of these should win" that this tool can safely
// invent, so it declines to choose. Nothing is written until a person removes the
// ambiguity, which costs a stalled cluster and saves a misdirected one.
func blockContestedSecrets(entries []Entry) {
	claimants := make(map[string][]string, len(entries))
	for _, entry := range entries {
		if entry.InvalidReason != "" || entry.Cluster.SecretName == "" {
			continue
		}
		claimants[entry.Cluster.SecretName] = append(claimants[entry.Cluster.SecretName], entry.Cluster.Name)
	}

	for i := range entries {
		contenders := claimants[entries[i].Cluster.SecretName]
		if entries[i].InvalidReason != "" || len(contenders) < 2 {
			continue
		}

		others := make([]string, 0, len(contenders)-1)
		for _, name := range contenders {
			if name != entries[i].Cluster.Name {
				others = append(others, name)
			}
		}
		// Sorted so the message reads the same however the API returned the list;
		// it is written into status, where a value that reshuffles on its own would
		// look like something changed.
		sort.Strings(others)

		entries[i].InvalidReason = fmt.Sprintf(
			"secretName %q is also claimed by %s; none of them will be reconciled until one claim remains",
			entries[i].Cluster.SecretName, strings.Join(quoted(others), ", "))
		entries[i].InvalidCause = v1alpha1.ReasonSecretNameConflict
	}
}

func quoted(names []string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, strconv.Quote(name))
	}
	return out
}

// UpdateStatus writes the status subresource for one connection.
func (c *Client) UpdateStatus(ctx context.Context, name string, status v1alpha1.ClusterConnectionStatus) error {
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
	if err != nil {
		return fmt.Errorf("encoding status for %s: %w", name, err)
	}

	// Fetching first keeps the update honest about resourceVersion: a status
	// write that clobbered a concurrent spec edit would lose the edit.
	current, err := c.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading %s before writing status: %w", name, err)
	}
	if err := unstructured.SetNestedMap(current.Object, raw, "status"); err != nil {
		return fmt.Errorf("setting status on %s: %w", name, err)
	}

	if _, err := c.resource.UpdateStatus(ctx, current, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("updating status of %s: %w", name, err)
	}
	return nil
}

// Reference names one ClusterConnection as the involvedObject of an Event.
//
// It is a read because of the UID. kubectl's Events section field-selects on
// involvedObject.uid, so a reference assembled from the name alone produces an
// Event the API server accepts and 'kubectl describe' never shows — the one
// command it exists for. Kind and apiVersion come off the object for the same
// reason: they are also selected on, and taking them from the object is one fewer
// thing to keep in step with the CRD.
//
// One extra read per Event, which is affordable precisely because Events here are
// rare: an unchanged pass records none.
func (c *Client) Reference(ctx context.Context, name string) (corev1.ObjectReference, error) {
	obj, err := c.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return corev1.ObjectReference{}, fmt.Errorf("reading %s to record an event against it: %w", name, err)
	}
	return corev1.ObjectReference{
		APIVersion: obj.GetAPIVersion(),
		Kind:       obj.GetKind(),
		Namespace:  obj.GetNamespace(),
		Name:       obj.GetName(),
		UID:        obj.GetUID(),
	}, nil
}

// decode converts one object into an Entry, resolving its spec into the runtime
// cluster type.
//
// It never fails. Every way an object can be unusable is reported on the entry
// instead, because the caller writes that verdict back to the object, and an
// entry it never received is an object that says nothing about why it is being
// ignored.
func decode(obj *unstructured.Unstructured) Entry {
	// Taken from the object rather than from the decoded spec, so that they survive
	// an object the Go types cannot parse at all.
	entry := Entry{
		Cluster:    config.Cluster{Name: obj.GetName()},
		Generation: obj.GetGeneration(),
		Status:     decodeStatus(obj),
	}
	entry.ObservedGeneration = entry.Status.ObservedGeneration

	var cc v1alpha1.ClusterConnection
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &cc); err != nil {
		entry.InvalidReason = fmt.Sprintf("decoding clusterconnection: %v", err)
		entry.InvalidCause = v1alpha1.ReasonInvalidSpec
		return entry
	}

	cluster, err := config.FromSpec(cc.Name, cc.Spec)
	if err != nil {
		entry.InvalidReason = err.Error()
		entry.InvalidCause = v1alpha1.ReasonInvalidSpec
		return entry
	}
	entry.Cluster = cluster
	return entry
}

// decodeStatus decodes the status subtree on its own.
//
// A spec these types cannot parse says nothing about the status beside it, and
// the status is worth keeping: it records the fingerprint of what was last
// published, so discarding it would make fixing the spec cost a needless
// reissue. If the status itself will not decode there is nothing to preserve,
// and an empty one is the honest answer.
func decodeStatus(obj *unstructured.Unstructured) v1alpha1.ClusterConnectionStatus {
	raw, found, err := unstructured.NestedMap(obj.Object, "status")
	if err != nil || !found {
		return v1alpha1.ClusterConnectionStatus{}
	}

	var status v1alpha1.ClusterConnectionStatus
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &status); err != nil {
		return v1alpha1.ClusterConnectionStatus{}
	}
	return status
}

// IsCRDMissing reports whether the ClusterConnection CRD itself is absent,
// which is worth distinguishing from an empty inventory: one means the chart's
// crds/ were never applied, the other means no clusters are declared yet.
func IsCRDMissing(err error) bool {
	return meta.IsNoMatchError(err) || apierrors.IsNotFound(err)
}
