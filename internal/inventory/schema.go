package inventory

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

// crdName is the ClusterConnection CRD, which is also the single object
// k2a-token-sync's ClusterRole permits it to read by name.
const crdName = "clusterconnections.k2a-token-sync.io"

// crdResource identifies CustomResourceDefinitions for the dynamic client.
//
// Read dynamically rather than through a typed client so that noticing a stale
// schema costs no new module dependency: apiextensions-apiserver would be
// pulled in for one Get, and the dynamic client is already constructed for
// ClusterConnections themselves.
var crdResource = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// SchemaCheck is what comparing the served CRD against this binary found.
//
// The three states are deliberately distinct. Current and stale are the answers
// the check exists to give; "could not verify" is neither, and conflating it
// with stale would fire on every operator who upgraded the image before the
// chart — which is exactly the audience this feature is for.
type SchemaCheck struct {
	// MissingSpecFields and MissingStatusFields name the properties this binary
	// knows that the CRD does not, so the report can say what is being discarded
	// rather than only that something is.
	MissingSpecFields   []string
	MissingStatusFields []string

	// Unverifiable is set when the CRD could not be read at all — most often
	// because the ClusterRole granting the read has not been applied yet.
	Unverifiable error
}

// Stale reports whether the CRD is older than this binary.
func (c SchemaCheck) Stale() bool {
	return len(c.MissingSpecFields) > 0 || len(c.MissingStatusFields) > 0
}

// Missing names every field the API server would discard, spec and status
// together, for a message that has to fit on one line.
func (c SchemaCheck) Missing() string {
	all := make([]string, 0, len(c.MissingSpecFields)+len(c.MissingStatusFields))
	for _, f := range c.MissingSpecFields {
		all = append(all, "spec."+f)
	}
	for _, f := range c.MissingStatusFields {
		all = append(all, "status."+f)
	}
	return strings.Join(all, ", ")
}

// CheckSchema compares the CRD the API server is serving against the fields this
// binary knows about.
//
// A CRD older than the binary is silent by construction, which is the whole
// reason for this. Helm never upgraded a crds/ directory, so a schema could
// trail the image; the API server then prunes spec fields it does not
// recognise, and from inside a pruned field is indistinguishable from one
// nobody set. A connection asking to be scoped below cluster-admin resolves to
// the default instead and reports itself perfectly healthy.
//
// The comparison is one-directional. A CRD carrying more fields than this binary
// knows is a downgrade — the binary ignores what it does not understand, nothing
// is discarded, and there is nothing to report.
func CheckSchema(ctx context.Context, dyn dynamic.Interface) SchemaCheck {
	crd, err := dyn.Resource(crdResource).Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		// Absence is not this check's business: the scheduler already reports a
		// missing CRD, names the remedy, and recovers on its own. Reporting it
		// twice, in different words, would only compete with that.
		if apierrors.IsNotFound(err) {
			return SchemaCheck{}
		}
		return SchemaCheck{Unverifiable: fmt.Errorf("reading the %s CRD: %w", crdName, err)}
	}

	served, err := servedProperties(crd)
	if err != nil {
		return SchemaCheck{Unverifiable: err}
	}

	return SchemaCheck{
		MissingSpecFields:   missingFrom(served["spec"], jsonFieldNames(v1alpha1.ClusterConnectionSpec{})),
		MissingStatusFields: missingFrom(served["status"], jsonFieldNames(v1alpha1.ClusterConnectionStatus{})),
	}
}

// servedProperties pulls the spec and status property names out of the CRD's
// structural schema, for the version this binary speaks.
//
// It looks the version up by name rather than taking the first or the storage
// version: a CRD may serve several, and the only one whose schema governs what
// this binary writes is its own.
func servedProperties(crd *unstructured.Unstructured) (map[string][]string, error) {
	versions, found, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
	if err != nil || !found {
		return nil, fmt.Errorf("the %s CRD declares no versions", crdName)
	}

	for _, raw := range versions {
		version, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(version, "name"); name != GroupVersionResource.Version {
			continue
		}
		out := make(map[string][]string, 2)
		for _, section := range []string{"spec", "status"} {
			props, found, err := unstructured.NestedMap(version,
				"schema", "openAPIV3Schema", "properties", section, "properties")
			if err != nil || !found {
				// A section the schema does not describe at all is reported as
				// having no properties, so every field this binary knows counts
				// as missing — which is the honest reading of a schema that
				// would discard all of them.
				continue
			}
			names := make([]string, 0, len(props))
			for name := range props {
				names = append(names, name)
			}
			out[section] = names
		}
		return out, nil
	}
	return nil, fmt.Errorf("the %s CRD does not serve %s", crdName, GroupVersionResource.Version)
}

// jsonFieldNames reports the JSON names of a struct's fields.
//
// Reflection rather than a hand-kept list, and that is the point: a field added
// to the Go types extends this check by itself. A list would have to be updated
// on every future schema change, which is precisely the kind of obligation that
// gets missed — and a staleness check that quietly stops covering new fields is
// worse than none, because it looks like it is working.
func jsonFieldNames(v any) []string {
	t := reflect.TypeOf(v)
	names := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		tag := t.Field(i).Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			continue
		}
		names = append(names, name)
	}
	return names
}

// missingFrom returns the wanted names the served schema does not carry, sorted
// so a message built from them reads the same on every pass.
func missingFrom(served, wanted []string) []string {
	var missing []string
	for _, name := range wanted {
		if !slices.Contains(served, name) {
			missing = append(missing, name)
		}
	}
	slices.Sort(missing)
	return missing
}
