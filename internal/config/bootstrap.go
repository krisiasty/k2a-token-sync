package config

import (
	"errors"
	"fmt"

	"github.com/krisiasty/k2a-token-sync/api/v1alpha1"
)

// BootstrapClusterInput is the subset of cluster settings the bootstrap
// subcommand collects from flags.
type BootstrapClusterInput struct {
	Name                    string
	Endpoint                string
	ServiceAccountName      string
	ServiceAccountNamespace string
	SelfServiceAccountName  string
}

// BootstrapCluster builds a Cluster from command-line input.
//
// It builds a spec and resolves it through FromSpec rather than assembling a
// Cluster directly. Bootstrap has to reach the same conclusions the API path
// does, and it runs before any object exists — so nothing has applied the
// schema's defaults, and this is the only place they can come from.
//
// Assembling the struct here duplicated that, and the duplication cost a live
// failure: SelfTokenTTL was added to Cluster and filled in on the API path only,
// leaving bootstrap to ask the API server for a token with expirationSeconds: 0.
// Going through FromSpec means a field can no longer be set on one path and
// forgotten on the other.
func BootstrapCluster(in BootstrapClusterInput) (Cluster, error) {
	if in.Name == "" {
		return Cluster{}, errors.New("cluster name must not be empty")
	}
	if len(in.Name) > maxClusterNameLength {
		return Cluster{}, fmt.Errorf("cluster name %q exceeds %d characters", in.Name, maxClusterNameLength)
	}

	spec := v1alpha1.ClusterConnectionSpec{
		Endpoint:               in.Endpoint,
		SelfServiceAccountName: in.SelfServiceAccountName,
	}
	// Left nil unless asked for, so FromSpec applies the same default the schema
	// would rather than a half-populated reference.
	if in.ServiceAccountName != "" || in.ServiceAccountNamespace != "" {
		spec.ServiceAccount = &v1alpha1.ServiceAccountRef{
			Name:      in.ServiceAccountName,
			Namespace: in.ServiceAccountNamespace,
		}
	}

	return FromSpec(in.Name, spec)
}
