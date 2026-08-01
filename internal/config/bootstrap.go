package config

import (
	"errors"
	"fmt"
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

// BootstrapCluster builds a Cluster from command-line input, applying the same
// normalisation and defaults the config file path uses so that a bootstrapped
// cluster and a configured one agree on names, endpoints and secret locations.
func BootstrapCluster(in BootstrapClusterInput) (Cluster, error) {
	var out Cluster

	if in.Name == "" {
		return out, errors.New("cluster name must not be empty")
	}
	if len(in.Name) > maxClusterNameLength {
		return out, fmt.Errorf("cluster name %q exceeds %d characters", in.Name, maxClusterNameLength)
	}

	endpoint, err := normaliseEndpoint(in.Endpoint)
	if err != nil {
		return out, err
	}

	return Cluster{
		Name:                   in.Name,
		Endpoint:               endpoint,
		DisplayName:            in.Name,
		SecretName:             "cluster-" + in.Name,
		SelfServiceAccountName: orDefault(in.SelfServiceAccountName, defaultSelfServiceAccount),
		ServiceAccount: ServiceAccountRef{
			Name:      orDefault(in.ServiceAccountName, defaultServiceAccountName),
			Namespace: orDefault(in.ServiceAccountNamespace, defaultServiceAccountNS),
		},
		TokenTTL:            defaultTokenTTL,
		ExpiryWarnThreshold: defaultExpiryWarnThreshold,
	}, nil
}
