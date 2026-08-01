#!/usr/bin/env bash
# Regenerates the CRD schema from the Go types in api/v1alpha1.
#
# The generated YAML is committed. CI runs this script and fails if the tree
# changes, so the schema cannot drift from the types it is generated from.
#
# controller-gen is invoked with 'go run pkg@version', which resolves in
# isolation and touches neither go.mod nor the module graph: a codegen tool run
# twice a year should not get a vote on the versions the daemon is built with.
set -euo pipefail

CONTROLLER_GEN_VERSION=v0.19.0

cd "$(dirname "$0")/.."

go run "sigs.k8s.io/controller-tools/cmd/controller-gen@${CONTROLLER_GEN_VERSION}" \
  crd \
  paths=./api/... \
  output:crd:dir=charts/k2a-token-sync/crds

echo "wrote charts/k2a-token-sync/crds"
