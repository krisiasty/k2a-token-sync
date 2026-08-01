// Package v1alpha1 defines the ClusterConnection API.
//
// The types here exist for two consumers: controller-gen, which generates the
// CRD schema from them, and k2a-token-sync, which converts listed objects into them
// with runtime.DefaultUnstructuredConverter. There is deliberately no generated
// clientset, no deepcopy code and no scheme registration — the dynamic client
// needs none of it, and each of those would be another artifact to keep in step.
//
// +groupName=k2a-token-sync.io
package v1alpha1
