{{- define "r2a-cert-sync.name" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "r2a-cert-sync.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/name: {{ include "r2a-cert-sync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "r2a-cert-sync.selectorLabels" -}}
app.kubernetes.io/name: {{ include "r2a-cert-sync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "r2a-cert-sync.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{/*
Distinct namespaces the generated ArgoCD cluster Secrets land in. The daemon
needs a Role in each one, so this drives RBAC rendering.
*/}}
{{- define "r2a-cert-sync.secretNamespaces" -}}
{{- $default := .Values.defaults.secretNamespace | default "argocd" }}
{{- $namespaces := list }}
{{- range .Values.clusters }}
{{- $namespaces = append $namespaces (.secretNamespace | default $default) }}
{{- end }}
{{- $namespaces | uniq | sortAlpha | toJson }}
{{- end }}

{{/*
Validates cluster entries early, so a typo fails at render time rather than in
the daemon's crash loop. The daemon revalidates everything itself.
*/}}
{{- define "r2a-cert-sync.validate" -}}
{{- $needsRancher := false }}
{{- range $i, $c := .Values.clusters }}
{{- if not $c.name }}
{{- fail (printf "clusters[%d]: name is required" $i) }}
{{- end }}
{{- if not $c.endpoint }}
{{- fail (printf "clusters[%d] (%s): endpoint is required" $i $c.name) }}
{{- end }}
{{- $provider := $c.provider | default "rancher" }}
{{- if not (has $provider (list "rancher" "direct")) }}
{{- fail (printf "clusters[%d] (%s): provider must be \"rancher\" or \"direct\", got %q" $i $c.name $provider) }}
{{- end }}
{{- if eq $provider "rancher" }}
{{- $needsRancher = true }}
{{- end }}
{{- if and $c.autoRotate (ne $provider "rancher") }}
{{- fail (printf "clusters[%d] (%s): autoRotate requires provider \"rancher\"; standalone RKE2 exposes no rotation API" $i $c.name) }}
{{- end }}
{{- end }}
{{- if and $needsRancher (not .Values.rancher.url) }}
{{- fail "at least one cluster uses provider \"rancher\" but rancher.url is not set" }}
{{- end }}
{{- if and .Values.rancher.url (not .Values.rancher.tokenSecret.name) }}
{{- fail "rancher.url is set but rancher.tokenSecret.name is empty" }}
{{- end }}
{{- end }}
