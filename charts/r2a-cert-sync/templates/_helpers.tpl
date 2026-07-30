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
Namespace of the ArgoCD instance this release serves. All generated cluster
Secrets go here, and the daemon needs one Role there.
*/}}
{{- define "r2a-cert-sync.argocdNamespace" -}}
{{- .Values.argocdNamespace | default "argocd" }}
{{- end }}

{{/*
Validates cluster entries early, so a typo fails at render time rather than in
the daemon's crash loop. The daemon revalidates everything itself.
*/}}
{{- define "r2a-cert-sync.validate" -}}
{{- $needsRancher := false }}
{{- $secretNames := list }}
{{- range $i, $c := .Values.clusters }}
{{- if not $c.name }}
{{- fail (printf "clusters[%d]: name is required" $i) }}
{{- end }}
{{- $secretName := $c.secretName | default (printf "cluster-%s" $c.name) }}
{{- if has $secretName $secretNames }}
{{- fail (printf "clusters[%d] (%s): secretName %q is already used by another cluster" $i $c.name $secretName) }}
{{- end }}
{{- $secretNames = append $secretNames $secretName }}
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
