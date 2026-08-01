{{- define "k2a-token-sync.name" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "k2a-token-sync.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/name: {{ include "k2a-token-sync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "k2a-token-sync.selectorLabels" -}}
app.kubernetes.io/name: {{ include "k2a-token-sync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
image.tag is required and deliberately not defaulted from Chart.appVersion.

Deriving it from chart metadata would tie the deployed version to a file inside
the tagged tree, and the tag is what triggers the build — so the chart could
never name the image that release produced. Keeping the version in deployment
values instead means it is set after the release, and the deployed version is
stated explicitly rather than implied.
*/}}
{{- define "k2a-token-sync.image" -}}
{{- if not .Values.image.tag }}
{{- fail "image.tag is required: set it to a released version, e.g. --set image.tag=v0.0.1 (see https://github.com/krisiasty/k2a-token-sync/releases)" }}
{{- end }}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag }}
{{- end }}

{{/*
Namespace of the ArgoCD instance this release serves. All generated cluster
Secrets go here, and the daemon needs one Role there.
*/}}
{{- define "k2a-token-sync.argocdNamespace" -}}
{{- .Values.argocdNamespace | default "argocd" }}
{{- end }}

{{/*
Validates cluster entries early, so a typo fails at render time rather than in
the daemon's crash loop. The daemon revalidates everything itself.
*/}}
{{- define "k2a-token-sync.validate" -}}
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
{{- end }}
{{- end }}
