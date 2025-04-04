{{/*
Expand the name of the chart.
*/}}
{{- define "hybrid-executors.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hybrid-executors.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "hybrid-executors.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hybrid-executors.labels" -}}
helm.sh/chart: {{ include "hybrid-executors.chart" . }}
{{ include "hybrid-executors.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hybrid-executors.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hybrid-executors.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Compute the Tobiko Cloud URL based on organization and project
*/}}
{{- define "hybrid-executors.cloudUrl" -}}
{{- $baseUrl := default "https://cloud.tobikodata.com" .Values.global.cloud.baseUrl -}}
{{- printf "%s/sqlmesh/%s/%s" $baseUrl .Values.global.cloud.org .Values.global.cloud.project -}}
{{- end }}

{{/*
Determine the name of the secret to use
*/}}
{{- define "hybrid-executors.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
    {{- .Values.secrets.existingSecret -}}
{{- else if .Values.secrets.sealedSecrets.enabled -}}
    {{- printf "%s-sealed-secrets" (include "hybrid-executors.fullname" .) -}}
{{- else if .Values.secrets.externalSecrets.enabled -}}
    {{- printf "%s-external-secrets" (include "hybrid-executors.fullname" .) -}}
{{- else -}}
    {{- printf "%s-secrets" (include "hybrid-executors.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Determine if a connection parameter should be treated as a secret
Returns "true" if the parameter name contains "password", "secret", or "token" (case-insensitive)
*/}}
{{- define "hybrid-executors.isSecretParam" -}}
{{- $paramName := . -}}
{{- $param := lower $paramName -}}
{{- if or (contains "password" $param) (contains "secret" $param) (contains "token" $param) -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{/*
Determine the default gateway to use
Logic:
1. If .Values.global.sqlmesh.defaultGateway is set, use that
2. If only one gateway is defined, use that one
3. If multiple gateways are defined, use the first one
*/}}
{{- define "hybrid-executors.defaultGateway" -}}
{{- if .Values.global.sqlmesh.defaultGateway -}}
{{- .Values.global.sqlmesh.defaultGateway -}}
{{- else -}}
{{- $gatewayCount := len .Values.global.sqlmesh.gateways -}}
{{- if eq $gatewayCount 1 -}}
{{- range $gatewayName, $gateway := .Values.global.sqlmesh.gateways -}}
{{- $gatewayName -}}
{{- end -}}
{{- else -}}
{{- $firstGateway := "" -}}
{{- range $gatewayName, $gateway := .Values.global.sqlmesh.gateways -}}
{{- if eq $firstGateway "" -}}
{{- $firstGateway = $gatewayName -}}
{{- end -}}
{{- end -}}
{{- $firstGateway -}}
{{- end -}}
{{- end -}}
{{- end -}} 