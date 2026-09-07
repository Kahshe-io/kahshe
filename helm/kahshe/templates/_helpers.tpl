{{- define "kahshe.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kahshe.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "kahshe.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "kahshe.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "kahshe.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kahshe.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "kahshe.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* The backend settings every role shares. */}}
{{- define "kahshe.backendEnv" -}}
- name: KAHSHE_BACKEND
  value: {{ .Values.backend.url | quote }}
{{- if .Values.backend.credentialSecret }}
- name: KAHSHE_CREDENTIAL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.backend.credentialSecret }}
      key: {{ .Values.backend.credentialKey }}
{{- end }}
- name: KAHSHE_SCOPE
  value: {{ .Values.backend.scope | quote }}
{{- end -}}

{{/* The build's memory and scratch settings; takes a .build map. int64 before quote: Helm
     holds YAML numbers as float64, and quote alone renders 33554432 as "3.3554432e+07", which
     the process refuses at startup. */}}
{{- define "kahshe.buildEnv" -}}
- name: KAHSHE_INDEX_THREADS
  value: {{ .indexThreads | int64 | quote }}
- name: KAHSHE_TERM_BUFFER_BYTES
  value: {{ .termBufferBytes | int64 | quote }}
- name: KAHSHE_GRAM_BUILD_MAX_BYTES
  value: {{ .gramBuildMaxBytes | int64 | quote }}
- name: KAHSHE_TERM_BUILD_MAX_SPILL_BYTES
  value: {{ .termBuildMaxSpillBytes | int64 | quote }}
{{- end -}}

{{/* The process-shaped settings every role shares: how it logs, and where and behind what its
     admin port listens. One place, so a token set for the proxy guards the indexer and the
     watcher too — they expose the same port. */}}
{{- define "kahshe.processEnv" -}}
{{- if not (has .Values.logging.format (list "text" "json")) }}
{{- fail (printf "logging.format must be text or json, not %v: the process refuses anything else at startup." .Values.logging.format) }}
{{- end }}
- name: KAHSHE_LOG_FORMAT
  value: {{ .Values.logging.format | quote }}
- name: KAHSHE_ADMIN_BIND
{{- if eq .Values.admin.bind "podIP" }}
  valueFrom:
    fieldRef: { fieldPath: status.podIP }
{{- else }}
  value: {{ .Values.admin.bind | quote }}
{{- end }}
{{- if .Values.admin.tokenSecret }}
- name: KAHSHE_ADMIN_TOKEN
  valueFrom:
    secretKeyRef: { name: {{ .Values.admin.tokenSecret }}, key: {{ .Values.admin.tokenKey }} }
{{- end }}
{{- end -}}

{{/* Extra KAHSHE_* settings from a map. */}}
{{- define "kahshe.extraEnv" -}}
{{- range $k, $v := . }}
- name: {{ $k }}
  value: {{ $v | quote }}
{{- end }}
{{- end -}}

{{- define "kahshe.metricsAnnotations" -}}
{{- if .Values.metrics.annotations }}
prometheus.io/scrape: "true"
prometheus.io/port: {{ .Values.service.adminPort | quote }}
prometheus.io/path: /metrics
{{- end }}
{{- end -}}

{{/*
TLS environment. The PEM pair is what a kubernetes.io/tls Secret holds, so nothing is converted.
*/}}
{{- define "kahshe.tlsEnv" -}}
{{- if .Values.tls.enabled }}
- name: KAHSHE_TLS_CERT
  value: /etc/kahshe/tls/tls.crt
- name: KAHSHE_TLS_KEY
  value: /etc/kahshe/tls/tls.key
- name: KAHSHE_TLS_RELOAD_MS
  value: {{ .Values.tls.reloadMs | int64 | quote }}
- name: KAHSHE_TLS_ADMIN
  value: {{ .Values.tls.admin | quote }}
{{- if ne .Values.tls.clientAuth "none" }}
- name: KAHSHE_TLS_CLIENT_AUTH
  value: {{ .Values.tls.clientAuth | quote }}
- name: KAHSHE_TLS_CLIENT_CA
  value: /etc/kahshe/tls-client-ca/ca.crt
{{- end }}
{{- end }}
{{- end -}}

{{- define "kahshe.tlsMounts" -}}
{{- if .Values.tls.enabled }}
- { name: tls, mountPath: /etc/kahshe/tls, readOnly: true }
{{- if ne .Values.tls.clientAuth "none" }}
- { name: tls-client-ca, mountPath: /etc/kahshe/tls-client-ca, readOnly: true }
{{- end }}
{{- end }}
{{- end -}}

{{- define "kahshe.tlsVolumes" -}}
{{- if .Values.tls.enabled }}
- name: tls
  secret:
    secretName: {{ .Values.tls.secretName }}
{{- if ne .Values.tls.clientAuth "none" }}
- name: tls-client-ca
  secret:
    secretName: {{ required "tls.clientCaSecret is required when tls.clientAuth is not none" .Values.tls.clientCaSecret }}
{{- end }}
{{- end }}
{{- end -}}

{{/* Probes must follow the admin port's scheme, or a TLS admin port fails every probe. */}}
{{- define "kahshe.probeScheme" -}}
{{- if and .Values.tls.enabled .Values.tls.admin }}HTTPS{{ else }}HTTP{{ end }}
{{- end -}}

{{- define "kahshe.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kahshe.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end -}}
