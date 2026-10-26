{{/* Chart name, truncated for the 63 character limit of Kubernetes names. */}}
{{- define "ksg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ksg.fullname" -}}
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

{{- define "ksg.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "ksg.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: kafka-schema-guardian
{{- end -}}

{{- define "ksg.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ksg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ksg.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "ksg.advertisedHost" -}}
{{- default (printf "%s-bootstrap.%s.svc.cluster.local" (include "ksg.fullname" .) .Release.Namespace) .Values.proxy.advertisedHost -}}
{{- end -}}

{{- define "ksg.kafkaBootstrap" -}}
{{- $host := default (printf "%s-kafka-bootstrap.%s.svc.cluster.local" .Values.kafka.clusterName .Values.kafka.namespace) .Values.kafka.bootstrapHost -}}
{{- printf "%s:%d" $host (int .Values.kafka.bootstrapPort) -}}
{{- end -}}

{{/*
One entry per broker: id and the port the proxy listens on for it. The proxy numbers the node IDs of all ranges in
ascending order and gives them consecutive ports starting at nodeStartPort.
*/}}
{{- define "ksg.nodePorts" -}}
{{- $ports := list -}}
{{- $offset := 0 -}}
{{- range .Values.proxy.nodeIdRanges -}}
{{- range $id := untilStep (int .start) (int (add1 (int .end))) 1 -}}
{{- $ports = append $ports (dict "id" $id "port" (add (int $.Values.proxy.nodeStartPort) $offset)) -}}
{{- $offset = add1 $offset -}}
{{- end -}}
{{- end -}}
{{- toJson $ports -}}
{{- end -}}

{{- define "ksg.validate" -}}
{{- if not (has .Values.mode (list "mtls" "tls" "plaintext")) -}}
{{- fail "mode must be one of: mtls, tls, plaintext" -}}
{{- end -}}
{{- if lt (int .Values.replicaCount) 1 -}}
{{- fail "replicaCount must be at least 1" -}}
{{- end -}}
{{- if not .Values.rules.topics -}}
{{- fail "rules.topics must define at least one topic" -}}
{{- end -}}
{{- if not (has .Values.rules.defaultPolicy (list "ALLOW" "DENY")) -}}
{{- fail "rules.defaultPolicy must be ALLOW or DENY" -}}
{{- end -}}
{{- range $topic, $cfg := .Values.rules.topics -}}
{{- if not $cfg.allowedSchemaIds -}}
{{- fail (printf "topic %s needs at least one allowedSchemaIds entry" $topic) -}}
{{- end -}}
{{- end -}}
{{- end -}}
