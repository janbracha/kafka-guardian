# syntax=docker/dockerfile:1.7
#
# kafka-schema-guardian: the official Kroxylicious proxy image plus the guardian plugin JAR.
#
#   docker build -t kafka-schema-guardian:local .
#
# Base images are pinned by tag AND digest (the digest is what is actually pulled; the tag documents it).
# Tag-only variant:  --build-arg PROXY_IMAGE=quay.io/kroxylicious/proxy:0.25.1 --build-arg BUILDER_IMAGE=maven:3.9.9-eclipse-temurin-21
# The proxy image version MUST equal <kroxylicious.version> in pom.xml: the plugin is compiled against that Filter API.

ARG BUILDER_IMAGE=maven:3.9.9-eclipse-temurin-21@sha256:3a4ab3276a087bf276f79cae96b1af04f53731bec53fb2e651aca79e4b10211e
ARG PROXY_IMAGE=quay.io/kroxylicious/proxy:0.25.1@sha256:55f639c6a7c479503203c55dc5d5f5b956a2e61dd188b3152b736ca20f648f90

# ---- Stage 1: build and unit-test the plugin. The JAR is platform independent, so it is built once on the build host.
FROM --platform=$BUILDPLATFORM ${BUILDER_IMAGE} AS build
WORKDIR /build
COPY pom.xml ./
COPY .mvn .mvn
COPY src src
# The configuration tests parse the shipped YAML files.
COPY deploy deploy
COPY docker docker
COPY examples examples
# Unit tests and the static checks run here; the integration tests run in CI (they need a broker and take minutes).
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -DskipITs verify \
    && cp target/kafka-schema-guardian-*.jar /build/kafka-schema-guardian.jar

# ---- Stage 2: runtime. Nothing but the plugin and the default configuration is added to the official image.
FROM ${PROXY_IMAGE} AS runtime
ARG PROXY_IMAGE

# The proxy image already runs as the non-root user 185; stated again so it can never silently change.
USER 185

# Plugins on the proxy classpath: everything in /opt/kroxylicious/libs.
COPY --from=build --chown=0:0 --chmod=0444 /build/kafka-schema-guardian.jar /opt/kroxylicious/libs/kafka-schema-guardian.jar

# Secure defaults: mTLS towards producers and Strimzi, deny-all rules. They need the Secrets described in the README, so
# without them the proxy refuses to start. Kubernetes mounts the real files over these paths.
COPY --chown=0:0 --chmod=0444 deploy/base/proxy-config.yaml /etc/kafka-schema-guardian/config/proxy-config.yaml
COPY --chown=0:0 --chmod=0444 docker/rules.yaml /etc/kafka-schema-guardian/rules/rules.yaml

EXPOSE 9092 9093 9094 9095 9190

ARG VERSION=1.0.0
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="kafka-schema-guardian" \
      org.opencontainers.image.description="Kroxylicious proxy with a filter that rejects Kafka records whose Confluent schema ID is not whitelisted" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.base.name="${PROXY_IMAGE}"

# ENTRYPOINT (tini + kroxylicious-start.sh) comes from the base image.
CMD ["--config", "/etc/kafka-schema-guardian/config/proxy-config.yaml"]
