SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

# ---- image coordinates: <registry>/<repository>:<tag> -------------------------------------------------------------
IMAGE_REGISTRY   ?= registry.example.com
IMAGE_REPOSITORY ?= platform/kafka-schema-guardian
IMAGE_TAG        ?= 1.1.1
IMAGE            ?= $(IMAGE_REGISTRY)/$(IMAGE_REPOSITORY):$(IMAGE_TAG)

# ---- container tooling: docker | podman | buildah -----------------------------------------------------------------
CONTAINER_ENGINE ?= docker
PLATFORMS        ?= linux/amd64,linux/arm64
EXTRA_BUILD_ARGS ?=
VCS_REF          ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
BUILD_ARGS       := --build-arg VERSION=$(IMAGE_TAG) --build-arg VCS_REF=$(VCS_REF) $(EXTRA_BUILD_ARGS)

# ---- Kubernetes -----------------------------------------------------------------------------------------------------
KUBECTL       ?= kubectl
NAMESPACE     ?= kafka-proxy
# deploy | deploy/overlays/tls | deploy/overlays/plaintext
KUSTOMIZE_DIR ?= deploy
PLACEHOLDER_IMAGE := registry.example.com/platform/kafka-schema-guardian:1.1.1

MVN       ?= ./mvnw
MVN_FLAGS ?= -B -ntp

# ---- Helm chart, published as an OCI artifact (Docker Hub: oci://registry-1.docker.io/<namespace>) ---------------------
CHART_DIR         ?= deploy/helm/kafka-schema-guardian-chart
CHART_VERSION     ?= $(IMAGE_TAG)
HELM_REGISTRY_HOST ?= $(if $(filter docker.io,$(IMAGE_REGISTRY)),registry-1.docker.io,$(IMAGE_REGISTRY))
HELM_OCI_REPO     ?= oci://$(HELM_REGISTRY_HOST)/$(patsubst %/,%,$(dir $(IMAGE_REPOSITORY)))

.PHONY: help clean test verify package format image image-multiarch image-push deploy deploy-optional undeploy \
        helm-lint helm-template helm-package helm-push

help: ## list the targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-16s %s\n", $$1, $$2}'

# ---- Maven ----------------------------------------------------------------------------------------------------------
clean: ## remove build output
	$(MVN) $(MVN_FLAGS) clean

test: ## unit tests (no Docker, no broker)
	$(MVN) $(MVN_FLAGS) test

verify: ## unit + integration tests and static checks (Spotless, Enforcer)
	$(MVN) $(MVN_FLAGS) verify

package: ## build the plugin JAR (unit tests included, integration tests skipped)
	$(MVN) $(MVN_FLAGS) -DskipITs package

format: ## apply the Spotless formatting
	$(MVN) $(MVN_FLAGS) spotless:apply

# ---- image ----------------------------------------------------------------------------------------------------------
# make image IMAGE_TAG=1.1.1 [CONTAINER_ENGINE=podman|buildah]   one image for the local platform
# make image-push ...                                            amd64+arm64 manifest list, pushed to $(IMAGE)
image: ## build the image for the local platform
ifeq ($(CONTAINER_ENGINE),docker)
	docker build $(BUILD_ARGS) -t $(IMAGE) .
else ifeq ($(CONTAINER_ENGINE),podman)
	podman build $(BUILD_ARGS) -t $(IMAGE) .
else ifeq ($(CONTAINER_ENGINE),buildah)
	buildah bud $(BUILD_ARGS) -t $(IMAGE) .
else
	@echo "unsupported CONTAINER_ENGINE=$(CONTAINER_ENGINE) (docker|podman|buildah)"; exit 2
endif

image-multiarch: ## build amd64+arm64 (docker: into the buildx cache; podman/buildah: a local manifest list)
ifeq ($(CONTAINER_ENGINE),docker)
	docker buildx build $(BUILD_ARGS) --platform $(PLATFORMS) -t $(IMAGE) .
else ifeq ($(CONTAINER_ENGINE),podman)
	-podman manifest rm $(IMAGE)
	podman build $(BUILD_ARGS) --platform $(PLATFORMS) --manifest $(IMAGE) .
else ifeq ($(CONTAINER_ENGINE),buildah)
	-buildah manifest rm $(IMAGE)
	buildah bud $(BUILD_ARGS) --platform $(PLATFORMS) --manifest $(IMAGE) .
else
	@echo "unsupported CONTAINER_ENGINE=$(CONTAINER_ENGINE) (docker|podman|buildah)"; exit 2
endif

image-push: ## build amd64+arm64 and push to $(IMAGE)
ifeq ($(CONTAINER_ENGINE),docker)
	docker buildx build $(BUILD_ARGS) --platform $(PLATFORMS) -t $(IMAGE) --push .
else ifeq ($(CONTAINER_ENGINE),podman)
	$(MAKE) --no-print-directory image-multiarch
	podman manifest push --all $(IMAGE) docker://$(IMAGE)
else ifeq ($(CONTAINER_ENGINE),buildah)
	$(MAKE) --no-print-directory image-multiarch
	buildah manifest push --all $(IMAGE) docker://$(IMAGE)
else
	@echo "unsupported CONTAINER_ENGINE=$(CONTAINER_ENGINE) (docker|podman|buildah)"; exit 2
endif

# ---- Kubernetes -----------------------------------------------------------------------------------------------------
deploy: ## render $(KUSTOMIZE_DIR), set the image to $(IMAGE), apply, wait for the rollout
	$(KUBECTL) kustomize $(KUSTOMIZE_DIR) | sed 's#$(PLACEHOLDER_IMAGE)#$(IMAGE)#g' | $(KUBECTL) apply -f -
	$(KUBECTL) -n $(NAMESPACE) rollout status deployment/kafka-schema-guardian --timeout=300s

deploy-optional: ## ServiceMonitor (needs Prometheus Operator CRDs) and HorizontalPodAutoscaler
	$(KUBECTL) apply -k deploy/optional

undeploy: ## delete everything deploy created (Secrets are left alone)
	-$(KUBECTL) delete -k deploy/optional --ignore-not-found
	$(KUBECTL) delete -k $(KUSTOMIZE_DIR) --ignore-not-found

helm-lint: ## lint the Helm chart
	helm lint $(CHART_DIR)

helm-template: ## render the Helm chart
	helm template kafka-schema-guardian $(CHART_DIR) --namespace $(NAMESPACE) \
	  --set image.repository=$(IMAGE_REGISTRY)/$(IMAGE_REPOSITORY) --set image.tag=$(IMAGE_TAG)

# The chart version is the image tag, so a chart and the image it deploys always carry the same version.
# The default image repository of the chart is rewritten to the published one in a temporary copy.
helm-package: ## package the chart into target/helm (points at $(IMAGE_REGISTRY)/$(IMAGE_REPOSITORY))
	rm -rf target/helm && mkdir -p target/helm
	cp -r $(CHART_DIR) target/helm/chart
	sed -i.bak 's#^\(  repository: \).*#\1$(IMAGE_REGISTRY)/$(IMAGE_REPOSITORY)#' target/helm/chart/values.yaml
	rm -f target/helm/chart/values.yaml.bak
	helm package target/helm/chart --version $(CHART_VERSION) --app-version $(IMAGE_TAG) --destination target/helm

helm-push: helm-package ## push the chart to $(HELM_OCI_REPO) (run helm registry login first)
	helm push target/helm/kafka-schema-guardian-chart-$(CHART_VERSION).tgz $(HELM_OCI_REPO)