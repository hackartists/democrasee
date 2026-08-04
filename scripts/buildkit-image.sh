#!/bin/bash
set -euo pipefail

# Build a Dockerfile with the in-cluster buildkitd and push the image to the
# in-cluster registry — the runner-pod replacement for the docker-CLI
# `make docker` path (that target remains for the local/ECR route). Adapted
# from asset's scripts/buildkit-image.sh; ratel difference: the app image's
# build CONTEXT is not the repo root but the dx build output directory
# (target/dx/app-shell/release/web), so the context is parameterized.
#
# Usage (PR preview image):
#   NAME=ratel-app-shell \
#   CONTEXT=target/dx/app-shell/release/web \
#   DOCKERFILE=app/ratel/Dockerfile \
#   IMAGE_TAG=pr-<num> \
#     bash scripts/buildkit-image.sh
#
# Env:
#   NAME           image repository name (required), e.g. ratel-app-shell
#   DOCKERFILE     Dockerfile path relative to the repo root (required)
#   IMAGE_TAG      image tag (required — PR previews use pr-<num>)
#   CONTEXT        build context directory (default `.` — repo root). The
#                  ratel app image packages the prebuilt dx output, so the
#                  build-image job passes target/dx/app-shell/release/web.
#   BUILDKIT_HOST  buildkitd address — preset in the runner pods
#                  (tcp://buildkitd.infra.svc.cluster.local:1234)
#   PUSH_REGISTRY  push target (default registry.infra.svc.cluster.local:5000
#                  — the in-cluster registry Service, http). Pods pull the
#                  SAME name: the k3s nodes' registries.yaml maps it to the
#                  Service IP endpoint, since node containerd cannot resolve
#                  cluster DNS itself.
#   PLATFORM       default linux/arm64 (both cluster nodes are arm64; the
#                  runner's native cargo/dx build already produced arm64
#                  binaries, so no cross-compile happens here — this is just
#                  the image manifest platform).

NAME="${NAME:?NAME is required (image repository name)}"
DOCKERFILE="${DOCKERFILE:?DOCKERFILE is required}"
IMAGE_TAG="${IMAGE_TAG:?IMAGE_TAG is required}"
CONTEXT="${CONTEXT:-.}"
PUSH_REGISTRY="${PUSH_REGISTRY:-registry.infra.svc.cluster.local:5000}"
PLATFORM="${PLATFORM:-linux/arm64}"
: "${BUILDKIT_HOST:?BUILDKIT_HOST is not set — run inside a runner pod or export it}"

test -d "${CONTEXT}" || {
  echo "::error::build context '${CONTEXT}' does not exist — did the dx build run?"
  exit 1
}
test -f "${DOCKERFILE}" || {
  echo "::error::Dockerfile '${DOCKERFILE}' does not exist"
  exit 1
}

image="${PUSH_REGISTRY}/${NAME}:${IMAGE_TAG}"
echo "building ${image} (${PLATFORM}, context=${CONTEXT}, buildkitd=${BUILDKIT_HOST})"

# Note: unlike asset's script there is no ENV / APP_FEATURES build-arg —
# ratel's app Dockerfile (app/ratel/Dockerfile) declares no ARGs; every
# compile-time knob was already baked by the native `make build-testing`
# that produced the context directory.
buildctl build \
  --frontend dockerfile.v0 \
  --local context="${CONTEXT}" \
  --local dockerfile="$(dirname "${DOCKERFILE}")" \
  --opt "filename=$(basename "${DOCKERFILE}")" \
  --opt "platform=${PLATFORM}" \
  --output "type=image,name=${image},push=true,registry.insecure=true"

echo "pushed ${image}"
