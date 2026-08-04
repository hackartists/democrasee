# 05 — PR preview CI on the home k3s cluster

The PR browser-e2e path (`playwright-tests` on ubuntu-latest + docker compose)
has been replaced by a per-PR preview deployed onto the home k3s cluster,
modeled on asset's proven `pr-workflow.yml` e2e pattern. One deliberate
difference: **ratel tears the preview down as soon as the tests finish**;
asset keeps its preview alive for the PR's lifetime.

## Architecture

```
GitHub PR
   │
   ▼
self-hosted runner pod (biyard org, home k3s, arm64, no docker CLI)
   │  build-image job: npm i → make build-testing (native arm64 dx build)
   ▼
buildkitd (tcp://buildkitd.infra.svc.cluster.local:1234)
   │  scripts/buildkit-image.sh — packages target/dx/app-shell/release/web
   │  with app/ratel/Dockerfile (no compile inside the Dockerfile)
   ▼
in-cluster registry (registry.infra.svc.cluster.local:5000)
   │  ratel-app-shell:pr-<num>   (k3s nodes resolve the name via registries.yaml)
   ▼
namespace ratel-pr-<num>          (e2e job: envsubst deploy/pr-preview/*.yaml)
   ├─ localstack   Deployment+Service :4566  (dynamodb, s3, sqs, streams)
   ├─ localstack-init  Job  — scripts/localstack-init-entrypoint.sh via
   │                    ConfigMaps (table + GSIs + streams + seed fixtures)
   ├─ qdrant       Deployment+Service :6333/:6334
   ├─ qdrant-init  Job  — creates the ratel-local-main collection
   ├─ app-shell    Deployment+Service :8080  (initContainer waits for the
   │                    ratel-local-main table before the server starts)
   └─ Ingress      ratel-<num>.pr.biyard.co → app-shell:8080
   ▼
ingress-nginx (ns infra) — the exact-host Ingress takes the name over from
the wildcard *.pr.biyard.co pr-nginx; the host rides the default TLS cert
   ▼
Playwright pod (mcr.microsoft.com/playwright:v1.58.2-noble, in the PR ns)
   kubectl cp playwright/ in → npm ci → npx playwright test tests/web
   chromium pinned to the ingress ClusterIP via E2E_HOST_RESOLVER_RULES
   ("MAP <host> <ingress-ip>"), ignoreHTTPSErrors env-gated in
   playwright.config.js → report/results kubectl cp'd back out
```

Why the compose hostnames survive: the app image is built with the SAME
compile-time env block the old `ratel-app` job used
(`DYNAMO_ENDPOINT=http://localstack:4566`, `DYNAMO_TABLE_PREFIX=ratel-local`,
`ENV=local`, bypass features). The k8s Services are named `localstack` and
`qdrant` — identical to the docker-compose hostnames — so every endpoint
baked into the binary keeps resolving in-cluster without a rebuild.

## Lifecycle

1. **PR push** → `build-image` (self-hosted): native arm64 build, push
   `ratel-app-shell:pr-<num>`. The x86 `ratel-app` docker-artifact job
   survives ONLY as the tauri-android-smoke-test backend.
2. **e2e** (needs build-image): recreate `ratel-pr-<num>` clean → create
   script/seed ConfigMaps → envsubst-apply `deploy/pr-preview/*.yaml` → wait
   (init Jobs complete → rollouts → curl the ingress with `--resolve`) → run
   the suite in the Playwright pod → dump pod logs into the job log →
   publish the HTML report to `/root/nginx/ratel-report-<num>/` (served at
   `https://ratel-report-<num>.pr.biyard.co`) → **delete the namespace** →
   sticky PR comment (`<!-- e2e-preview -->`) with the report URL.
3. **PR close** → `pr-preview-cleanup.yml` (safety net): deletes the
   namespace if a cancelled/crashed run left it behind, removes the report
   webroot, updates the sticky comment. No database step — the preview is
   fully namespace-contained.

The e2e job is `continue-on-error: true` (non-blocking) while the new path
stabilizes; promote it to a required check once it has green history.

## Debugging a failed run

- **Playwright report** — `https://ratel-report-<num>.pr.biyard.co` (sticky
  PR comment links it). Contains per-test screenshots, videos, and traces;
  published on failure too, with a placeholder page if no report was made.
- **Server-side story** — the "Dump pod logs" step prints every
  deployment's and job's logs into the job log right before teardown; since
  the namespace is deleted afterwards, this is the only place they survive.
- **Which phase died** — the wait step fails with the culprit's name:
  `job/localstack-init` (table/seed script), `job/qdrant-init`,
  a `rollout status` (image pull / crash / initContainer stuck waiting for
  the table), or the ingress curl (routing).
- **Reproduce locally** — the stack is the same shape as
  `docker compose --profile testing`; or, with cluster VPN access, run the
  e2e steps by hand with `PR_NUM=<something-unused>` and keep the namespace
  around (skip the teardown step) to poke at it with kubectl.
- **Manifest sanity** — `PR_NUM=999 envsubst '${PR_NUM}' < deploy/pr-preview/<f>.yaml | kubectl apply --dry-run=client -f -`.

## Switching to asset-style persistent previews later

Everything is already shaped for it:

1. Delete the "Tear down the preview namespace" step from the `e2e` job in
   `.github/workflows/pr-workflow.yml`.
2. In `.github/workflows/pr-preview-cleanup.yml`, the namespace delete then
   becomes the primary teardown (it is already written to handle a live
   namespace).
3. Optionally reword the sticky comment to advertise the live preview URL
   (`https://ratel-<num>.pr.biyard.co`) — with the namespace kept, the
   exact-host Ingress keeps serving it for the PR's lifetime.

Cost of persistence: one localstack + qdrant + app-shell set per open PR
(~1 GB RAM each), and LocalStack state lives in the pod (PERSISTENCE=0) —
a pod restart loses seeded data, which is fine for a throwaway test run but
would surprise reviewers using a long-lived preview; a persistent variant
should re-enable persistence onto a PVC or rerun the init Job on restart.
