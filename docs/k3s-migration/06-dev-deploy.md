# 06 — Dev deployment on the home k3s cluster

`push` to `dev` now deploys the dev environment to the home k3s cluster
(`.github/workflows/dev-workflow.yml`, modeled on asset's prod-workflow),
replacing the AWS path (Lambda build → cdk deploy → S3 sync + CloudFront).
The AWS dev stack is untouched and keeps serving its last-deployed version at
`dev.ratel.foundation` until that DNS is flipped.

## Architecture

```
push to dev
   │
   ▼
build-images (self-hosted arm64 runner pod, real dev secret env baked
   │           via option_env!: DYNAMO_TABLE_PREFIX=ratel-dev, real AWS
   │           creds, DYNAMO_ENDPOINT=none → real AWS DynamoDB)
   ├─ make build            → app-shell (client web,fullstack + server
   │                          `server`; NO bypass/local-dev/lambda)
   │                          → buildkitd → registry: ratel-app-shell:dev-<sha>, :dev
   └─ cargo build --release --features redpanda
        --bin ratel_cdc --bin ratel_worker
        → ldd guard → deploy/dev/Dockerfile.events (ubuntu:24.04 — same
          glibc-proven base as the app image; librdkafka statically linked
          via cmake-build, no ssl/sasl rdkafka features ⇒ only glibc/libgcc/
          zlib dynamic deps; ca-certificates for rustls→AWS)
        → registry: ratel-events:dev-<sha>, :dev
   ▼
deploy (envsubst deploy/dev/*.yaml → STABLE namespace ratel-dev; serialized
   │    by a job-level concurrency group, queued not cancelled)
   ├─ Secrets: ratel-dev-secrets (LAUNCHPAD_*, QDRANT_API_KEY — the set cdk
   │           injected into the Lambda RUNTIME env) + ratel-dev-firebase
   │           (FCM service-account JSON; absent ⇒ push gracefully disabled)
   ├─ app-shell  Deployment ×2 + Service :8080
   ├─ Ingress    hosts: ratel-dev.biyard.co (LIVE — wildcard *.biyard.co DNS
   │             already points at the cluster; cert-manager
   │             letsencrypt-route53 TLS, mirroring the dataroom prod
   │             ingress) + dev.ratel.foundation (INERT until DNS flips —
   │             cutover is then a pure DNS change; not in tls: because the
   │             route53 issuer can't solve DNS-01 for ratel.foundation yet)
   └─ events tier (image ratel-events):
        ratel-cdc              ×1 (strategy Recreate — single checkpointed
        │                         producer; no dry-run: producing CdcEvents
        │                         is side-effect-free for the app)
        ratel-worker-default   ×1 ┐  RATEL_KAFKA_BROKERS=redpanda-0.redpanda
        ratel-worker-analyze   ×1 ├─ .infra.svc.cluster.local:9093
        │  (1Gi memory request)   │  RATEL_CDC_TOPIC=dev.ratel.cdc.v1
        ratel-worker-egress    ×1 ┘  RATEL_WORKER_DRY_RUN=${WORKER_DRY_RUN}
   ▼
smoke-test — public https://ratel-dev.biyard.co/ with the real cert
```

## WORKER_DRY_RUN — the dev events cutover switch

The workers deploy in **shadow mode by default**: the workflow-level env
`WORKER_DRY_RUN: "1"` in dev-workflow.yml becomes `RATEL_WORKER_DRY_RUN=1`
on every `ratel-worker-*` pod (`common/events/consumer.rs`) — they consume
`dev.ratel.cdc.v1`, evaluate the filter rules, log
`dry-run: would dispatch` with the matched rules, commit offsets, and run
**no handlers**. The AWS EventBridge rules remain the live event path.

### Cutover procedure (plan § W5, step 2)

Do these together — running both paths live double-processes every event
(double XP, double notifications):

1. Verify shadow health: worker logs show `dry-run: would dispatch` lines
   whose matched rules agree with what EventBridge actually processed.
2. **Disable the AWS EventBridge rules** for dev (the 34 rules routed to the
   app-shell Lambda; see `cdk/lib/dynamo-stream-event.ts` /
   `01-events-redpanda-plan.md` § W5).
3. **Flip `WORKER_DRY_RUN` to `"0"`** in `.github/workflows/dev-workflow.yml`
   and push to dev (or, for an immediate flip without a deploy:
   `kubectl -n ratel-dev set env deploy/ratel-worker-default deploy/ratel-worker-analyze deploy/ratel-worker-egress RATEL_WORKER_DRY_RUN=0`
   — but commit the workflow change too, or the next deploy reverts it).
4. Watch worker logs + DLQ topic (`dev.ratel.cdc.v1.dlq`) for the first
   hours.

### Rollback (events)

Re-enable the EventBridge rules and set `WORKER_DRY_RUN` back to `"1"`
(same two-together rule, in reverse order: shadow the workers FIRST, then
re-enable the rules).

## Rollback (app)

- **Bad deploy**: `git revert` on dev → the workflow redeploys the previous
  sha (images are sha-tagged `dev-<sha>` and stay in the in-cluster
  registry). For an emergency pin without a build:
  `kubectl -n ratel-dev set image deploy/app-shell app-shell=registry.infra.svc.cluster.local:5000/ratel-app-shell:dev-<old-sha>`.
- **Whole-path fallback**: the AWS dev stack still serves its last-deployed
  version at `dev.ratel.foundation`; nothing in this pipeline touches it.
  The k3s `ratel-dev` namespace can stay running — it is invisible to users
  until the `dev.ratel.foundation` DNS points at the cluster.

## Job graph

| Job | Status | Notes |
|---|---|---|
| `build-images` | NEW | native arm64 `make build` + events cargo build → 2 images; saves the `ratel-cargo-*`/`ratel-target-*` caches the PR workflow restores read-only |
| `deploy` | REPLACED | was cdk → now envsubst/kubectl into `ratel-dev`; same job name so `create-release-pr`'s `needs` is untouched |
| `smoke-test` | NEW | public URL probe with the real certificate |
| `ratel-app-lambda` | REMOVED | AWS machinery (lambda build, S3 sync, CloudFront); `cdk/` + `make build-lambda` remain in-repo |
| `ratel-app` | KEPT | not deploy machinery — warms the `app-shell-build-*` cache PR jobs restore + x86 image artifact |
| `create-release-pr` | KEPT | still `needs: [deploy]` |
| `tauri-android-dev` | KEPT | dev APK artifact, points at `dev.ratel.foundation` (follows whichever backend that DNS serves) |

## Debugging

- Deploy failures dump pod state + events + per-deployment logs into the job
  log (`Dump pod state on failure`).
- Events tier: `kubectl -n ratel-dev logs deploy/ratel-worker-default`
  (`ratel-cdc`, `-analyze`, `-egress` likewise). The deploy job's events
  check warns (non-fatally) when no `worker consuming` /
  `dry-run: would dispatch` line is visible yet — a quiet dev stream is
  normal.
- Manifest sanity:
  `SHA_TAG=dev-test WORKER_DRY_RUN=1 envsubst '${SHA_TAG} ${WORKER_DRY_RUN}' < deploy/dev/<f>.yaml | kubectl apply --dry-run=client -f -`.
