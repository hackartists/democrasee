# 07 — Prod deployment on the home k3s cluster

`push` to `main` now deploys the prod environment to the home k3s cluster
(`.github/workflows/prod-workflow.yml`, modeled on the dev migration —
[06-dev-deploy.md](06-dev-deploy.md)), replacing the AWS path (Lambda build →
cdk deploy → S3 sync + CloudFront). The AWS prod stack is untouched and keeps
serving its last-deployed version at `ratel.foundation` until that DNS is
flipped.

## Architecture

```
push to main
   │
   ▼
build-images (self-hosted arm64 runner pod, real prod secret env baked
   │           via option_env!: DYNAMO_TABLE_PREFIX=ratel-prod, real AWS
   │           creds, DYNAMO_ENDPOINT=none → real AWS DynamoDB)
   ├─ make build            → app-shell (client web,fullstack + server
   │                          `server`; NO bypass/local-dev/lambda)
   │                          → buildkitd → registry: ratel-app-shell:prod-<sha>, :prod
   └─ cargo build --release --features redpanda
        --bin ratel_cdc --bin ratel_worker
        → ldd guard → deploy/dev/Dockerfile.events (shared with dev — the
          recipe only packages binaries; prod-ness is baked into them)
        → registry: ratel-events:prod-<sha>, :prod
   ▼
deploy (envsubst deploy/prod/*.yaml → STABLE namespace ratel-prod; serialized
   │    by a job-level concurrency group, queued not cancelled)
   ├─ Secrets: ratel-prod-secrets (LAUNCHPAD_*, QDRANT_API_KEY — the set cdk
   │           injected into the Lambda RUNTIME env) + ratel-prod-firebase
   │           (FCM service-account JSON; absent ⇒ push gracefully disabled)
   ├─ app-shell  Deployment ×2 + Service :8080
   ├─ Ingress    hosts: ratel-prod.biyard.co (LIVE — wildcard *.biyard.co DNS
   │             already points at the cluster; cert-manager
   │             letsencrypt-route53 TLS) + ratel.foundation (INERT until
   │             DNS flips — its cert CAN already issue via the route53
   │             dns01 solver for the ratel.foundation zone, proven by
   │             dev.ratel.foundation; cutover is then a pure DNS change)
   └─ events tier (image ratel-events), SHADOWED — see WORKER_DRY_RUN:
        ratel-cdc              ×1 (strategy Recreate — single checkpointed
        │                         producer; no dry-run: producing CdcEvents
        │                         is side-effect-free for the app)
        ratel-worker-default   ×1 ┐  RATEL_KAFKA_BROKERS=redpanda-0.redpanda
        ratel-worker-analyze   ×1 ├─ .infra.svc.cluster.local:9093
        │  (1Gi memory request)   │  RATEL_CDC_TOPIC=prod.ratel.cdc.v1
        ratel-worker-egress    ×1 ┘  RATEL_WORKER_DRY_RUN=${WORKER_DRY_RUN}
   ▼
smoke-test — public https://ratel-prod.biyard.co/ with the real cert
```

## WORKER_DRY_RUN — the prod events cutover switch

The workers deploy in **shadow mode**: the workflow-level env
`WORKER_DRY_RUN: "1"` in prod-workflow.yml becomes `RATEL_WORKER_DRY_RUN=1`
on every `ratel-worker-*` pod (`common/events/consumer.rs`) — they consume
`prod.ratel.cdc.v1`, evaluate the filter rules, log
`dry-run: would dispatch` with the matched rules, commit offsets, and run
**no handlers**. The AWS EventBridge path remains the live event path.

**Unlike dev** (whose EventBridge bus turned out to have no rules, making its
flip risk-free), the prod bus `ratel-prod-bus` has **~40 LIVE Pipes + Rules**
driving the app-shell Lambdas
([aws-infrastructure-inventory.md](aws-infrastructure-inventory.md)). Running
both paths live double-processes every event — double XP, double
notifications, double cross-posts (egress side effects reach real external
platforms). Prod stays shadowed until the deliberate cutover below.

### Cutover procedure (plan § W5)

Do these together:

1. Verify shadow health over a MEANINGFUL WINDOW (prod traffic, at least a
   day recommended): worker logs show `dry-run: would dispatch` lines whose
   matched rules agree with what EventBridge actually processed
   ([03-filter-matrix.md](03-filter-matrix.md) is the rule-by-rule parity
   reference).
2. **Disable the AWS EventBridge rules** for prod (the rules on
   `ratel-prod-bus` routed to the app-shell Lambdas; see
   `cdk/lib/dynamo-stream-event.ts` / `01-events-redpanda-plan.md` § W5).
3. **Flip `WORKER_DRY_RUN` to `"0"`** in `.github/workflows/prod-workflow.yml`
   and push to main (or, for an immediate flip without a deploy:
   `kubectl -n ratel-prod set env deploy/ratel-worker-default deploy/ratel-worker-analyze deploy/ratel-worker-egress RATEL_WORKER_DRY_RUN=0`
   — but commit the workflow change too, or the next deploy reverts it).
4. Watch worker logs + DLQ topic (`prod.ratel.cdc.v1.dlq`) for the first
   hours.

While shadowed, offsets keep advancing — events seen during shadow are
**skipped** on the flip, not replayed. That is by design (EventBridge already
processed them); it also means the flip itself must be prompt after step 2,
or events land in a gap where neither path runs handlers.

### Rollback (events)

Set `WORKER_DRY_RUN` back to `"1"` FIRST (re-shadow the workers), then
re-enable the EventBridge rules — the same two-together rule, in reverse
order, so no window exists where both paths are live.

## Rollback (app)

- **Bad deploy**: `git revert` on main → the workflow redeploys the previous
  sha (images are sha-tagged `prod-<sha>` and stay in the in-cluster
  registry). For an emergency pin without a build:
  `kubectl -n ratel-prod set image deploy/app-shell app-shell=registry.infra.svc.cluster.local:5000/ratel-app-shell:prod-<old-sha>`.
- **Whole-path fallback**: the AWS prod stack still serves its last-deployed
  version at `ratel.foundation`; nothing in this pipeline touches it (no
  more S3 sync / CloudFront invalidation / lambda update). The k3s
  `ratel-prod` namespace is invisible to users until the `ratel.foundation`
  DNS points at the cluster — and after the DNS cutover, pointing it back at
  CloudFront restores the AWS path.

## DNS cutover (app)

When ready to serve `ratel.foundation` from the cluster:

1. Verify `https://ratel-prod.biyard.co` end to end (it is the same
   Deployment the real domain will hit).
2. Confirm the `ratel-foundation-tls` certificate is issued:
   `kubectl -n ratel-prod get certificate`.
3. Point the `ratel.foundation` A/ALIAS record at the cluster's public
   entrypoint (currently CloudFront). Keep the CloudFront distribution and
   S3 bucket as the rollback target.
4. Watch traffic/error rates; rollback is re-pointing the DNS.

## Job graph

| Job | Status | Notes |
|---|---|---|
| `build-images` | NEW | native arm64 `make build` + events cargo build → 2 images; restores the `ratel-cargo-*`/`ratel-target-*` caches the dev branch keeps warm |
| `deploy` | REPLACED | was cdk deploy → now envsubst/kubectl into `ratel-prod` |
| `smoke-test` | NEW | public URL probe (`ratel-prod.biyard.co`) with the real certificate |
| `ratel-app-lambda` | REMOVED | AWS machinery (lambda build, S3 sync, CloudFront invalidation, lambda zip artifact); `cdk/` + `make build-lambda` remain in-repo |
| `android-release` | KEPT | Play internal track, points at `ratel.foundation` (follows whichever backend that DNS serves) |
| `ios-release` | KEPT | TestFlight, same DNS-following behavior |

## Debugging

- Deploy failures dump pod state + events + per-deployment logs into the job
  log (`Dump pod state on failure`).
- Events tier: `kubectl -n ratel-prod logs deploy/ratel-worker-default`
  (`ratel-cdc`, `-analyze`, `-egress` likewise). The deploy job's events
  check warns (non-fatally) when no `worker consuming` /
  `dry-run: would dispatch` line is visible yet.
- Manifest sanity:
  `SHA_TAG=prod-test WORKER_DRY_RUN=1 envsubst '${SHA_TAG} ${WORKER_DRY_RUN}' < deploy/prod/<f>.yaml | kubectl apply --dry-run=client -f -`.
