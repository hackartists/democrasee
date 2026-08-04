# Redpanda 이벤트 파이프라인 테스트 가이드

**선행 문서**: [02-redpanda-implementation.md](02-redpanda-implementation.md) ·
필터 스펙: [03-filter-matrix.md](03-filter-matrix.md)

## 구성 요소 요약

| 바이너리 | 역할 | 실행 |
|---|---|---|
| `ratel_cdc` | DynamoDB Streams → Redpanda produce (샤드 체크포인트 포함) | `cargo run --bin ratel_cdc --features redpanda` |
| `ratel_worker` | Redpanda consume → 디스패처 → 핸들러 (재시도/DLQ) | `cargo run --bin ratel_worker --features redpanda` |

- 토픽 (k3s Redpanda에 생성 완료): `dev.ratel.cdc.v1` (12파티션), `dev.ratel.cdc.v1.dlq`
- 브로커: `redpanda-0.redpanda.infra.svc.cluster.local:9093` — **VPN 연결 시 로컬에서 클러스터 DNS로 바로 접근됨**
- 체크포인트: 메인 테이블 `pk=CDC_CHECKPOINT#{table}` / `sk=SHARD#{shard_id}`

## 환경변수 (런타임)

| env | 기본값 | 비고 |
|---|---|---|
| `RATEL_KAFKA_BROKERS` | (필수) | `redpanda-0.redpanda.infra.svc.cluster.local:9093` |
| `RATEL_CDC_TOPIC` | `dev.ratel.cdc.v1` | |
| `RATEL_CDC_DLQ_TOPIC` | `{topic}.dlq` | |
| `RATEL_CDC_START` | `latest` | `trim_horizon`으로 백필 가능 |
| `RATEL_WORKER_ROLE` | `all` | `default` / `analyze` / `egress` / `all` (all은 SSE 제외) |
| `RATEL_WORKER_GROUP` | `ratel-worker-{role}` | |
| `RATEL_STREAM_POLLER` | `on` | `off`면 앱 내장 local-dev poller 비활성 (이중 처리 방지) |
| `DYNAMO_TABLE_PREFIX` | `ratel-local` | CDC 체크포인트/테이블 대상. 런타임 env 우선 |

## 로컬 e2e 테스트 절차

```bash
# 1. 로컬 인프라 (LocalStack DynamoDB, streams 활성)
AWS_REGION=ap-northeast-2 AWS_DEFAULT_REGION=ap-northeast-2 make infra

# 2. CDC 프로듀서 (터미널 1)
cd app/ratel
RATEL_KAFKA_BROKERS=redpanda-0.redpanda.infra.svc.cluster.local:9093 \
DYNAMO_TABLE_PREFIX=ratel-local \
  cargo run --bin ratel_cdc --features redpanda

# 3. 워커 (터미널 2)
cd app/ratel
RATEL_KAFKA_BROKERS=redpanda-0.redpanda.infra.svc.cluster.local:9093 \
DYNAMO_TABLE_PREFIX=ratel-local \
RATEL_WORKER_ROLE=all \
  cargo run --bin ratel_worker --features redpanda

# 4. 앱 (터미널 3) — 내장 poller 끄고 실행 (이중 처리 방지)
cd app/ratel
RATEL_STREAM_POLLER=off DYNAMO_TABLE_PREFIX=ratel-local \
  dx serve --port 8000 --web
```

이후 앱에서 포스트 작성/발행, 팔로우, 알림 발생 등 액션을 수행하면:
- CDC 로그에 produce 기록
- 워커 로그에 `dispatched` (matched rule 이름들, debug 레벨 — `RUST_LOG=debug` 권장)
- 실패 시 `dev.ratel.cdc.v1.dlq`에 `{"failed_rules": [...], "event": ...}` 적재

## 이벤트 확인 (Redpanda 쪽)

```bash
# 토픽 tail (k3s)
kubectl exec -n infra redpanda-0 -c redpanda -- rpk topic consume dev.ratel.cdc.v1 --offset end -n 5

# DLQ 확인
kubectl exec -n infra redpanda-0 -c redpanda -- rpk topic consume dev.ratel.cdc.v1.dlq --offset start -n 10

# consumer group 랙
kubectl exec -n infra redpanda-0 -c redpanda -- rpk group describe ratel-worker-all
```

Redpanda Console(웹 UI)로도 확인 가능: `kubectl port-forward -n infra svc/redpanda-console 8090:8080` → http://localhost:8090

## 체크포인트 확인/리셋

```bash
# 확인
aws dynamodb query --endpoint-url http://localhost:4566 --region ap-northeast-2 \
  --table-name ratel-local-main \
  --key-condition-expression "pk = :pk" \
  --expression-attribute-values '{":pk":{"S":"CDC_CHECKPOINT#ratel-local-main"}}'

# 리셋 (처음부터 다시 읽게 하려면 해당 row 삭제 후 RATEL_CDC_START=trim_horizon)
```

## 시나리오별 기대 동작

| 액션 | 기대 룰 (worker 로그) |
|---|---|
| 포스트 발행 (draft→published) | `TimelineUpdate`, `PostVectorIndex`, (public이면) `PostPublishedForSyndication`(egress) |
| 알림 생성 | `NotificationSend` → 이어서 `InboxPushFanout`(egress) |
| 팔로우 | `FollowXpRecord` |
| 스페이스 코멘트 작성 | `DiscussionXpRecord`, `AiModeratorReplyIndex`, `EssenceIndexDiscussionComment` |
| 분석 리포트 시작 | `AnalyzeReportInProgress` (role=analyze 또는 all) |

전체 룰 매핑: [03-filter-matrix.md](03-filter-matrix.md)

## 알려진 제약 / 주의

1. **FactFoldChat(SSE)는 워커가 처리하지 않는다** — `RoleSet::from_worker_role`이 Sse를
   제외. 로컬에서는 앱 내장 poller가 처리(즉 arcade SSE 테스트는 `RATEL_STREAM_POLLER=on`
   유지 필요 — 이 경우 다른 이벤트가 이중 처리되므로 SSE 테스트와 파이프라인 테스트는
   분리해서 진행).
2. **at-least-once** — CDC 재시작/워커 재시도 시 중복 디스패치 가능. XP 기록류 멱등성
   보강은 컷오버 전 별도 작업(01 문서 리스크 참조).
3. 워커 graceful shutdown: SIGTERM 수신 시 처리 중 메시지 완료+커밋 후 종료.
4. 프로드 Lambda 경로(`EventBridgeEnvelope::proc()`)는 이번 단계에서 변경되지 않음 —
   컷오버 전까지 AWS 경로는 현행 유지.
