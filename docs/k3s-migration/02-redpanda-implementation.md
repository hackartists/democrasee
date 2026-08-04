# Redpanda 이벤트 파이프라인 구현 계획

**작성일**: 2026-08-04 · **브랜치**: `deploy/migrate-into-k3s`
**선행 문서**: [01-events-redpanda-plan.md](01-events-redpanda-plan.md)

## 0. 확인된 k3s 인프라 (k3s-home 클러스터, VPN 접근 확인 완료)

| 항목 | 값 |
|---|---|
| Redpanda | `infra` ns, StatefulSet `redpanda-0` (single-node, RF=1) |
| Kafka 엔드포인트 | `redpanda-0.redpanda.infra.svc.cluster.local:9093` — **VPN에서 클러스터 DNS/pod IP 모두 접근 확인됨** |
| 인증 | 없음 (SASL off, plaintext). `auto_create_topics_enabled: true` |
| 콘솔 | `redpanda-console` svc (8080) |
| 기존 토픽 네이밍 | `prod.<service>.<action>.v1` (타 프로젝트) — ratel은 `{env}.ratel.cdc.v1` 사용 |
| 노드 | arm64 (Raspberry Pi + lima) — k3s 배포 시 arm64 이미지 필요 (이번 범위 아님) |

## 1. 결정 사항 (구현 계약)

### 1-1. Cargo feature / 의존성

```toml
# app/ratel/Cargo.toml
rdkafka = { version = "0.38", features = ["cmake-build", "tokio"], optional = true }

[features]
redpanda = ["server", "dep:rdkafka"]
```

- `redpanda` feature는 **web/mobile/lambda/기본 server 빌드에 영향 없음**. CDC·워커
  실행 시에만 `--features redpanda`.
- 바이너리는 기존 `src/bin/` 패턴(파일 자동 발견 + `#[cfg(not(feature = ...))] fn main`
  가드)을 따른다 — `required-features` 불필요.

### 1-2. 모듈 구조 (신규)

```
app/ratel/src/common/events/
├── mod.rs
├── cdc_event.rs      # CdcEvent 타입 + Streams 레코드 변환
├── config.rs         # 런타임 env 설정 (EventsConfig)
├── dispatcher.rs     # 통합 디스패처 (39 Pipe 필터 이관)
├── checkpoint.rs     # CDC shard 체크포인트 (DynamoDB)
├── producer.rs       # Redpanda producer 래퍼
└── consumer.rs       # Redpanda consumer 루프 (워커)

app/ratel/src/bin/
├── ratel_cdc.rs      # DynamoDB Streams → Redpanda
└── ratel_worker.rs   # Redpanda → dispatcher
```

### 1-3. 메시지 포맷 (`CdcEvent`, JSON, topic key = pk 문자열)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    pub schema_version: u32,                       // 1
    pub event_name: String,                        // "INSERT" | "MODIFY" | "REMOVE"
    pub keys: HashMap<String, serde_dynamo::AttributeValue>,
    pub new_image: Option<HashMap<String, serde_dynamo::AttributeValue>>,
    pub old_image: Option<HashMap<String, serde_dynamo::AttributeValue>>,
    pub approximate_creation_ms: Option<i64>,
    pub sequence_number: Option<String>,
    pub source_table: String,
}
```

- REMOVE 이벤트도 old_image를 정직하게 old_image 필드에 담는다
  (EventBridge `inputTemplate`의 OldImage→newImage 치환 꼼수 제거).
- Kafka 파티션 key = `keys["pk"]` 문자열 → per-entity 순서 보장 (Streams와 동등).

### 1-4. 런타임 환경변수 (신규 코드는 `std::env::var` — `option_env!` 금지)

| env | 기본값 | 용도 |
|---|---|---|
| `RATEL_KAFKA_BROKERS` | (필수) | 예: `redpanda-0.redpanda.infra.svc.cluster.local:9093` |
| `RATEL_CDC_TOPIC` | `dev.ratel.cdc.v1` | CDC 토픽 |
| `RATEL_CDC_DLQ_TOPIC` | `{topic}.dlq` | DLQ 토픽 |
| `RATEL_CDC_START` | `latest` | 체크포인트 없을 때 시작점: `latest` \| `trim_horizon` |
| `RATEL_WORKER_ROLE` | `all` | `default` \| `analyze` \| `egress` \| `all` |
| `RATEL_WORKER_GROUP` | `ratel-worker-{role}` | consumer group id |
| `RATEL_STREAM_POLLER` | `on` | `off`면 local-dev 인프로세스 poller 비활성 (CDC/워커 테스트 시 이중 처리 방지) |

### 1-5. 체크포인트 (CDC)

- 저장소: DynamoDB 메인 테이블. `pk = "CDC_CHECKPOINT#{table_name}"`,
  `sk = "SHARD#{shard_id}"`, attrs: `seq` (마지막 처리 sequence number), `updated_at`.
- produce 배치 delivery 확인 후 커밋. 재기동 시 `AFTER_SEQUENCE_NUMBER(seq)` 이터레이터,
  체크포인트 없으면 `RATEL_CDC_START`.
- shard 분할/병합: `describe_stream` 주기 갱신(기존 poller 골격 재사용).

### 1-6. 디스패처 (`dispatcher.rs`) — 이번 작업의 핵심

- 시그니처: `pub async fn dispatch(event: &CdcEvent, roles: RoleSet) -> Vec<DispatchOutcome>`
- **`cdk/lib/dynamo-stream-event.ts`의 39개 Pipe 필터를 1:1 이관** — sk prefix,
  eventName, status/visibility/state/publish_state, OldImage 조건 포함.
  필터 매트릭스 문서(`docs/k3s-migration/03-filter-matrix.md`)로 이관 검증.
- 이벤트 클래스 → 워커 role 라우팅:

| EventClass | Role | 대상 (detailType 기준) |
|---|---|---|
| `Default` | worker-default | Timeline/Popular/Notification/XP/Essence/Space lifecycle/Vector/AiModerator/CharacterXp/ActivityScore |
| `Analyze` | worker-analyze | AnalyzeReportInProgress, AnalyzeDiscussionInProgress |
| `Egress` | worker-egress | PostPublishedForSyndication, SyndicationJobReady, InboxPushFanout |
| `Sse` | (워커 제외) | FactFoldChat — API 프로세스 인프로세스 hub 전용. 워커 role에 포함 안 됨 |

- `fanout_hot_space` 후처리를 프로드(`proc()`)와 동일하게 디스패처에 포함.
- 프로드/로컬 불일치 6건 해소 (01 문서 §1 참조).
- `stream_handler::handle_stream_record`는 디스패처 위임 래퍼로 축소
  (local-dev poller 경로 호환 유지, Sse 포함 전체 role로 호출).
- **`EventBridgeEnvelope::proc()`는 이번 단계에서 손대지 않는다** — 프로드 경로는
  컷오버 시점까지 현행 유지(리스크 격리). 컷오버 후 스택 해체와 함께 제거.

### 1-7. 워커 (`ratel_worker.rs`)

- rdkafka `StreamConsumer`, `enable.auto.commit=false`, 처리 성공 후 커밋.
- 메시지당: CdcEvent 파싱 → `dispatch(event, role)` → 실패 시 재시도 3회
  (백오프 1s/5s/15s) → 최종 실패 시 DLQ produce 후 커밋(파이프라인 정체 방지).
- `analyze` role: `max.poll.interval.ms=600000` (5분 작업 + 여유).

### 1-8. CDC (`ratel_cdc.rs`)

- `stream_poller.rs` 승격: 체크포인트 + LATEST/TRIM_HORIZON + produce(acks=all,
  delivery 확인 후 체크포인트 갱신). 1초 폴링(현행 5초에서 단축).
- 필터링 없이 **모든 스트림 레코드를 produce** (필터는 컨슈머 디스패처 책임).
  단, `CDC_CHECKPOINT#` pk 자기 자신의 변경은 produce 제외(무한 루프 방지).

## 2. 작업 분담 (서브에이전트)

| # | 작업 | 담당 | 의존 |
|---|---|---|---|
| T2 | 스캐폴딩: feature, events 모듈 뼈대, CdcEvent, config, dispatcher 스텁 | 메인 | — |
| T3 | 디스패처 구현 + stream_handler 위임 + 필터 매트릭스 + 유닛 테스트 | Agent A | T2 |
| T4 | CDC 프로듀서 (checkpoint.rs, producer.rs, ratel_cdc.rs) | Agent B | T2 (T3와 병렬) |
| T5 | 워커 (consumer.rs, ratel_worker.rs) | Agent C | T2 (T3와 병렬, dispatch 스텁 사용) |
| T6 | 토픽 생성, 빌드 매트릭스 검증, e2e 스모크, 테스트 가이드 | 메인 | T3~T5 |

## 3. 검증 계획

1. **빌드 매트릭스**: `cargo check --features server` / `--features web` /
   `--features "server,redpanda"` / `dx check --web` — 전부 `RUSTFLAGS='-D warnings'`.
2. **디스패처 유닛 테스트**: 필터 매트릭스의 대표 케이스(각 detailType별 매치/비매치,
   OldImage 조건, REMOVE 경로)를 CdcEvent 픽스처로 검증.
3. **e2e 스모크** (로컬): localstack DynamoDB(`make infra`) → `ratel_cdc`
   (브로커=k3s Redpanda) → `rpk topic consume`으로 이벤트 도달 확인 →
   `ratel_worker --role all` 로그로 디스패치 확인.
4. **사용자 테스트 가이드**: `docs/k3s-migration/04-testing-guide.md`.

## 4. 이번 범위에서 제외 (후속)

- `EventBridgeEnvelope::proc()` 위임/제거 — 컷오버 시.
- k3s Deployment 매니페스트 + arm64 이미지 빌드 — 코드 검증 후.
- dev/prod 컷오버 절차 실행 (01 문서 W5).
- XP 핸들러 멱등성 보강(조건부 쓰기) — 컷오버 전 별도 PR.
