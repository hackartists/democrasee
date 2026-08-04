# Phase 1 마이그레이션 계획: DynamoDB Streams + EventBridge → Redpanda

**작성일**: 2026-08-04 (범위 확정: S3/SNS 유지 결정 반영) · **브랜치**: `deploy/migrate-into-k3s`
**선행 문서**: [aws-infrastructure-inventory.md](aws-infrastructure-inventory.md)

## 범위 결정 (2026-08-04)

| 항목 | 결정 | 근거 |
|---|---|---|
| **DynamoDB** | AWS 유지 | 이 단계 비범위. 이벤트 근원은 계속 DynamoDB Streams |
| **S3** | **AWS 유지** (MinIO 이전 취소) | 기존 데이터량이 많음. presigned 업로드/서빙 현행 유지. k3s 앱은 IAM 키로 S3 API 호출 가능하므로 이전 불필요 |
| **SNS** | **AWS 유지** | 조사 결과 pub/sub이 아니라 SMS 발송 전용(`send_code.rs:83`, topic 없음). Redpanda 대상 아님. IAM 키만으로 k3s에서 동작 |
| **Streams + EventBridge** | **Redpanda로 마이그레이션** | 본 문서의 범위. 39 Pipes + 34 Rules + Lambda 3종 이벤트 경로 전체 대체 |

참고(조사 기록): S3를 MinIO로 옮길 경우 endpoint override 부재(`aws_s3.rs`),
`force_path_style` 미지원, `get_url()`의 `https://{bucket}/{key}` 하드코딩
(`common/utils/aws/s3.rs:86`) 수정이 필요하다. 공개 URL이 도메인 기반이라
DNS 전환만으로 기존 asset URL을 살릴 수 있다는 점도 확인됨 — 향후 재검토 시 참조.

---

## 1. 현재 구조 (조사 결과)

- **프로듀서**: 앱은 EventBridge에 직접 publish하지 않는다(`PutEvents` 0건).
  모든 이벤트는 DynamoDB 쓰기 → Streams → **39 Pipes**(sk prefix + eventName +
  status/visibility/state/OldImage 필터, `cdk/lib/dynamo-stream-event.ts` ~1,965줄)
  → bus `ratel-{env}-bus` → **34 Rules**(detailType) → Lambda 3종.
- **Lambda 타깃 3종**: default(API Lambda), analyze(1GB/5분), non-VPC egress
  (cross-posting·FCM push — VPC Lambda의 NAT 부재 우회용).
- **컨슈머**: `EventBridgeEnvelope::proc()` (`common/types/event_bridge_envelope.rs`)의
  34개 DetailType match arm이 핸들러 호출. 약 12개 arm 뒤에 `fanout_hot_space`
  후처리가 붙는다.
- **로컬 패리티**: `stream_poller.rs`(Latest iterator, 5초 폴링, **체크포인트 없음**,
  재시작 시 백로그 유실) → `stream_handler.rs`(sk prefix 디스패치).
- **프로드/로컬 불일치 (조사에서 확인된 6건)**:
  1. `PopularPostUpdate` — 로컬 브랜치 자체가 없음 (인기글 fan-out이 로컬에서 안 돎)
  2. `SPACE_ANALYZE_REPORT#` — 로컬에 `status=in_progress` 가드 없음 (과잉 실행)
  3. `fanout_hot_space` — 프로드(`proc()`) 전용, 로컬 미실행
  4. `FACT_FOLD_CHAT#` SSE fan-out — 로컬 전용, 프로드 Pipe 없음
  5. `SUB_TEAM_ANNOUNCEMENT#` INSERT 케이스 — 로컬에만 존재 (프로드는 MODIFY만)
  6. `parse_detail`이 REMOVE 이벤트에서도 `newImage` 필드를 읽음 — REMOVE Pipe들이
     `inputTemplate`으로 OldImage를 newImage 자리에 밀어넣어 보정 중
- 스케줄(cron) 룰 없음. DynamoDB 외 이벤트 소스는 AI 스택의 S3 ObjectCreated 하나뿐
  (Bedrock 거취와 함께 별도 처리).

## 2. 목표 아키텍처

DynamoDB가 AWS에 남으므로 이벤트의 근원은 여전히 DynamoDB Streams다.
Redpanda는 "Streams를 읽어 토픽화하는 CDC 프로듀서"와 "토픽을 소비하는 워커" 사이의
버스가 된다:

```
DynamoDB Streams (AWS)
   │  DescribeStream/GetRecords + 체크포인트
   ▼
ratel-cdc (k3s Deployment, 1 replica)          ← stream_poller.rs 승격
   │  produce: topic ratel.cdc, key = pk (per-entity 순서 보장)
   ▼
Redpanda (k3s, dev single-node → prod 3-node RF=3)
   │
   ├─ consumer group: worker-default   → 타임라인/알림/XP/essence/공간 라이프사이클
   ├─ consumer group: worker-analyze   → AnalyzeReport/Discussion (1GB, 최대 5분 작업)
   ├─ consumer group: worker-egress    → cross-posting, FCM push (외부 egress)
   └─ consumer group: api-{pod-uid}    → FactFoldChat → SSE hub (API pod별 독립 그룹)
```

### 설계 결정과 근거

| 결정 | 선택 | 근거 |
|---|---|---|
| 토픽 설계 | 단일 raw CDC 토픽 `ratel.cdc`, key=pk | DynamoDB Streams의 per-item 순서 보장을 파티션 키로 그대로 승계. sk prefix 라우팅은 컨슈머 디스패처가 담당(현 stream_handler와 동일 모델). 토픽 분리는 병목이 실측되면 그때 |
| 메시지 포맷 | JSON: `{event_name, keys, new_image, old_image, approx_ts, seq}` (serde_dynamo AttributeValue) | 핸들러가 이미 소비하는 형태. REMOVE 이벤트에서 OldImage를 newImage 자리에 넣던 꼼수(불일치 6번) 제거 — old/new를 정직하게 분리 |
| 워커 분리 | 3개 consumer group (default/analyze/egress) | 현행 Lambda 3종 분리와 동형. analyze는 메모리·처리시간 특성이 달라 리소스/커밋 정책 분리 필요. egress 분리는 NAT 문제가 아니라(k3s엔 없음) 외부 API 지연·장애 격리 목적 |
| 필터 위치 | 컨슈머 디스패처(Rust)로 일원화 | 39개 Pipe의 status/visibility/OldImage 필터를 코드로 이관. 프로드/로컬 불일치 6건을 해소하는 유일한 지점. `dynamo-stream-event.ts`가 이관 스펙의 원본 |
| 전달 보장 | at-least-once + 핸들러 멱등성 감사 | Pipes도 at-least-once였으므로 의미론 동일. XP 기록류(INSERT 기반)는 중복 유입 시 이중 지급 가능성 — 컷오버 전 감사 |
| DLQ | `ratel.cdc.dlq` 토픽 + 재시도 N회 | 현행은 로그 후 삼킴(Lambda 재시도 의존). 명시적 DLQ + 재처리 커맨드로 개선 |
| CDC 고가용성 | 1 replica + 체크포인트, 재시작 시 이어읽기 | 현 poller의 최대 결함(Latest iterator, 재시작 유실)을 체크포인트로 해결. 리더 선출 불필요 — K8s 재기동으로 충분 |

## 3. 작업 항목

### W1. 디스패처 통합 리팩토링 — 크리티컬 패스, 인프라 무관, 즉시 착수 가능

1. `stream_handler.rs`를 "이벤트 디스패처" 모듈로 승격:
   - 39개 Pipe 필터(sk prefix + eventName + status/visibility/state/OldImage 조건)를
     Rust 매칭으로 완전 이관. **필터 매트릭스 체크리스트**를 먼저 작성해
     `dynamo-stream-event.ts` 대비 누락을 방지.
   - `EventBridgeEnvelope::proc()`에만 있던 `fanout_hot_space` 후처리를 디스패처로 이동.
   - 불일치 해소: `PopularPostUpdate` 로컬 추가, `SPACE_ANALYZE_REPORT#` status 가드,
     `SUB_TEAM_ANNOUNCEMENT#` INSERT 케이스 프로드/로컬 정합.
2. 세 경로가 디스패처를 공유: (a) 기존 local-dev poller, (b) 신규 Redpanda 워커,
   (c) 전환기의 EventBridge Lambda(`proc()`이 디스패처에 위임).
   → 전환 전후 동작 동등성이 코드 레벨에서 보장됨.
3. 독립 PR로 분리 — 이것만으로도 로컬/프로드 동작 불일치 해소라는 가치가 있음.

### W2. CDC 프로듀서 (`ratel-cdc`)

1. `stream_poller.rs` 기반 신규 바이너리(또는 app-shell 실행 모드):
   - shard 체크포인트 저장 — DynamoDB 메인 테이블에 `pk=CDC_CHECKPOINT#{shard}` 권장
     (Redpanda compacted topic 대안보다 단순).
   - 최초 기동 시 컷오버 시각 기준 `LATEST`(백필 필요 시 `TRIM_HORIZON`),
     이후 체크포인트 이어읽기.
   - shard 분할/병합 추적(`describe_stream` 주기 갱신 — 기존 코드 골격 재사용).
   - rdkafka로 `ratel.cdc` produce, key=pk, acks=all.
2. 신규 코드는 컴파일타임 `option_env!` 대신 **런타임 env**로 작성
   (k8s에서 이미지 하나로 dev/prod 운용 대비. 앱 전체의 option_env! 정리는 별도 단계).

### W3. 워커 (`ratel-worker-*`)

1. Redpanda 컨슈머 루프 + W1 디스패처 호출. 처리 성공 후 수동 오프셋 커밋.
2. 그룹별 배포: default / analyze(메모리 1Gi+, `max.poll.interval.ms` 상향) /
   egress(cross-posting·push).
3. 재시도 + DLQ produce. DLQ 재처리 운영 커맨드(기존 `bin/run_pending_discussions.rs`
   패턴 확장).
4. **FactFoldChat SSE**: 인프로세스 SSE hub이므로 워커가 소비하면 API pod의
   클라이언트에 못 보낸다 → API pod가 각자 고유 consumer group으로
   `FACT_FOLD_CHAT#` 이벤트를 직접 구독해 자기 hub에 fan-out.
   (Redpanda 도입이 이 기능의 멀티팟 확장을 처음으로 가능하게 함.)

### W4. 인프라 (k3s + 로컬)

1. Redpanda 배포 — dev single-node로 시작, prod 3-node(RF=3). Helm chart 또는
   Redpanda Operator. Console 포함.
2. 토픽 프로비저닝: `ratel.cdc`(파티션 수는 DynamoDB shard 수 이상, 예: 12),
   `ratel.cdc.dlq`. retention은 재처리 요구 기준(예: 7일).
3. docker-compose에 redpanda 추가 → 로컬 개발도 동일 파이프라인으로 통일
   (기존 poller 직결 모드는 폴백으로 유지).
4. 워커/CDC는 AWS 자격증명(DynamoDB Streams 읽기) 필요 — k3s Secret으로 주입.

### W5. 컷오버 (dev → prod)

1. **Shadow**: dev에서 CDC + 워커 기동, EventBridge Rules 활성 유지, 워커는
   dry-run(로그만). 이벤트 도달률/필터 일치율을 프로드 경로와 비교.
2. **dev 컷오버**: EventBridge Rules 일괄 disable → 워커 dry-run 해제.
   **동시 활성화 구간을 만들지 않는다** — XP 등 비멱등 핸들러의 이중 처리 방지.
   CDC는 disable 이전부터 체크포인트를 쌓고 있으므로 갭 없음.
3. 1~2주 관찰 — 컨슈머 랙, DLQ 유입, 도메인 지표(타임라인 반영, 알림, XP,
   essence 인덱싱, cross-posting, push).
4. **prod 컷오버**: 동일 절차.
5. **해체**: `ratel-{env}-stream-ap-northeast-2` 스택 삭제(39 Pipes + 34 Rules),
   bus 사용 중단. Lambda의 EventBridge self-routing(`run.rs`의 lambda feature 분기)은
   컴퓨트 마이그레이션 때 함께 제거.

**롤백**: EventBridge Rules re-enable + 워커 정지로 즉시 복귀.
CDC 체크포인트는 보존하면 재컷오버 시 이어읽기 가능.

## 4. 리스크

| 리스크 | 완화 |
|---|---|
| XP/알림 등 비멱등 핸들러의 중복 처리 (at-least-once) | 컷오버 전 멱등성 감사. XP 기록은 (user, source-entity) 조건부 쓰기로 보강 |
| CDC 프로듀서 단일 장애점 | 1 replica + K8s 재기동 + 체크포인트로 유실 없음. 컨슈머 랙/CDC 지연 알림 설정 |
| 처리 순서 의존 핸들러 (예: `SpaceActionStatusChange`의 old/new 비교) | key=pk 파티셔닝으로 per-entity 순서 유지 — Streams와 동등 |
| analyze 장시간 처리로 컨슈머 그룹 리밸런싱 | 별도 그룹 + poll interval 상향, 필요 시 K8s Job 위임 |
| Pipe 필터 → Rust 필터 이관 누락 | 필터 매트릭스 체크리스트 + shadow 단계 이벤트 카운트 비교 |
| DynamoDB Streams 24시간 보존 한계 | CDC 중단 24시간 초과 시 유실 — CDC 지연 모니터링 필수, 알림 임계 6시간 |

## 5. 실행 순서

```
B1. 디스패처 통합 리팩토링 (필터 이관)   ← 즉시 착수, 독립 PR
B2. Redpanda k3s 배포 + docker-compose
B3. CDC 프로듀서 + 워커 구현
B4. dev shadow → dev 컷오버 → 1~2주 관찰
B5. prod 컷오버 → stream 스택 해체
```

완료 시 잔존 AWS 의존: DynamoDB(+Streams는 CDC 소스로만), S3, SES, SNS(SMS),
Bedrock, CloudFront/Route53/ACM/API GW(컴퓨트 단계에서 해소), ECR.
