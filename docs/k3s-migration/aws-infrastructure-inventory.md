# AWS Infrastructure Inventory (k3s 마이그레이션 사전 조사)

**작성일**: 2026-08-04 · **브랜치**: `deploy/migrate-into-k3s`
**소스**: `cdk/bin/cdk.ts`, `cdk/lib/*.ts`, `.github/workflows/{dev,prod}-workflow.yml`, `app/ratel/src/common/config/server/`

리전: **ap-northeast-2 단일 리전** (2026-04 멀티리전 제거 완료, `remove-multi-region` 참조).
스테이지: `dev` (dev.ratel.foundation) / `prod` (ratel.foundation), 동일 스택 구성을 ENV로 분리.

---

## 1. 현재 배포 중인 CDK 스택 (`cdk/bin/cdk.ts` 기준)

| 스택 | 리전 | 내용 |
|---|---|---|
| `ratel-vpc-endpoints-ap-northeast-2` | ap-northeast-2 | 싱글톤(스테이지 공유). Shared SG(자기참조), DynamoDB Gateway Endpoint, Bedrock Runtime Interface Endpoint, SES Interface Endpoint. **NAT 없음** — VPC Lambda는 외부 인터넷 egress 불가 |
| `ratel-{env}-cluster-ap-northeast-2` | ap-northeast-2 | ECS Cluster(default VPC) + CloudMap Private DNS `ratel-{env}-svc.local`. 현재 실질 워크로드 없음(과거 Qdrant용, 잔존) |
| `ratel-{env}-svc-ap-northeast-2` | ap-northeast-2 | **API Lambda #1 (non-VPC)** — RegionalLambdaStack. cross-posting/push 등 인터넷 egress 필요한 이벤트 처리용 |
| `ratel-{env}-lambda-ap-northeast-2` | ap-northeast-2 | **API Lambda #2 (VPC-attached)** — 메인 API + Analyze Lambda. Qdrant CloudMap DNS 해석 위해 VPC 부착 |
| `GlobalAccel` | us-east-1 | CloudFront + ACM(us-east-1 필수) + S3 정적 버킷 + Route53 alias. 웹 진입점 |
| `ratel-{env}-dynamodb` | ap-northeast-2 | DynamoDB 단일 테이블 `ratel-{env}-main` (pk/sk, PAY_PER_REQUEST, PITR, Stream NEW_AND_OLD_IMAGES, deletion protection). 코드상 `replicationRegions: [eu-central-1, us-east-1]` 선언 잔존(주석상 실제 레플리카는 삭제됨 — 배포 전 정리 필요) |
| `ratel-{env}-stream-ap-northeast-2` | ap-northeast-2 | DynamoDB Streams → EventBridge Pipes(필터) → EventBus → Rules → Lambda. **약 40개 Pipe + 40개 Rule** |

### 코드에는 있으나 현재 미배포/주석 처리
- `RegionalServiceStack` (ECS Fargate API) — 주석 처리, Lambda로 대체됨
- `AlbStack` (Qdrant gRPC용 공유 ALB) — 주석 처리
- Qdrant ECS/EFS 스택 — 제거됨. **Qdrant는 biyard-infra-cluster의 공유 인스턴스를 사용** (`QDRANT_URL`/`QDRANT_API_KEY`/`QDRANT_PREFIX=ratel-{env}`)
- `DaemonStack` (fetcher, ECR `ratel/fetcher`, Fargate), `ImageWorkerStack` (SQS + Lambda), `StaticStack` — bin에서 인스턴스화 안 됨
- `cdk-ai.ts` (`AiStack`) — Bedrock Agent + Knowledge Base(Titan embed v2, Nova Pro, S3 `metadata.ratel.foundation` 데이터소스). 별도 수동 배포용

---

## 2. 컴퓨트 (요청 경로)

```
사용자 → Route53 → CloudFront({env}.ratel.foundation)
  ├─ /assets, /*.js, /*.wasm 등 정적 경로 → S3 (OAI, dx build 산출물)
  └─ default behavior → api.{host} (API GW custom domain)
        → API Gateway HTTP API → Lambda (bootstrap, PROVIDED_AL2023, x86_64)
```

| 함수 | 스펙 | 비고 |
|---|---|---|
| API Lambda (VPC) | 128MB / 30s | 메인 트래픽. `lambda-api.{host}` 도메인 |
| API Lambda (non-VPC) | 128MB / 30s | `api.{host}` 도메인 + cross-posting/FCM push 이벤트 타깃(NAT 부재 우회) |
| Analyze Lambda | 1024MB / 5min | lindera + LDA/TF-IDF/텍스트네트워크 분석 파이프라인 전용 |
| Arcade SSE | Function URL, RESPONSE_STREAM | API GW가 스트리밍 미지원이라 Lambda Function URL 직결. CORS + 쿠키 인증 |

- 같은 `bootstrap` 바이너리 하나가 API GW 이벤트 / EventBridge 이벤트 / Function URL을 self-routing.
- Route53 latency 레코드(A/AAAA)로 `api.{host}` → API GW regional domain 연결. 리전별 디버그 도메인 `ap-northeast-2.api.{host}`도 생성.

## 3. 데이터 & 이벤트

- **DynamoDB**: 단일 테이블 `ratel-{env}-main`. Streams가 이벤트 아키텍처의 근간.
- **EventBridge Pipes/Rules** (`dynamo-stream-event.ts`, ~1,965줄): sk prefix 필터로 도메인 이벤트화. 주요 detailType:
  - 피드/랭킹: `TimelineUpdate`, `PopularPostUpdate`, `PopularSpaceUpdate`
  - 벡터 인덱싱: `PostVectorIndex/Delete` (→ Qdrant)
  - AI 모더레이터: `AiModeratorReplyCheck/ReplyIndex`
  - 알림: `NotificationSend`, `InboxPushFanout` (FCM push → non-VPC Lambda)
  - XP/점수: `PollXpRecord`, `QuizXpRecord`, `DiscussionXpRecord`, `FollowXpRecord`, `ActivityScoreAggregate`, `CharacterXpDelta`
  - 스페이스 라이프사이클: `SpaceStatusChangeEvent`, `SpaceActionStatusChange`, `SpacePublished`, `SubTeamAnnouncementPublished`
  - 분석: `AnalyzeReportInProgress`, `AnalyzeDiscussionInProgress` (→ Analyze Lambda)
  - Essence(Notion 커넥터) 인덱싱: `EssenceIndex/Delete{Post,PostComment,DiscussionComment,Poll,Quiz}`, `EssenceActionMetadataUpdate`
  - 크로스포스팅: `PostPublishedForSyndication`, `SyndicationJobReady` (→ non-VPC Lambda)
- 로컬 개발 패리티: `stream_handler.rs` / `stream_poller.rs`가 같은 이벤트를 폴링 처리 → **k3s에서는 이 poller 방식이 스트림 처리 대체 후보**.

## 4. 사용 중인 AWS 관리형 서비스 (앱 런타임 의존성)

| 서비스 | 용도 | 앱 내 위치 |
|---|---|---|
| DynamoDB | 주 데이터베이스 (DynamoEntity 매크로) | `config/server/dynamodb.rs`, `DYNAMO_TABLE_PREFIX`/`DYNAMODB_ENDPOINT` |
| S3 | 자산 업로드(presigned, `BUCKET_NAME=metadata.ratel.foundation`), 웹 정적 호스팅 버킷 | `aws_s3.rs` |
| SES v2 | 이메일 발송 (`FROM_EMAIL`) | `aws_ses.rs` |
| SNS | SNS 클라이언트 (`SNS_REGION`) | `aws_sns.rs` |
| Bedrock | LLM 호출(InvokeModel*, IAM role로 허용) + Titan 임베딩 | `bedrock_embeddings_config.rs`, AiStack |
| CloudFront | CDN/웹 진입점, 배포시 invalidation | dev/prod workflow |
| Route53 | `ratel.foundation` hosted zone, alias/latency 레코드 | CDK |
| ACM | 웹(us-east-1)/API(리전) 인증서, DNS validation | CDK |
| API Gateway HTTP API | Lambda 프록시 + custom domain | CDK |
| ECR | `ratel/app-shell`(테스트용), `ratel/fetcher` | workflows, DaemonStack |
| CloudWatch Logs | Lambda/ECS 로그 | 암묵적 |
| SQS | image-worker 큐 (현재 미배포) | ImageWorkerStack |

## 5. AWS 외부 의존성 (k3s 이전과 무관하게 유지)

- **Qdrant** — biyard-infra-cluster 공유 인스턴스 (`QDRANT_URL/API_KEY`, prefix `ratel-{env}`)
- **Firebase/FCM** — push (`GOOGLE_APPLICATION_CREDENTIALS=.gcp/firebase-service-account.json`, FIREBASE_* env)
- **ICP** — `RATEL_CANISTER_ID`, `IC_URL=https://icp.dev.biyard.co`, `ICP_IDENTITY_PEM`
- **Kaia 블록체인** — `KAIA_ENDPOINT`(kairos), feepayer/owner 키
- **PortOne** — 결제 (STORE_ID, INICIS/KPN 채널키, API_SECRET)
- **Binance API**, **Telegram Bot**, **LinkedIn OAuth**(크로스포스팅), **Bluesky**(bsky.social), **WalletConnect**, **Slack webhook**, **Launchpad 파트너 API**
- 암호화 키: BBS_BLS_*/P256_* (검증 크리덴셜), `CROSS_POSTING_DATA_KEY` (AEAD, 1Password 백업), `VOTER_TAG_SECRET`, `ATTR_VOTING_AUTHORITY_JSON`

## 6. 배포 파이프라인 (GitHub Actions)

- `dev-workflow.yml` / `prod-workflow.yml`:
  1. `make build-lambda` (bootstrap 바이너리, 컴파일 타임 env 다수 — `option_env!` 주의)
  2. `aws s3 sync` (웹 정적) + `cloudfront create-invalidation`
  3. `make cdk-deploy-v2` → `cdk deploy --all --concurrency 3`
- PR workflow는 LocalStack(DynamoDB) + Docker 이미지(`ratel/app-shell`) 기반 테스트.

## 7. k3s 마이그레이션 관점 핵심 포인트

1. **Lambda → 상시 컨테이너**: 같은 axum 바이너리가 이미 서버 모드(`docker-compose`의 app-shell)로 돌아가므로 컨테이너화 자체는 완료 상태. API GW/Function URL(SSE 스트리밍) → Ingress로 통합 가능 (Ingress는 스트리밍 기본 지원 — SSE 우회 구조 제거 가능).
2. **DynamoDB Streams + EventBridge가 최대 난제**: ~40개 Pipe/Rule. 옵션: (a) DynamoDB를 AWS에 남기고 `stream_poller.rs`를 k3s 상시 워커로 승격, (b) ScyllaDB Alternator 등 대체 + poller, (c) DB 전면 교체. 로컬 개발용 poller가 이미 존재한다는 점이 (a)/(b)의 근거.
3. **DynamoDB 자체 거취 결정 필요**: 앱은 `DYNAMODB_ENDPOINT` override를 이미 지원(LocalStack 테스트 경로) → 호환 스토어 포인팅 가능.
4. **관리형 서비스 대체 매핑**: SES(→ SMTP relay 또는 SES API 유지), S3(→ MinIO 또는 S3 유지), Bedrock(→ 유지 또는 Ollama; `WriterAi`가 이미 aws/ollama/fixture 백엔드 추상화 보유), CloudFront+S3 정적(→ Ingress + 캐시 또는 CDN 유지).
5. **Analyze 워크로드**: 1GB/5분 스파이크성 작업 — k3s에서는 별도 Deployment 또는 Job 큐로 분리 권장.
6. **cron 성격 리소스 없음**: EventBridge scheduled rule은 현재 없음(모두 스트림 기반). 크로스포스팅 retry sweeper도 스트림 MODIFY 트리거.
7. **잔존물 정리 대상**: ECS Cluster/CloudMap(빈 클러스터), DynamoDB `replicationRegions` 선언, 주석 처리된 스택들, NAT 부재로 인한 Lambda 이원화(k3s에서는 자연 해소).
