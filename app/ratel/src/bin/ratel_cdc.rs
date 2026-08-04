//! DynamoDB Streams → Redpanda CDC producer.
//!
//! Promotes the local-dev `stream_poller` loop into a checkpointed producer:
//! every stream record is serialized as a `CdcEvent` and produced to the CDC
//! topic (key = item pk, acks=all + idempotence). After a shard batch has
//! been fully delivery-acknowledged, the last sequence number is checkpointed
//! in the main DynamoDB table so a restart resumes with
//! `AFTER_SEQUENCE_NUMBER(seq)`; shards without a checkpoint start at
//! `RATEL_CDC_START` (`latest` | `trim_horizon`).
//!
//! No filtering happens here — that is the consumer dispatcher's job. The
//! only records this binary skips are its own `CDC_CHECKPOINT#` rows
//! (self-feed guard); their sequence numbers are still checkpointed past.
//!
//! Usage:
//!   RATEL_KAFKA_BROKERS=<host:port> \
//!     cargo run --bin ratel_cdc --features redpanda

#[cfg(not(feature = "redpanda"))]
fn main() {
    eprintln!("ratel_cdc requires --features redpanda");
    std::process::exit(1);
}

#[cfg(feature = "redpanda")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, Instant};

    use app_shell::common::CommonConfig;
    use app_shell::common::events::checkpoint::{CheckpointStore, epoch_ms, main_table_name};
    use app_shell::common::events::producer::CdcProducer;
    use app_shell::common::events::{
        CDC_CHECKPOINT_PK_PREFIX, EventsConfig, cdc_event_from_stream_record,
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,app_shell=debug")),
        )
        .init();

    let config = match EventsConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ratel_cdc: {e}");
            std::process::exit(1);
        }
    };

    let cfg = CommonConfig::default();
    let dynamodb = cfg.dynamodb().clone();
    let table_name = main_table_name();

    tracing::info!(
        brokers = %config.brokers,
        topic = %config.topic,
        table = %table_name,
        start = ?config.start,
        "ratel_cdc starting"
    );

    let streams_client = build_streams_client();
    let stream_arn = wait_for_stream_arn(&dynamodb, &table_name).await;
    tracing::info!(stream_arn = %stream_arn, "DynamoDB stream discovered");

    let store = CheckpointStore::new(dynamodb, table_name.clone());
    // shard_id → last checkpointed sequence number (in-memory mirror of the
    // checkpoint table, updated after every successful save).
    let mut checkpoints: HashMap<String, String> = store.load_all().await?;
    tracing::info!(count = checkpoints.len(), "loaded shard checkpoints");

    let producer = CdcProducer::new(&config.brokers)?;

    // shard_id → live shard iterator. A shard with no entry gets its iterator
    // rebuilt from the checkpoint (or RATEL_CDC_START) at the next discovery.
    let mut shard_iterators: HashMap<String, String> = HashMap::new();
    // Shards whose stream is fully consumed (get_records returned no
    // next_shard_iterator) — never polled again.
    let mut closed_shards: HashSet<String> = HashSet::new();

    // Metrics: produced counts per shard over the current 60s window, plus
    // the age of the newest record seen per shard (best lag hint DynamoDB
    // Streams offers — there is no MillisBehindLatest like Kinesis).
    let mut window_counts: HashMap<String, u64> = HashMap::new();
    let mut last_event_age_ms: HashMap<String, i64> = HashMap::new();
    let mut last_metrics = Instant::now();
    let mut last_discovery: Option<Instant> = None;

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        // ── Shard discovery (every 10s, or immediately when nothing is live) ──
        let need_discovery = shard_iterators.is_empty()
            || last_discovery.is_none_or(|t| t.elapsed() >= Duration::from_secs(10));
        if need_discovery {
            last_discovery = Some(Instant::now());
            match streams_client
                .describe_stream()
                .stream_arn(&stream_arn)
                .send()
                .await
            {
                Ok(output) => {
                    if let Some(desc) = output.stream_description() {
                        for shard in desc.shards() {
                            let shard_id = shard.shard_id().unwrap_or_default().to_string();
                            if shard_id.is_empty()
                                || shard_iterators.contains_key(&shard_id)
                                || closed_shards.contains(&shard_id)
                            {
                                continue;
                            }
                            match new_shard_iterator(
                                &streams_client,
                                &stream_arn,
                                &shard_id,
                                checkpoints.get(&shard_id).map(String::as_str),
                                config.start,
                            )
                            .await
                            {
                                Ok(Some(iter)) => {
                                    tracing::info!(shard_id = %shard_id, "polling shard");
                                    shard_iterators.insert(shard_id, iter);
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    tracing::warn!(shard_id = %shard_id, error = %e, "failed to create shard iterator");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(stream_arn = %stream_arn, error = %e, "failed to describe stream");
                }
            }
        }

        // ── Poll each live shard ──
        let shard_ids: Vec<String> = shard_iterators.keys().cloned().collect();
        for shard_id in shard_ids {
            let iterator = match shard_iterators.get(&shard_id) {
                Some(i) => i.clone(),
                None => continue,
            };
            match streams_client
                .get_records()
                .shard_iterator(iterator)
                .limit(100)
                .send()
                .await
            {
                Ok(output) => {
                    let mut last_seq: Option<String> = None;
                    let mut batch_failed = false;

                    for record in output.records() {
                        let seq = record
                            .dynamodb()
                            .and_then(|d| d.sequence_number())
                            .map(str::to_string);
                        let event = cdc_event_from_stream_record(record, &table_name);

                        // Self-feed guard: never produce our own checkpoint
                        // rows, but still checkpoint past them.
                        let is_checkpoint_row = event
                            .pk()
                            .is_some_and(|pk| pk.starts_with(CDC_CHECKPOINT_PK_PREFIX));
                        if !is_checkpoint_row {
                            if let Err(e) = producer.send(&config.topic, &event).await {
                                tracing::error!(
                                    shard_id = %shard_id,
                                    error = %e,
                                    "produce failed; rewinding shard to last checkpoint"
                                );
                                batch_failed = true;
                                break;
                            }
                            *window_counts.entry(shard_id.clone()).or_default() += 1;
                        }

                        if let Some(ms) = event.approximate_creation_ms {
                            last_event_age_ms.insert(shard_id.clone(), epoch_ms() - ms);
                        }
                        last_seq = seq.or(last_seq);
                    }

                    if batch_failed {
                        // The iterator already advanced past the unproduced
                        // records, so drop it — the next discovery rebuilds it
                        // with AFTER_SEQUENCE_NUMBER(last checkpointed seq)
                        // and the failed records are re-read.
                        shard_iterators.remove(&shard_id);
                        continue;
                    }

                    // Every produce in the batch was delivery-acknowledged —
                    // safe to commit the checkpoint.
                    if let Some(seq) = last_seq {
                        match store.save(&shard_id, &seq).await {
                            Ok(()) => {
                                checkpoints.insert(shard_id.clone(), seq);
                            }
                            Err(e) => {
                                // At-least-once: a lost checkpoint only means
                                // re-producing this batch after a restart.
                                tracing::warn!(shard_id = %shard_id, error = %e, "checkpoint save failed");
                            }
                        }
                    }

                    match output.next_shard_iterator() {
                        Some(next) => {
                            shard_iterators.insert(shard_id.clone(), next.to_string());
                        }
                        None => {
                            tracing::info!(shard_id = %shard_id, "shard closed; done polling it");
                            shard_iterators.remove(&shard_id);
                            closed_shards.insert(shard_id);
                        }
                    }
                }
                Err(e) => {
                    let expired = e
                        .as_service_error()
                        .map(|se| se.is_expired_iterator_exception())
                        .unwrap_or(false);
                    if expired {
                        tracing::warn!(shard_id = %shard_id, "shard iterator expired; rebuilding from checkpoint");
                    } else {
                        tracing::warn!(shard_id = %shard_id, error = %e, "get_records failed; rebuilding iterator from checkpoint");
                    }
                    shard_iterators.remove(&shard_id);
                }
            }
        }

        // ── Metrics logging (every 60s) ──
        if last_metrics.elapsed() >= Duration::from_secs(60) {
            let total: u64 = window_counts.values().sum();
            for (shard, count) in &window_counts {
                tracing::info!(
                    shard_id = %shard,
                    produced = count,
                    last_event_age_ms = last_event_age_ms.get(shard).copied().unwrap_or(-1),
                    "cdc shard stats (60s window)"
                );
            }
            tracing::info!(
                total_produced = total,
                live_shards = shard_iterators.len(),
                closed_shards = closed_shards.len(),
                "cdc stats (60s window)"
            );
            window_counts.clear();
            last_metrics = Instant::now();
        }
    }
}

/// Build the DynamoDB Streams client the same way `stream_poller` does:
/// region/credentials from `AwsConfig`, endpoint override from `DynamoConfig`
/// — so the binary works against LocalStack and real AWS alike.
#[cfg(feature = "redpanda")]
fn build_streams_client() -> aws_sdk_dynamodbstreams::Client {
    use app_shell::common::aws_config::AwsConfig;
    use app_shell::common::dynamodb::DynamoConfig;

    let aws_cfg = AwsConfig::default();
    let mut builder = aws_sdk_dynamodbstreams::Config::builder()
        .region(aws_config::Region::new(aws_cfg.region))
        .behavior_version_latest()
        .credentials_provider(aws_credential_types::Credentials::new(
            aws_cfg.access_key_id,
            aws_cfg.secret_access_key,
            None,
            None,
            "loaded-from-config",
        ));

    let ddb_cfg = DynamoConfig::default();
    if let Some(ep) = ddb_cfg.endpoint {
        builder = builder.endpoint_url(ep);
    }

    aws_sdk_dynamodbstreams::Client::from_conf(builder.build())
}

/// Discover the main table's stream ARN, retrying until it exists (the table
/// or its stream may not be provisioned yet at startup).
#[cfg(feature = "redpanda")]
async fn wait_for_stream_arn(dynamodb: &aws_sdk_dynamodb::Client, table_name: &str) -> String {
    loop {
        let arn = dynamodb
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .ok()
            .and_then(|output| output.table)
            .and_then(|table| table.latest_stream_arn);
        match arn {
            Some(arn) => return arn,
            None => {
                tracing::warn!(table = %table_name, "no DynamoDB stream found yet; retrying in 5s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

/// Create a shard iterator: from the checkpoint via `AFTER_SEQUENCE_NUMBER`
/// when one exists, otherwise from `RATEL_CDC_START`. If the checkpointed
/// sequence number can no longer be resolved (e.g. trimmed past the 24h
/// retention), fall back to the start position — those records are gone
/// either way.
#[cfg(feature = "redpanda")]
async fn new_shard_iterator(
    client: &aws_sdk_dynamodbstreams::Client,
    stream_arn: &str,
    shard_id: &str,
    checkpoint_seq: Option<&str>,
    start: app_shell::common::events::CdcStart,
) -> Result<Option<String>, String> {
    use app_shell::common::events::CdcStart;
    use aws_sdk_dynamodbstreams::types::ShardIteratorType;

    if let Some(seq) = checkpoint_seq {
        match client
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id)
            .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
            .sequence_number(seq)
            .send()
            .await
        {
            Ok(output) => return Ok(output.shard_iterator().map(str::to_string)),
            Err(e) => {
                tracing::warn!(
                    shard_id = %shard_id,
                    seq = %seq,
                    error = %e,
                    "checkpointed iterator unavailable; falling back to start position"
                );
            }
        }
    }

    let iterator_type = match start {
        CdcStart::Latest => ShardIteratorType::Latest,
        CdcStart::TrimHorizon => ShardIteratorType::TrimHorizon,
    };
    let output = client
        .get_shard_iterator()
        .stream_arn(stream_arn)
        .shard_id(shard_id)
        .shard_iterator_type(iterator_type)
        .send()
        .await
        .map_err(|e| format!("get_shard_iterator failed: {e}"))?;
    Ok(output.shard_iterator().map(str::to_string))
}
