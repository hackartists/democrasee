//! Redpanda consumer loop for workers (feature `redpanda`).
//!
//! `enable.auto.commit=false`; per message: parse `CdcEvent` → `dispatch` →
//! retry with backoff (1s/5s/15s) → DLQ on final failure → commit. A message
//! is always committed after it is handled (success, DLQ'd parse failure, or
//! DLQ'd dispatch failure) so a poison message can never wedge the partition.
//!
//! Re-dispatching the same event on retry is safe: rules are independent and
//! the production EventBridge path was at-least-once too.

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::cdc_event::CdcEvent;
use super::config::EventsConfig;
use super::dispatcher::{RoleSet, dispatch};
use crate::common::utils::time::sleep;

/// Backoff before each dispatch retry (initial attempt + 3 retries total).
const RETRY_BACKOFF_SECS: [u64; 3] = [1, 5, 15];

/// Consume `config.topic` and dispatch every `CdcEvent` with the role set
/// derived from `config.worker_role`. Returns `Ok(())` on graceful shutdown
/// (SIGTERM / ctrl_c) after finishing and committing the in-flight message.
pub async fn run_worker(config: &EventsConfig) -> Result<(), String> {
    let roles = RoleSet::from_worker_role(&config.worker_role);

    let mut consumer_cfg = ClientConfig::new();
    consumer_cfg
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.worker_group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "45000");
    if config.worker_role == "analyze" {
        // Analysis jobs run up to ~5 minutes each; give the poll loop headroom
        // so the group coordinator doesn't evict a busy worker.
        consumer_cfg.set("max.poll.interval.ms", "600000");
    }
    let consumer: StreamConsumer = consumer_cfg
        .create()
        .map_err(|e| format!("failed to create kafka consumer: {e}"))?;
    consumer
        .subscribe(&[&config.topic])
        .map_err(|e| format!("failed to subscribe to {}: {e}", config.topic))?;

    let dlq = DlqProducer::new(&config.brokers, &config.dlq_topic)?;

    tracing::info!(
        brokers = %config.brokers,
        topic = %config.topic,
        dlq_topic = %config.dlq_topic,
        group = %config.worker_group,
        role = %config.worker_role,
        "worker consuming"
    );

    let mut shutdown = std::pin::pin!(shutdown_signal());
    loop {
        // Only race the signal while waiting for a message — once one is
        // received it is processed and committed to completion, so k3s
        // rolling restarts never drop an in-flight event.
        let msg = tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received; exiting worker loop");
                return Ok(());
            }
            received = consumer.recv() => match received {
                Ok(msg) => msg,
                Err(e) => {
                    tracing::error!(error = %e, "kafka receive error");
                    continue;
                }
            },
        };
        handle_message(&consumer, &dlq, &msg, roles).await;
    }
}

/// Process one message end-to-end and commit it.
async fn handle_message(
    consumer: &StreamConsumer,
    dlq: &DlqProducer,
    msg: &BorrowedMessage<'_>,
    roles: RoleSet,
) {
    let payload = msg.payload().unwrap_or_default();
    let key = msg
        .key()
        .and_then(|k| std::str::from_utf8(k).ok())
        .map(str::to_string);

    // Parse failure: never wedge the partition — forward the raw payload to
    // the DLQ and commit.
    let event: CdcEvent = match serde_json::from_slice(payload) {
        Ok(event) => event,
        Err(e) => {
            tracing::error!(
                error = %e,
                partition = msg.partition(),
                offset = msg.offset(),
                "unparseable CDC payload; forwarding raw payload to DLQ"
            );
            dlq.send(key.as_deref(), payload.to_vec()).await;
            commit(consumer, msg);
            return;
        }
    };

    // Shadow mode (`RATEL_WORKER_DRY_RUN=1`): evaluate filters and log what
    // WOULD run, but execute no handlers. Lets the pipeline run against a
    // live stream while EventBridge is still the active consumer (dev
    // cutover plan W5) without double-processing side effects.
    if dry_run() {
        let matched = super::dispatcher::matched_rules(&event, roles);
        tracing::info!(
            matched = ?matched,
            pk = ?event.pk(),
            sk = ?event.sk(),
            "dry-run: would dispatch"
        );
        commit(consumer, msg);
        return;
    }

    let mut summary = dispatch(&event, roles).await;
    if !summary.is_ok() {
        for (retry, backoff_secs) in RETRY_BACKOFF_SECS.iter().enumerate() {
            tracing::warn!(
                failed = ?summary.failed,
                retry = retry + 1,
                backoff_secs,
                pk = ?event.pk(),
                sk = ?event.sk(),
                "dispatch failed; retrying"
            );
            sleep(Duration::from_secs(*backoff_secs)).await;
            summary = dispatch(&event, roles).await;
            if summary.is_ok() {
                break;
            }
        }
    }

    if summary.is_ok() {
        tracing::debug!(
            matched = ?summary.matched,
            pk = ?event.pk(),
            sk = ?event.sk(),
            "dispatched"
        );
    } else {
        tracing::error!(
            failed = ?summary.failed,
            pk = ?event.pk(),
            sk = ?event.sk(),
            "dispatch failed after retries; forwarding event to DLQ"
        );
        let wrapper = serde_json::json!({
            "failed_rules": summary.failed,
            "event": event,
        });
        let payload = serde_json::to_vec(&wrapper).unwrap_or_default();
        dlq.send(key.as_deref(), payload).await;
    }
    commit(consumer, msg);
}

/// `RATEL_WORKER_DRY_RUN=1|true|on` — shadow mode, read once per message so
/// it stays a pure runtime switch.
fn dry_run() -> bool {
    matches!(
        std::env::var("RATEL_WORKER_DRY_RUN").as_deref(),
        Ok("1") | Ok("true") | Ok("on")
    )
}

fn commit(consumer: &StreamConsumer, msg: &BorrowedMessage<'_>) {
    if let Err(e) = consumer.commit_message(msg, CommitMode::Async) {
        tracing::error!(error = %e, "failed to commit offset");
    }
}

/// Minimal DLQ producer private to the worker. Deliberately not shared with
/// `producer.rs` (the CDC producer wrapper) to keep the two paths independent.
struct DlqProducer {
    producer: FutureProducer,
    topic: String,
}

impl DlqProducer {
    fn new(brokers: &str, topic: &str) -> Result<Self, String> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("message.timeout.ms", "30000")
            .create()
            .map_err(|e| format!("failed to create DLQ producer: {e}"))?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }

    /// Best-effort DLQ produce. A DLQ failure is logged but does not block the
    /// commit — the message is already lost to normal processing, and wedging
    /// the partition on a broken DLQ would take down live traffic too.
    async fn send(&self, key: Option<&str>, payload: Vec<u8>) {
        let record = FutureRecord::<str, [u8]>::to(&self.topic).payload(payload.as_slice());
        let record = match key {
            Some(k) => record.key(k),
            None => record,
        };
        match self.producer.send(record, Duration::from_secs(30)).await {
            Ok(_) => tracing::warn!(topic = %self.topic, "message forwarded to DLQ"),
            Err((e, _)) => tracing::error!(
                error = %e,
                topic = %self.topic,
                "failed to produce to DLQ; committing anyway to avoid wedging the partition"
            ),
        }
    }
}

/// Resolves when SIGTERM (k3s pod termination) or ctrl_c is received.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
